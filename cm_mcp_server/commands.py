"""
Comandos em andamento e travados (somente leitura).

O CM lista comandos ativos em endpoints separados por nível, e um comando aparece só no nível
em que foi disparado (um restart iniciado na página do HDFS não aparece nos comandos do
cluster). Este módulo consulta todos os níveis, remove duplicados, calcula há quanto tempo
cada comando roda, marca como travado o que passa do limite e segue a cadeia de subcomandos
ativos para mostrar em que etapa ele está parado.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from cm_mcp_server.core import _items, _out, cm, mcp

MAX_DETAILED = 10      # comandos com a cadeia de subcomandos detalhada
MAX_DEPTH = 5          # profundidade máxima ao seguir subcomandos ativos


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _elapsed_min(cmd: dict, now: datetime) -> float | None:
    start = _parse_ts(cmd.get("startTime"))
    if start is None:
        return None
    end = _parse_ts(cmd.get("endTime")) or now
    return round((end - start).total_seconds() / 60, 1)


def _context(cmd: dict) -> dict:
    sref, rref, href, cref = (cmd.get(k) or {} for k in ("serviceRef", "roleRef", "hostRef", "clusterRef"))
    return {k: v for k, v in {
        "cluster": cref.get("clusterName") or sref.get("clusterName") or rref.get("clusterName"),
        "service": sref.get("serviceName") or rref.get("serviceName"),
        "role": rref.get("roleName"),
        "host": href.get("hostname") or href.get("hostId"),
    }.items() if v}


def _slim(cmd: dict, now: datetime) -> dict:
    return {k: v for k, v in {
        "id": cmd.get("id"), "name": cmd.get("name"), "active": cmd.get("active"),
        "success": cmd.get("success"), "startTime": cmd.get("startTime"), "endTime": cmd.get("endTime"),
        "elapsedMin": _elapsed_min(cmd, now), "resultMessage": cmd.get("resultMessage"),
        **_context(cmd),
    }.items() if v is not None}


def active_chain(command_id: int, now: datetime | None = None) -> list[dict]:
    """Do comando até o subcomando ativo mais profundo: mostra em que etapa ele está."""
    now = now or datetime.now(timezone.utc)
    chain: list[dict] = []
    current = command_id
    for _ in range(MAX_DEPTH):
        detail = cm().get(f"commands/{current}")
        children = [c for c in _items(detail.get("children") or {}) if c.get("active")]
        if not children:
            break
        child = min(children, key=lambda c: c.get("startTime") or "")
        chain.append(_slim(child, now))
        if child.get("id") is None:
            break
        current = child["id"]
    return chain


def collect_running_commands(cluster: str | None = None, stuck_minutes: float = 60,
                             include_roles: bool = False, detail: bool = True) -> dict:
    """Comandos ativos em todos os níveis do CM, do mais antigo para o mais novo."""
    c = cm()
    now = datetime.now(timezone.utc)
    errors: list[str] = []

    def get_items(path: str) -> list[dict]:
        try:
            return _items(c.get(path))
        except Exception as e:  # noqa: BLE001
            errors.append(f"{path}: {e}"[:200])
            return []

    sources: list[tuple[str, str, dict]] = [("cm", "cm/commands", {}),
                                            ("management", "cm/service/commands", {"service": "mgmt"})]
    if include_roles:
        for r in get_items("cm/service/roles"):
            sources.append(("role", f"cm/service/roles/{r['name']}/commands",
                            {"service": "mgmt", "role": r["name"]}))
    clusters = [cluster] if cluster else [x.get("name") for x in get_items("clusters")]
    for cl in clusters:
        sources.append(("cluster", f"clusters/{cl}/commands", {"cluster": cl}))
        for s in get_items(f"clusters/{cl}/services"):
            svc = s.get("name")
            sources.append(("service", f"clusters/{cl}/services/{svc}/commands", {"cluster": cl, "service": svc}))
            if include_roles:
                for r in get_items(f"clusters/{cl}/services/{svc}/roles"):
                    sources.append(("role", f"clusters/{cl}/services/{svc}/roles/{r['name']}/commands",
                                    {"cluster": cl, "service": svc, "role": r["name"]}))

    seen: set[Any] = set()
    rows: list[dict] = []
    for scope, path, defaults in sources:
        for cmd in get_items(path):
            if cmd.get("active") is False:
                continue
            key = cmd.get("id") or (path, cmd.get("name"), cmd.get("startTime"))
            if key in seen:
                continue
            seen.add(key)
            elapsed = _elapsed_min(cmd, now)
            rows.append({**_slim(cmd, now), **{k: v for k, v in defaults.items() if k not in _context(cmd)},
                         "scope": scope,
                         "stuck": elapsed is not None and elapsed >= stuck_minutes})

    rows.sort(key=lambda r: r.get("elapsedMin") or 0, reverse=True)
    if detail:
        for row in rows[:MAX_DETAILED]:
            if row.get("id") is None:
                continue
            try:
                chain = active_chain(row["id"], now)
            except Exception as e:  # noqa: BLE001
                errors.append(f"commands/{row['id']}: {e}"[:200])
                continue
            if chain:
                row["currentStep"] = chain[-1]
                row["activeSteps"] = [s.get("name") for s in chain]
    return {
        "stuckMinutes": stuck_minutes,
        "sourcesChecked": len(sources),
        "running": len(rows),
        "stuck": sum(1 for r in rows if r["stuck"]),
        "commands": rows,
        **({"errors": errors[:10]} if errors else {}),
    }


@mcp.tool()
def list_running_commands(cluster: str | None = None, stuck_minutes: float = 60,
                          include_roles: bool = False) -> str:
    """
    Todos os comandos em andamento no CM, em todos os níveis: globais, Management Service,
    clusters e serviços (e roles, com include_roles=True). Um restart iniciado direto num
    serviço só aparece no nível do serviço, por isso olhar só o cluster não basta.
    Para cada comando: escopo, cluster/serviço/role/host, há quantos minutos roda, se está
    travado (passou de stuck_minutes) e a etapa atual (o subcomando ativo mais profundo).
    Ordenado do mais antigo para o mais novo.
    cluster: um cluster específico (padrão: todos).
    stuck_minutes: a partir de quantos minutos um comando é considerado travado (padrão 60).
    include_roles: também consulta os comandos de cada role (mais lento).
    """
    data = collect_running_commands(cluster, stuck_minutes, include_roles)
    if not data["commands"]:
        data["commands"] = "Nenhum comando em andamento."
    return _out(data)
