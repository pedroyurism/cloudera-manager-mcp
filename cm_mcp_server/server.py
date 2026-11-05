"""
Cloudera Manager MCP Server (read-only)

Expõe a API REST do Cloudera Manager como tools MCP, apenas com operações de leitura.
Configuração por variáveis de ambiente (veja .env.example).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from cm_mcp_server.core import _health, _items, _iso, _out, cm, mcp

# ---------------------------------------------------------------------------
# Tools – visão geral
# ---------------------------------------------------------------------------

@mcp.tool()
def cm_info() -> str:
    """Versão do Cloudera Manager, versão da API em uso e status geral do CM."""
    c = cm()
    info: dict[str, Any] = {"apiVersion": c.version}
    try:
        info["cmVersion"] = c.get("cm/version")
    except Exception as e:  # noqa: BLE001
        info["cmVersion"] = f"erro: {e}"
    try:
        mgmt = c.get("cm/service", {"view": "full"})
        info["managementService"] = {"name": mgmt.get("name"), "state": mgmt.get("serviceState"), **_health(mgmt)}
    except Exception as e:  # noqa: BLE001
        info["managementService"] = f"erro: {e}"
    return _out(info)


@mcp.tool()
def list_clusters() -> str:
    """Lista os clusters gerenciados, com versão do runtime, status e saúde."""
    clusters = _items(cm().get("clusters", {"view": "full"}))
    return _out([
        {
            "name": cl.get("name"),
            "displayName": cl.get("displayName"),
            "fullVersion": cl.get("fullVersion"),
            "entityStatus": cl.get("entityStatus"),
            "maintenanceMode": cl.get("maintenanceMode"),
            "clusterType": cl.get("clusterType"),
        }
        for cl in clusters
    ])


@mcp.tool()
def list_services(cluster: str) -> str:
    """Lista os serviços de um cluster com estado, saúde e checks não saudáveis."""
    services = _items(cm().get(f"clusters/{cluster}/services", {"view": "full"}))
    return _out([
        {
            "name": s.get("name"),
            "type": s.get("type"),
            "displayName": s.get("displayName"),
            "serviceState": s.get("serviceState"),
            "configStale": s.get("configStalenessStatus"),
            "clientConfigStale": s.get("clientConfigStalenessStatus"),
            "maintenanceMode": s.get("maintenanceMode"),
            **_health(s),
        }
        for s in services
    ])


@mcp.tool()
def cluster_health_summary(cluster: str) -> str:
    """Resumo rápido: só serviços e roles que NÃO estão GOOD, com os checks que falharam."""
    c = cm()
    problems = []
    for s in _items(c.get(f"clusters/{cluster}/services", {"view": "full"})):
        bad_roles = []
        if s.get("healthSummary") != "GOOD":
            for r in _items(c.get(f"clusters/{cluster}/services/{s['name']}/roles", {"view": "full"})):
                if r.get("healthSummary") not in ("GOOD", "DISABLED"):
                    bad_roles.append({
                        "role": r.get("name"),
                        "type": r.get("type"),
                        "host": (r.get("hostRef") or {}).get("hostname") or (r.get("hostRef") or {}).get("hostId"),
                        "roleState": r.get("roleState"),
                        **_health(r),
                    })
            problems.append({
                "service": s.get("name"),
                "type": s.get("type"),
                "serviceState": s.get("serviceState"),
                **_health(s),
                "unhealthyRoles": bad_roles,
            })
    return _out(problems or {"status": "Todos os serviços estão GOOD."})


# ---------------------------------------------------------------------------
# Tools – serviços e roles
# ---------------------------------------------------------------------------

@mcp.tool()
def get_service(cluster: str, service: str) -> str:
    """Detalhes de um serviço (estado, saúde, health checks, staleness)."""
    s = cm().get(f"clusters/{cluster}/services/{service}", {"view": "full"})
    return _out({
        "name": s.get("name"),
        "type": s.get("type"),
        "serviceState": s.get("serviceState"),
        "healthSummary": s.get("healthSummary"),
        "healthChecks": s.get("healthChecks"),
        "configStalenessStatus": s.get("configStalenessStatus"),
        "maintenanceMode": s.get("maintenanceMode"),
        "serviceUrl": s.get("serviceUrl"),
    })


@mcp.tool()
def list_roles(cluster: str, service: str, only_unhealthy: bool = False) -> str:
    """Lista as roles de um serviço (tipo, host, estado, saúde). Use only_unhealthy=True para filtrar."""
    roles = _items(cm().get(f"clusters/{cluster}/services/{service}/roles", {"view": "full"}))
    out = []
    for r in roles:
        if only_unhealthy and r.get("healthSummary") in ("GOOD", "DISABLED"):
            continue
        out.append({
            "name": r.get("name"),
            "type": r.get("type"),
            "host": (r.get("hostRef") or {}).get("hostname") or (r.get("hostRef") or {}).get("hostId"),
            "roleState": r.get("roleState"),
            "haStatus": r.get("haStatus"),
            "configStale": r.get("configStalenessStatus"),
            "maintenanceMode": r.get("maintenanceMode"),
            **_health(r),
        })
    return _out(out)


@mcp.tool()
def get_service_config(cluster: str, service: str, only_non_default: bool = True) -> str:
    """Configuração de nível de serviço. Por padrão mostra só o que foi alterado em relação ao default."""
    view = "summary" if only_non_default else "full"
    cfg = _items(cm().get(f"clusters/{cluster}/services/{service}/config", {"view": view}))
    return _out([
        {k: v for k, v in {
            "name": i.get("name"),
            "value": i.get("value"),
            "default": i.get("default"),
            "displayName": i.get("displayName"),
            "sensitive": i.get("sensitive"),
        }.items() if v is not None}
        for i in cfg
    ])


@mcp.tool()
def get_role_config_groups(cluster: str, service: str, only_non_default: bool = True) -> str:
    """Role config groups de um serviço e suas configurações (por padrão só valores alterados)."""
    c = cm()
    groups = _items(c.get(f"clusters/{cluster}/services/{service}/roleConfigGroups"))
    view = "summary" if only_non_default else "full"
    out = []
    for g in groups:
        cfg = _items(c.get(f"clusters/{cluster}/services/{service}/roleConfigGroups/{g['name']}/config", {"view": view}))
        out.append({
            "name": g.get("name"),
            "roleType": g.get("roleType"),
            "base": g.get("base"),
            "config": {i.get("name"): i.get("value") for i in cfg},
        })
    return _out(out)


@mcp.tool()
def get_role_log(cluster: str, service: str, role: str, log: str = "stderr", tail_lines: int = 200) -> str:
    """Últimas linhas do log de uma role. log: 'full' (log do processo), 'stderr' ou 'stdout'."""
    if log not in ("full", "stderr", "stdout"):
        return "log deve ser 'full', 'stderr' ou 'stdout'."
    text = cm().get(f"clusters/{cluster}/services/{service}/roles/{role}/logs/{log}", raw=True)
    lines = text.splitlines()[-max(1, tail_lines):]
    return _out("\n".join(lines))


# ---------------------------------------------------------------------------
# Tools – hosts
# ---------------------------------------------------------------------------

@mcp.tool()
def list_hosts(cluster: str | None = None, only_unhealthy: bool = False) -> str:
    """Lista hosts (de um cluster ou de todo o CM): IP, rack, cores, memória, saúde, último heartbeat."""
    c = cm()
    path = f"clusters/{cluster}/hosts" if cluster else "hosts"
    hosts = _items(c.get(path, {"view": "full"}))
    out = []
    for h in hosts:
        if only_unhealthy and h.get("healthSummary") == "GOOD":
            continue
        out.append({
            "hostname": h.get("hostname"),
            "hostId": h.get("hostId"),
            "ip": h.get("ipAddress"),
            "rack": h.get("rackId"),
            "cores": h.get("numCores"),
            "memoryGB": round(h["totalPhysMemBytes"] / 1024**3, 1) if h.get("totalPhysMemBytes") else None,
            "lastHeartbeat": h.get("lastHeartbeat"),
            "maintenanceMode": h.get("maintenanceMode"),
            "roles": len(h.get("roleRefs") or []),
            **_health(h),
        })
    return _out(out)


@mcp.tool()
def get_host(host_id: str) -> str:
    """Detalhes de um host (aceita hostId), incluindo roles hospedadas e health checks."""
    h = cm().get(f"hosts/{host_id}", {"view": "full"})
    return _out({
        "hostname": h.get("hostname"),
        "ip": h.get("ipAddress"),
        "rack": h.get("rackId"),
        "cores": h.get("numCores"),
        "totalPhysMemBytes": h.get("totalPhysMemBytes"),
        "lastHeartbeat": h.get("lastHeartbeat"),
        "distribution": h.get("distribution"),
        "healthSummary": h.get("healthSummary"),
        "healthChecks": h.get("healthChecks"),
        "roles": [
            f"{r.get('serviceName')}/{r.get('roleName')}" for r in h.get("roleRefs") or []
        ],
    })


# ---------------------------------------------------------------------------
# Tools – monitoramento
# ---------------------------------------------------------------------------

@mcp.tool()
def query_timeseries(tsquery: str, minutes: int = 60, desired_rollup: str = "RAW") -> str:
    """
    Executa uma tsquery do Cloudera Manager.
    Ex.: "select cpu_percent where category = HOST",
         "select total_capacity_used_across_datanodes where entityName = hdfs",
         "select allocated_memory_mb_cumulative where category = YARN_POOL".
    desired_rollup: RAW, TEN_MINUTELY, HOURLY, SIX_HOURLY, DAILY, WEEKLY.
    """
    now = datetime.now(timezone.utc)
    resp = cm().get("timeseries", {
        "query": tsquery,
        "from": _iso(now - timedelta(minutes=minutes)),
        "to": _iso(now),
        "desiredRollup": desired_rollup,
        "mustUseDesiredRollup": "false",
    })
    out = []
    for item in _items(resp):
        for ts in item.get("timeSeries", []):
            md = ts.get("metadata", {})
            pts = ts.get("data", [])
            vals = [p.get("value") for p in pts if p.get("value") is not None]
            out.append({
                "metric": md.get("metricName"),
                "entity": md.get("entityName"),
                "unit": md.get("unitNumerators"),
                "points": len(pts),
                "min": min(vals) if vals else None,
                "max": max(vals) if vals else None,
                "last": vals[-1] if vals else None,
                "last_ts": pts[-1].get("timestamp") if pts else None,
            })
        if item.get("errors"):
            out.append({"errors": item["errors"]})
    return _out(out or "Sem dados para essa consulta.")


@mcp.tool()
def list_events(hours: int = 24, severity: str | None = "CRITICAL", category: str | None = None,
                service: str | None = None, max_results: int = 50) -> str:
    """
    Eventos do CM nas últimas N horas.
    severity: CRITICAL, IMPORTANT, INFORMATIONAL (ou None para todos).
    category: HEALTH_EVENT, LOG_MESSAGE, AUDIT_EVENT, ACTIVITY_EVENT, SYSTEM (opcional).
    service: filtra pelo nome do serviço (opcional).
    """
    since = _iso(datetime.now(timezone.utc) - timedelta(hours=hours))
    filters = [f"timeReceived=ge={since}"]
    if severity:
        filters.append(f"severity=={severity}")
    if category:
        filters.append(f"category=={category}")
    if service:
        filters.append(f"attributes.SERVICE=={service}")
    resp = cm().get("events", {"query": ";".join(filters), "maxResults": max_results})
    out = []
    for e in _items(resp):
        attrs = {a.get("name"): a.get("values") for a in e.get("attributes", [])}
        out.append({
            "time": e.get("timeOccurred"),
            "severity": e.get("severity"),
            "category": e.get("category"),
            "content": (e.get("content") or "")[:500],
            "service": attrs.get("SERVICE"),
            "role": attrs.get("ROLE"),
            "host": attrs.get("HOSTS"),
            "alert": e.get("alert"),
        })
    return _out(out)


@mcp.tool()
def list_active_commands(cluster: str | None = None) -> str:
    """Comandos em execução no CM (restart, deploy client config, upgrade etc.)."""
    path = f"clusters/{cluster}/commands" if cluster else "cm/commands"
    cmds = _items(cm().get(path))
    return _out([
        {"id": c.get("id"), "name": c.get("name"), "startTime": c.get("startTime"),
         "active": c.get("active"), "resultMessage": c.get("resultMessage")}
        for c in cmds
    ] or "Nenhum comando ativo.")


@mcp.tool()
def get_command(command_id: int) -> str:
    """Detalhes de um comando (inclusive subcomandos que falharam)."""
    c = cm().get(f"commands/{command_id}")

    def slim(x: dict) -> dict:
        return {
            "id": x.get("id"), "name": x.get("name"), "active": x.get("active"),
            "success": x.get("success"), "resultMessage": x.get("resultMessage"),
            "startTime": x.get("startTime"), "endTime": x.get("endTime"),
            "service": (x.get("serviceRef") or {}).get("serviceName"),
            "role": (x.get("roleRef") or {}).get("roleName"),
            "host": (x.get("hostRef") or {}).get("hostname"),
        }

    children = _items(c.get("children") or {})
    return _out({
        **slim(c),
        "failedChildren": [slim(ch) for ch in children if ch.get("success") is False],
        "totalChildren": len(children),
    })


@mcp.tool()
def list_parcels(cluster: str) -> str:
    """Parcels do cluster e seu estágio (ACTIVATED, DISTRIBUTED, DOWNLOADED...)."""
    parcels = _items(cm().get(f"clusters/{cluster}/parcels"))
    return _out([
        {"product": p.get("product"), "version": p.get("version"), "stage": p.get("stage")}
        for p in parcels if p.get("stage") not in ("AVAILABLE_REMOTELY",)
    ])


@mcp.tool()
def list_audits(hours: int = 24, max_results: int = 100, user: str | None = None) -> str:
    """Auditoria do CM (quem fez o quê) nas últimas N horas. Filtra opcionalmente por usuário."""
    now = datetime.now(timezone.utc)
    resp = cm().get("audits", {
        "startTime": _iso(now - timedelta(hours=hours)),
        "endTime": _iso(now),
        "maxResults": max_results,
    })
    out = []
    for a in _items(resp):
        if user and a.get("username") != user:
            continue
        out.append({
            "time": a.get("timestamp"),
            "user": a.get("username"),
            "command": a.get("command"),
            "service": (a.get("serviceRef") or {}).get("serviceName"),
            "role": (a.get("roleRef") or {}).get("roleName"),
            "allowed": a.get("allowed"),
            "ip": a.get("ipAddress"),
        })
    return _out(out)


@mcp.tool()
def cm_api_get(path: str, params_json: str = "{}") -> str:
    """
    Escape hatch SOMENTE LEITURA: faz um GET em qualquer endpoint da API do CM
    (path relativo à versão, ex.: 'clusters/Cluster1/services/hdfs/nameservices').
    params_json: objeto JSON com query params, ex.: '{"view": "full"}'.
    """
    try:
        params = json.loads(params_json or "{}")
    except json.JSONDecodeError as e:
        return f"params_json inválido: {e}"
    return _out(cm().get(path, params))


# Registra as tools de YARN e de Impala (módulos separados)
from cm_mcp_server import impala, yarn  # noqa: E402,F401


# ---------------------------------------------------------------------------
# Entrypoints
# ---------------------------------------------------------------------------

def main_stdio() -> None:
    mcp.run()


def main_http() -> None:
    import os

    host = os.environ.get("MCP_HTTP_HOST", "127.0.0.1")
    port = int(os.environ.get("MCP_HTTP_PORT", "8000"))
    mcp.run(transport="http", host=host, port=port)


if __name__ == "__main__":
    main_stdio()
