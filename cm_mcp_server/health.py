"""
Relatório de saúde do ambiente (somente leitura).

Junta numa única chamada o que um health check de plantão olha:
  - serviços que não estão GOOD e os checks que falharam;
  - roles paradas ou com problema e o estado de alta disponibilidade (Active/Standby);
  - hosts com problema (disco, swap, clock offset) e agentes sem heartbeat recente;
  - Management Service (Service Monitor, Host Monitor, Event Server...), avisando quando o
    monitoramento está cego;
  - alertas (eventos com alert=true) da janela, agrupados por severidade e serviço;
  - serviços e roles com configuração desatualizada (restart/refresh pendente);
  - comandos em andamento em todos os níveis, avisando os travados;
e calcula um status geral: OK, ATENCAO ou CRITICO.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from cm_mcp_server.commands import collect_running_commands
from cm_mcp_server.core import _items, _iso, _out, cm, mcp

LEVELS = ("critico", "atencao", "info")
STATUS_BY_LEVEL = {"critico": "CRITICO", "atencao": "ATENCAO"}
UNKNOWN_HEALTH = ("UNKNOWN", "HISTORY_NOT_AVAILABLE", "NOT_AVAILABLE")
STOPPED_STATES = ("STOPPED", "UNKNOWN", "HISTORY_NOT_AVAILABLE")

# Roles do Management Service e o que se perde quando cada uma está fora.
MGMT_ROLES = {
    "SERVICEMONITOR": ("critico", "Service Monitor fora: a saúde de serviços e roles, as métricas e o "
                                  "histórico de queries Impala e apps YARN param de atualizar (o CM pode "
                                  "mostrar GOOD desatualizado). Monitoramento cego."),
    "HOSTMONITOR": ("critico", "Host Monitor fora: a saúde e as métricas dos hosts param de atualizar. "
                               "Monitoramento de hosts cego."),
    "EVENTSERVER": ("critico", "Event Server fora: eventos e alertas não são registrados. "
                               "Monitoramento de eventos cego."),
    "ALERTPUBLISHER": ("atencao", "Alert Publisher fora: alertas não são enviados (e-mail/SNMP)."),
    "REPORTSMANAGER": ("atencao", "Reports Manager fora: relatórios de uso do HDFS não são gerados."),
}

# Checks de host que o relatório destaca.
KEY_HOST_CHECKS = {"clock": ("CLOCK_OFFSET",), "swap": ("SWAP",),
                   "disco": ("FREE_SPACE", "DATA_DIRECTORY", "DISK")}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _bad_checks(obj: dict) -> list[str]:
    return [f"{h.get('name')}={h.get('summary')}" for h in obj.get("healthChecks") or []
            if h.get("summary") not in ("GOOD", "DISABLED", "NOT_AVAILABLE") and not h.get("suppressed")]


def _host_of(role: dict) -> str | None:
    ref = role.get("hostRef") or {}
    return ref.get("hostname") or ref.get("hostId")


class _Report:
    def __init__(self) -> None:
        self.findings: list[dict] = []

    def add(self, level: str, area: str, entity: str, message: str) -> None:
        self.findings.append({"level": level, "area": area, "entity": entity, "message": message})

    def guard(self, area: str, what: str, fn, *args, **kwargs) -> Any:
        """Executa uma seção; se a API falhar (ex.: 403), registra e segue com o resto."""
        try:
            return fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001
            self.add("atencao", "acesso", what, f"não foi possível ler {what}: {e}"[:300])
            return None


# ---------------------------------------------------------------------------
# Seções
# ---------------------------------------------------------------------------

def _services(rep: _Report, cluster: str) -> tuple[list[dict], dict]:
    services = _items(cm().get(f"clusters/{cluster}/services", {"view": "full"}))
    counts: Counter = Counter()
    for s in services:
        name, health = s.get("name"), s.get("healthSummary")
        counts["total"] += 1
        if s.get("maintenanceMode"):
            counts["maintenance"] += 1
            if health not in ("GOOD", "DISABLED", None):
                rep.add("info", "servicos", f"{cluster}/{name}", f"em manutenção (saúde {health})")
            continue
        counts[health or "UNKNOWN"] += 1
        checks = _bad_checks(s)
        detail = f"; checks: {', '.join(checks)}" if checks else ""
        if health == "BAD":
            rep.add("critico", "servicos", f"{cluster}/{name}", f"saúde BAD{detail}")
        elif health == "CONCERNING":
            rep.add("atencao", "servicos", f"{cluster}/{name}", f"saúde CONCERNING{detail}")
        elif health in UNKNOWN_HEALTH:
            rep.add("atencao", "servicos", f"{cluster}/{name}",
                    f"saúde {health}: o CM não está recebendo dados desse serviço")
        if s.get("serviceState") == "STOPPED":
            rep.add("atencao", "servicos", f"{cluster}/{name}", "serviço parado")
    return services, dict(counts)


def _roles(rep: _Report, cluster: str, services: list[dict]) -> tuple[dict, list[dict], list[dict]]:
    counts: Counter = Counter()
    ha_rows: list[dict] = []
    stale_roles: dict[str, Counter] = defaultdict(Counter)
    for s in services:
        svc = s.get("name")
        roles = rep.guard("acesso", f"roles de {cluster}/{svc}", lambda: _items(
            cm().get(f"clusters/{cluster}/services/{svc}/roles", {"view": "full"})))
        if roles is None:
            continue
        ha_groups: dict[str, list[dict]] = defaultdict(list)
        for r in roles:
            counts["total"] += 1
            state, health, rtype = r.get("roleState"), r.get("healthSummary"), r.get("type")
            entity = f"{cluster}/{svc}/{r.get('name')} ({rtype} em {_host_of(r)})"
            if r.get("haStatus") is not None or rtype in ("NAMENODE", "RESOURCEMANAGER"):
                ha_groups[rtype].append(r)
            stale = r.get("configStalenessStatus")
            if stale in ("STALE", "STALE_REFRESHABLE"):
                stale_roles[svc][stale] += 1
            if r.get("maintenanceMode") or s.get("maintenanceMode"):
                if state in STOPPED_STATES or health in ("BAD", "CONCERNING"):
                    rep.add("info", "roles", entity, f"em manutenção ({state}, saúde {health})")
                continue
            if state == "NA" and health not in ("BAD", "CONCERNING"):
                continue  # gateways e roles sem processo
            checks = _bad_checks(r)
            detail = f"; checks: {', '.join(checks)}" if checks else ""
            if state in STOPPED_STATES and s.get("serviceState") != "STOPPED":
                counts["stopped"] += 1
                label = {"STOPPED": "parada", "UNKNOWN": "em estado desconhecido",
                         "HISTORY_NOT_AVAILABLE": "sem histórico de estado"}[state]
                rep.add("critico" if health == "BAD" else "atencao", "roles", entity,
                        f"role {label} ({state}, saúde {health}){detail}")
            elif health == "BAD":
                counts["unhealthy"] += 1
                rep.add("critico", "roles", entity, f"saúde BAD{detail}")
            elif health == "CONCERNING":
                counts["unhealthy"] += 1
                rep.add("atencao", "roles", entity, f"saúde CONCERNING{detail}")
        for rtype, group in ha_groups.items():
            ha_rows.append(_ha_check(rep, cluster, svc, rtype, group))
    stale_rows = [{"service": svc, "restart": c.get("STALE", 0), "refresh": c.get("STALE_REFRESHABLE", 0)}
                  for svc, c in stale_roles.items()]
    return dict(counts), ha_rows, stale_rows


def _ha_check(rep: _Report, cluster: str, svc: str, rtype: str, group: list[dict]) -> dict:
    running = [r for r in group if r.get("roleState") == "STARTED"]
    active = [_host_of(r) for r in running if r.get("haStatus") == "ACTIVE"]
    standby = [_host_of(r) for r in running if r.get("haStatus") == "STANDBY"]
    entity = f"{cluster}/{svc} {rtype}"
    row = {"service": svc, "roleType": rtype, "instances": len(group), "active": active, "standby": standby}
    if len(group) == 1:
        row["ha"] = False
        rep.add("info", "ha", entity, f"sem alta disponibilidade (1 instância em {_host_of(group[0])})")
        return row
    row["ha"] = True
    if not active:
        rep.add("critico", "ha", entity, f"nenhuma instância ACTIVE entre {len(group)} (standby: "
                                         f"{', '.join(standby) or 'nenhum'})")
    elif len(active) > 1:
        rep.add("critico", "ha", entity, f"{len(active)} instâncias ACTIVE ao mesmo tempo: {', '.join(active)}")
    elif not standby:
        rep.add("atencao", "ha", entity, f"ativo em {active[0]}, mas nenhum standby disponível")
    return row


def _hosts(rep: _Report, cluster: str, heartbeat_stale_seconds: int) -> tuple[dict, dict]:
    hosts = _items(cm().get(f"clusters/{cluster}/hosts", {"view": "full"}))
    now = datetime.now(timezone.utc)
    counts: Counter = Counter()
    key_checks: dict[str, Counter] = {k: Counter() for k in KEY_HOST_CHECKS}
    for h in hosts:
        name = h.get("hostname") or h.get("hostId")
        counts["total"] += 1
        if h.get("maintenanceMode"):
            counts["maintenance"] += 1
            continue
        for hc in h.get("healthChecks") or []:
            for key, needles in KEY_HOST_CHECKS.items():
                if any(n in (hc.get("name") or "") for n in needles) and hc.get("summary") != "DISABLED":
                    key_checks[key][hc.get("summary")] += 1
        health = h.get("healthSummary")
        counts[health or "UNKNOWN"] += 1
        checks = _bad_checks(h)
        detail = f"; checks: {', '.join(checks)}" if checks else ""
        if health == "BAD":
            rep.add("critico", "hosts", name, f"saúde BAD{detail}")
        elif health == "CONCERNING":
            rep.add("atencao", "hosts", name, f"saúde CONCERNING{detail}")
        hb = _parse_ts(h.get("lastHeartbeat"))
        if hb is None:
            counts["noHeartbeat"] += 1
            rep.add("atencao", "hosts", name, "o CM não informa o último heartbeat do agente")
        elif (age := (now - hb).total_seconds()) > heartbeat_stale_seconds:
            counts["staleHeartbeat"] += 1
            rep.add("critico", "hosts", name,
                    f"agente sem heartbeat há {round(age / 60, 1)} min (limite {heartbeat_stale_seconds}s)")
    return dict(counts), {k: dict(v) for k, v in key_checks.items() if v}


def _management(rep: _Report) -> dict:
    svc = cm().get("cm/service", {"view": "full"})
    out: dict[str, Any] = {"state": svc.get("serviceState"), "health": svc.get("healthSummary"), "roles": []}
    if svc.get("serviceState") not in ("STARTED", None):
        rep.add("critico", "management", "mgmt",
                f"Cloudera Management Service {svc.get('serviceState')}: todo o monitoramento está cego")
    roles = _items(cm().get("cm/service/roles", {"view": "full"}))
    present = set()
    for r in roles:
        rtype = r.get("type")
        present.add(rtype)
        state, health = r.get("roleState"), r.get("healthSummary")
        out["roles"].append({"type": rtype, "host": _host_of(r), "state": state, "health": health})
        level, impact = MGMT_ROLES.get(rtype, ("atencao", f"{rtype} com problema."))
        checks = _bad_checks(r)
        detail = f" Checks: {', '.join(checks)}." if checks else ""
        if state != "STARTED" or health == "BAD":
            rep.add(level, "management", f"mgmt/{rtype}", f"{state}, saúde {health}. {impact}{detail}")
        elif health == "CONCERNING":
            rep.add("atencao", "management", f"mgmt/{rtype}", f"saúde CONCERNING.{detail}")
    for rtype in ("SERVICEMONITOR", "HOSTMONITOR", "EVENTSERVER"):
        if rtype not in present:
            rep.add("critico", "management", f"mgmt/{rtype}", f"role não existe. {MGMT_ROLES[rtype][1]}")
    return out


def _fetch_alerts(hours: float, max_alerts: int) -> tuple[list[dict], int | None]:
    since = _iso(datetime.now(timezone.utc) - timedelta(hours=hours))
    events: list[dict] = []
    total = None
    while len(events) < max_alerts:
        page = cm().get("events", {"query": f"alert==true;timeReceived=ge={since}",
                                   "maxResults": min(100, max_alerts - len(events)),
                                   "resultOffset": len(events)})
        batch = _items(page)
        if isinstance(page, dict) and page.get("totalResults") is not None:
            total = int(page["totalResults"])
        events.extend(batch)
        if len(batch) < 100 or (total is not None and len(events) >= total):
            break
    return events, total


def _alerts(rep: _Report, hours: float, max_alerts: int, latest: int) -> dict:
    events, total = _fetch_alerts(hours, max_alerts)
    by_sev: Counter = Counter()
    by_svc: dict[str, Counter] = defaultdict(Counter)
    rows = []
    for e in events:
        attrs = {a.get("name"): a.get("values") or [] for a in e.get("attributes", [])}
        svc = (attrs.get("SERVICE") or attrs.get("HOSTS") or ["ClouderaManager"])[0]
        sev = e.get("severity") or "?"
        by_sev[sev] += 1
        by_svc[svc][sev] += 1
        rows.append({"time": e.get("timeOccurred"), "severity": sev, "service": svc,
                     "summary": ((attrs.get("ALERT_SUMMARY") or [None])[0] or e.get("content") or "")[:200]})
    rows.sort(key=lambda r: r["time"] or "", reverse=True)
    crit = by_sev.get("CRITICAL", 0)
    if crit:
        worst = max(by_svc.items(), key=lambda kv: kv[1].get("CRITICAL", 0))[0]
        rep.add("atencao", "alertas", "eventos",
                f"{crit} alerta(s) CRITICAL nas últimas {hours:g}h (mais em {worst}); veja se já foram resolvidos")
    return {
        "windowHours": hours,
        "total": total if total is not None else len(events),
        "analyzed": len(events),
        "bySeverity": dict(by_sev),
        "byService": sorted(({"service": s, **dict(c), "total": sum(c.values())} for s, c in by_svc.items()),
                            key=lambda r: r["total"], reverse=True),
        "latest": rows[:latest],
    }


def _stale_services(rep: _Report, cluster: str, services: list[dict], stale_roles: list[dict]) -> dict:
    rows = []
    roles_by_svc = {r["service"]: r for r in stale_roles}
    for s in services:
        svc = s.get("name")
        cfg, client = s.get("configStalenessStatus"), s.get("clientConfigStalenessStatus")
        rr = roles_by_svc.get(svc, {})
        if cfg in ("STALE", "STALE_REFRESHABLE") or client == "STALE" or rr:
            row = {"service": svc, "config": cfg, "clientConfig": client,
                   "rolesNeedRestart": rr.get("restart", 0), "rolesNeedRefresh": rr.get("refresh", 0)}
            rows.append(row)
            actions = []
            if cfg == "STALE" or row["rolesNeedRestart"]:
                actions.append(f"restart ({row['rolesNeedRestart']} role(s))" if row["rolesNeedRestart"] else "restart")
            if cfg == "STALE_REFRESHABLE" or row["rolesNeedRefresh"]:
                actions.append("refresh")
            if client == "STALE":
                actions.append("deploy client configuration")
            rep.add("atencao", "restart", f"{cluster}/{svc}",
                    "configuração alterada e ainda não aplicada: pendente " + ", ".join(actions))
    return {"services": rows, "count": len(rows)}


def _commands(rep: _Report, cluster: str | None, stuck_minutes: float) -> dict:
    data = collect_running_commands(cluster, stuck_minutes, include_roles=False)
    for cmd in data["commands"]:
        if not cmd["stuck"]:
            continue
        where = "/".join(cmd[k] for k in ("cluster", "service", "role") if cmd.get(k)) or cmd["scope"]
        step = cmd.get("currentStep")
        step_txt = ""
        if step:
            place = step.get("role") or step.get("host")
            step_txt = (f"; etapa atual: {step.get('name')}" + (f" em {place}" if place else "")
                        + (f" há {step['elapsedMin']} min" if step.get("elapsedMin") is not None else ""))
        rep.add("atencao", "comandos", f"{where} #{cmd.get('id')}",
                f"comando '{cmd.get('name')}' rodando há {cmd.get('elapsedMin')} min "
                f"(limite {stuck_minutes:g} min){step_txt}")
    if data.get("errors"):
        rep.add("atencao", "acesso", "comandos",
                f"não foi possível ler {len(data['errors'])} lista(s) de comandos: {data['errors'][0]}"[:300])
    return data


# ---------------------------------------------------------------------------
# Relatório
# ---------------------------------------------------------------------------

def build_health_report(cluster: str | None = None, hours: float = 24, heartbeat_stale_seconds: int = 120,
                        max_alerts: int = 500, latest_alerts: int = 10,
                        stuck_command_minutes: float = 60) -> dict:
    rep = _Report()
    c = cm()
    report: dict[str, Any] = {"generatedAt": _iso(datetime.now(timezone.utc))}
    report["cm"] = {"apiVersion": c.version,
                    "version": (rep.guard("acesso", "versão do CM", c.get, "cm/version") or {}).get("version")}

    clusters = [cluster] if cluster else [x.get("name") for x in _items(c.get("clusters"))]
    report["clusters"] = []
    totals: dict[str, Counter] = defaultdict(Counter)
    for name in clusters:
        entry: dict[str, Any] = {"name": name}
        res = rep.guard("acesso", f"serviços de {name}", _services, rep, name)
        services, svc_counts = res if res else ([], {})
        entry["services"] = svc_counts
        role_counts, ha_rows, stale_roles = _roles(rep, name, services)
        entry["roles"], entry["highAvailability"] = role_counts, ha_rows
        res = rep.guard("acesso", f"hosts de {name}", _hosts, rep, name, heartbeat_stale_seconds)
        entry["hosts"], entry["hostKeyChecks"] = res if res else ({}, {})
        entry["pendingRestart"] = _stale_services(rep, name, services, stale_roles)
        for key in ("services", "roles", "hosts"):
            totals[key].update(entry[key])
        totals["pendingRestart"]["services"] += entry["pendingRestart"]["count"]
        report["clusters"].append(entry)

    report["managementService"] = rep.guard("acesso", "Management Service", _management, rep)
    report["alerts"] = rep.guard("acesso", "alertas", _alerts, rep, hours, max_alerts, latest_alerts)
    report["commands"] = rep.guard("acesso", "comandos", _commands, rep, cluster, stuck_command_minutes)

    rep.findings.sort(key=lambda f: LEVELS.index(f["level"]))
    levels = Counter(f["level"] for f in rep.findings)
    status = next((STATUS_BY_LEVEL[lv] for lv in ("critico", "atencao") if levels.get(lv)), "OK")
    report = {
        "overallStatus": status,
        "summary": f"{levels.get('critico', 0)} crítico(s), {levels.get('atencao', 0)} de atenção, "
                   f"{levels.get('info', 0)} informativo(s)",
        **report,
        "counters": {k: dict(v) for k, v in totals.items()}
        | {"alerts": (report["alerts"] or {}).get("bySeverity", {}),
           "commands": {k: (report["commands"] or {}).get(k, 0) for k in ("running", "stuck")},
           "findings": dict(levels)},
        "findings": rep.findings,
    }
    return report


STATUS_LABEL = {"OK": "OK", "ATENCAO": "ATENÇÃO", "CRITICO": "CRÍTICO"}
LEVEL_LABEL = {"critico": "CRÍTICO", "atencao": "ATENÇÃO", "info": "INFO"}


def render_text(r: dict) -> str:
    """Versão para terminal do relatório."""
    cmv = r.get("cm") or {}
    lines = [f"Relatório de saúde - Cloudera Manager {cmv.get('version') or '?'} (API {cmv.get('apiVersion')}) "
             f"- {r.get('generatedAt')}",
             f"STATUS GERAL: {STATUS_LABEL[r['overallStatus']]}  ({r['summary']})", ""]
    if r["findings"]:
        lines.append("Problemas encontrados:")
        for f in r["findings"]:
            lines.append(f"  [{LEVEL_LABEL[f['level']]:8s}] {f['area']:10s} {f['entity']}: {f['message']}")
        lines.append("")
    for c in r["clusters"]:
        s, ro, h = c.get("services", {}), c.get("roles", {}), c.get("hosts", {})
        lines.append(f"Cluster {c['name']}")
        lines.append(f"  Serviços: {s.get('total', 0)} (GOOD {s.get('GOOD', 0)}, CONCERNING {s.get('CONCERNING', 0)}, "
                     f"BAD {s.get('BAD', 0)}, manutenção {s.get('maintenance', 0)})")
        lines.append(f"  Roles:    {ro.get('total', 0)} (paradas {ro.get('stopped', 0)}, "
                     f"com problema {ro.get('unhealthy', 0)})")
        lines.append(f"  Hosts:    {h.get('total', 0)} (GOOD {h.get('GOOD', 0)}, CONCERNING {h.get('CONCERNING', 0)}, "
                     f"BAD {h.get('BAD', 0)}, sem heartbeat {h.get('staleHeartbeat', 0)})")
        if c.get("hostKeyChecks"):
            kc = "; ".join(f"{k}: " + ", ".join(f"{s2} {n}" for s2, n in v.items())
                           for k, v in c["hostKeyChecks"].items())
            lines.append(f"  Checks de host: {kc}")
        for ha in c.get("highAvailability", []):
            if ha["ha"]:
                lines.append(f"  HA {ha['service']} {ha['roleType']}: ativo {', '.join(ha['active']) or '-'}; "
                             f"standby {', '.join(ha['standby']) or '-'}")
            else:
                lines.append(f"  HA {ha['service']} {ha['roleType']}: sem HA (1 instância)")
        lines.append(f"  Restart/refresh pendente: {c['pendingRestart']['count']} serviço(s)")
        lines.append("")
    m = r.get("managementService")
    if m:
        lines.append(f"Management Service: {m['state']}, saúde {m['health']}")
        lines.append("  " + "; ".join(f"{x['type']} {x['state']}/{x['health']}" for x in m["roles"]))
        lines.append("")
    a = r.get("alerts")
    if a:
        sev = ", ".join(f"{k} {v}" for k, v in a["bySeverity"].items()) or "nenhum"
        lines.append(f"Alertas nas últimas {a['windowHours']:g}h: {a['total']} ({sev})")
        if a["byService"]:
            lines.append("  Por serviço: " + ", ".join(f"{x['service']} {x['total']}" for x in a["byService"][:8]))
        for e in a["latest"][:5]:
            lines.append(f"  {e['time']} {e['severity']:13s} {e['service']}: {e['summary']}")
        lines.append("")
    cmds = r.get("commands")
    if cmds:
        lines.append(f"Comandos em andamento: {cmds['running']} (travados {cmds['stuck']}, "
                     f"limite {cmds['stuckMinutes']:g} min)")
        for x in cmds["commands"][:10]:
            where = "/".join(x[k] for k in ("cluster", "service", "role") if x.get(k)) or x["scope"]
            step = (x.get("currentStep") or {}).get("name")
            lines.append(f"  {'TRAVADO ' if x['stuck'] else ''}#{x.get('id')} {x.get('name')} em {where}, "
                         f"há {x.get('elapsedMin')} min" + (f"; etapa atual: {step}" if step else ""))
    return "\n".join(lines).rstrip() + "\n"


@mcp.tool()
def cluster_health_report(cluster: str | None = None, hours: float = 24, output: str = "json",
                          heartbeat_stale_seconds: int = 120, max_alerts: int = 500,
                          latest_alerts: int = 10, stuck_command_minutes: float = 60) -> str:
    """
    Relatório de saúde completo do ambiente com status geral OK / ATENCAO / CRITICO.
    Cobre serviços que não estão GOOD (com os checks que falharam), roles paradas ou com
    problema, alta disponibilidade (Active/Standby de NameNode, ResourceManager etc.), hosts
    (checks de disco, swap e clock offset e agentes sem heartbeat recente), Management Service
    (avisa quando o monitoramento está cego), alertas da janela agrupados por severidade e
    serviço, restart/refresh pendente por configuração desatualizada e comandos em andamento
    em todos os níveis (travados viram atenção).
    Cada problema vem em 'findings' com level critico, atencao ou info.
    cluster: um cluster específico (padrão: todos).
    hours: janela dos alertas.
    output: 'json' (padrão) ou 'text' (relatório legível).
    heartbeat_stale_seconds: idade máxima do último heartbeat do agente (padrão 120 s).
    stuck_command_minutes: a partir de quantos minutos um comando é considerado travado (padrão 60).
    """
    if output not in ("json", "text"):
        return "output deve ser 'json' ou 'text'."
    report = build_health_report(cluster, hours, heartbeat_stale_seconds, max_alerts, latest_alerts,
                                 stuck_command_minutes)
    return _out(render_text(report) if output == "text" else report)
