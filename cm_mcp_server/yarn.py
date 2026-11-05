"""
Tools de monitoramento YARN (somente leitura).

Fontes de dados:
- Cloudera Manager  /yarnApplications   → jobs, usuários, consumo por aplicação (Service Monitor)
- Cloudera Manager  /yarnUtilization    → relatório de utilização por fila/usuário (médias e contenção)
- Cloudera Manager  tsquery YARN_POOL   → uso atual e pico das filas
- ResourceManager   /ws/v1/cluster/...  → árvore do scheduler (capacidade configurada x usada), métricas
                                          do cluster e detalhe de uma aplicação
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from cm_mcp_server.core import _items, _iso, _out, cm, mcp, rm, yarn_service_name

PAGE_SIZE = 100
MB_SEC_TO_GB_H = 1 / (1024 * 3600)
SEC_TO_H = 1 / 3600


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


def _num(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _live(v: Any) -> float | None:
    """Valor de recurso 'atual'. CM e RM usam -1 em apps terminadas para dizer 'não se aplica'."""
    if v is None:
        return None
    n = _num(v)
    return n if n >= 0 else None


def _build_filter(*, executing: bool | None, user: str | None, queue: str | None,
                  extra: str | None) -> str:
    parts = []
    if executing is not None:
        parts.append(f"executing = {'true' if executing else 'false'}")
    if user:
        parts.append(f"user = {user}")
    if queue:
        parts.append(f"pool = {queue}")
    if extra:
        parts.append(f"({extra})")
    return " and ".join(parts)


def _fetch_apps(cluster: str, service: str, *, hours: float, executing: bool | None,
                user: str | None = None, queue: str | None = None, extra: str | None = None,
                max_apps: int = 2000) -> tuple[list[dict], bool]:
    """Busca aplicações no CM com paginação. Retorna (apps, truncado)."""
    now = datetime.now(timezone.utc)
    flt = _build_filter(executing=executing, user=user, queue=queue, extra=extra)
    apps: list[dict] = []
    offset = 0
    while len(apps) < max_apps:
        page = cm().get(f"clusters/{cluster}/services/{service}/yarnApplications", {
            "filter": flt or None,
            "from": _iso(now - timedelta(hours=hours)),
            "to": _iso(now),
            "limit": min(PAGE_SIZE, max_apps - len(apps)),
            "offset": offset,
        })
        batch = page.get("applications", []) if isinstance(page, dict) else []
        apps.extend(batch)
        if len(batch) < PAGE_SIZE:
            return apps, False
        offset += len(batch)
    return apps, True


def _app_summary(a: dict, now: datetime) -> dict:
    start, end = _parse_ts(a.get("startTime")), _parse_ts(a.get("endTime"))
    duration = ((end or now) - start).total_seconds() / 60 if start else None
    attrs = a.get("attributes") or {}
    mem = _live(a.get("allocatedMB"))
    return {k: v for k, v in {
        "applicationId": a.get("applicationId"),
        "name": (a.get("name") or "")[:120],
        "user": a.get("user"),
        "queue": a.get("pool"),
        "type": attrs.get("application_type") or attrs.get("applicationType"),
        "state": a.get("state"),
        "progress": a.get("progress"),
        "startTime": a.get("startTime"),
        "endTime": a.get("endTime"),
        "durationMin": round(duration, 1) if duration is not None else None,
        "currentMemoryGB": round(mem / 1024, 1) if mem is not None else None,
        "currentVcores": a.get("allocatedVCores") if _live(a.get("allocatedVCores")) is not None else None,
        "runningContainers": a.get("runningContainers") if _live(a.get("runningContainers")) is not None else None,
        "memoryGBHours": round(_num(a.get("allocatedMemorySeconds")) * MB_SEC_TO_GB_H, 2),
        "vcoreHours": round(_num(a.get("allocatedVcoreSeconds")) * SEC_TO_H, 2),
        "peakContainerMemoryMB": a.get("containerUsedMemoryMax"),
    }.items() if v not in (None, "")}


SORT_KEYS = {
    "memory": lambda s: s.get("memoryGBHours", 0),
    "vcores": lambda s: s.get("vcoreHours", 0),
    "current_memory": lambda s: s.get("currentMemoryGB") or 0,
    "duration": lambda s: s.get("durationMin") or 0,
    "start": lambda s: s.get("startTime") or "",
}


# ---------------------------------------------------------------------------
# Aplicações (jobs)
# ---------------------------------------------------------------------------

@mcp.tool()
def yarn_list_applications(cluster: str, state: str = "running", hours: float = 1,
                           user: str | None = None, queue: str | None = None,
                           sort_by: str = "current_memory", limit: int = 25,
                           extra_filter: str | None = None, service: str | None = None) -> str:
    """
    Lista jobs YARN (Spark, Hive/Tez, MapReduce, Impala não) a partir do Cloudera Manager.

    state: 'running' (em execução), 'finished' (terminados na janela) ou 'all'.
    hours: janela de tempo para trás a partir de agora.
    user / queue: filtros exatos (queue no formato do CM, ex.: 'root.default').
    sort_by: current_memory | memory (GB-hora acumulado) | vcores (vcore-hora) | duration | start.
    extra_filter: filtro adicional na sintaxe do CM, ex.: "application_duration > 30m"
                  (veja yarn_application_attributes para os atributos aceitos).
    service: nome do serviço YARN (autodetectado se omitido).
    """
    if sort_by not in SORT_KEYS:
        return f"sort_by deve ser um de: {', '.join(SORT_KEYS)}"
    executing = {"running": True, "finished": False, "all": None}.get(state)
    if state not in ("running", "finished", "all"):
        return "state deve ser 'running', 'finished' ou 'all'."
    svc = yarn_service_name(cluster, service)
    apps, truncated = _fetch_apps(cluster, svc, hours=hours, executing=executing,
                                  user=user, queue=queue, extra=extra_filter)
    now = datetime.now(timezone.utc)
    rows = sorted((_app_summary(a, now) for a in apps), key=SORT_KEYS[sort_by], reverse=True)
    return _out({
        "service": svc,
        "window": f"últimas {hours}h",
        "totalMatched": len(apps),
        "truncated": truncated,
        "showing": min(limit, len(rows)),
        "applications": rows[:limit],
    })


@mcp.tool()
def yarn_top_consumers(cluster: str, hours: float = 24, group_by: str = "user",
                       rank_by: str = "memory", top: int = 10, queue: str | None = None,
                       user: str | None = None, max_apps: int = 3000,
                       service: str | None = None) -> str:
    """
    Ranking de quem mais consome recursos do YARN na janela: por usuário (padrão) ou por fila.

    Agrega todas as aplicações (em execução + terminadas) das últimas N horas e soma o consumo
    acumulado (GB-hora de memória e vcore-hora), além do uso instantâneo das que estão rodando.
    group_by: 'user' ou 'queue'.
    rank_by: memory (GB-hora) | vcores (vcore-hora) | apps (quantidade) | current_memory (agora).
    queue / user: restringe a uma fila ou a um usuário.
    Observação: o consumo acumulado de apps que começaram antes da janela entra inteiro.
    """
    if group_by not in ("user", "queue"):
        return "group_by deve ser 'user' ou 'queue'."
    rank_keys = {"memory": "memoryGBHours", "vcores": "vcoreHours", "apps": "apps",
                 "current_memory": "currentMemoryGB"}
    if rank_by not in rank_keys:
        return f"rank_by deve ser um de: {', '.join(rank_keys)}"
    svc = yarn_service_name(cluster, service)
    running, t1 = _fetch_apps(cluster, svc, hours=hours, executing=True, user=user,
                              queue=queue, max_apps=max_apps)
    finished, t2 = _fetch_apps(cluster, svc, hours=hours, executing=False, user=user,
                               queue=queue, max_apps=max_apps)
    seen: set[str] = set()
    agg: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "apps": 0, "running": 0, "failedOrKilled": 0, "memoryGBHours": 0.0, "vcoreHours": 0.0,
        "currentMemoryGB": 0.0, "currentVcores": 0, "_other": defaultdict(float), "_heaviest": None,
    })
    other_dim = "queue" if group_by == "user" else "user"
    for a in running + finished:
        app_id = a.get("applicationId")
        if app_id in seen:
            continue
        seen.add(app_id)
        key = (a.get("user") if group_by == "user" else a.get("pool")) or "(desconhecido)"
        g = agg[key]
        mem_gbh = _num(a.get("allocatedMemorySeconds")) * MB_SEC_TO_GB_H
        g["apps"] += 1
        g["memoryGBHours"] += mem_gbh
        g["vcoreHours"] += _num(a.get("allocatedVcoreSeconds")) * SEC_TO_H
        if not a.get("endTime"):
            g["running"] += 1
            g["currentMemoryGB"] += (_live(a.get("allocatedMB")) or 0) / 1024
            g["currentVcores"] += int(_live(a.get("allocatedVCores")) or 0)
        if (a.get("state") or "").upper() in ("FAILED", "KILLED"):
            g["failedOrKilled"] += 1
        g["_other"][(a.get("pool") if group_by == "user" else a.get("user")) or "?"] += mem_gbh
        if g["_heaviest"] is None or mem_gbh > g["_heaviest"][1]:
            g["_heaviest"] = (app_id, mem_gbh, (a.get("name") or "")[:80])

    total_mem = sum(g["memoryGBHours"] for g in agg.values()) or 1
    total_vc = sum(g["vcoreHours"] for g in agg.values()) or 1
    rows = []
    for key, g in agg.items():
        others = sorted(g["_other"].items(), key=lambda kv: kv[1], reverse=True)[:3]
        heaviest = g["_heaviest"]
        rows.append({
            group_by: key,
            "apps": g["apps"],
            "running": g["running"],
            "failedOrKilled": g["failedOrKilled"],
            "memoryGBHours": round(g["memoryGBHours"], 2),
            "memorySharePct": round(100 * g["memoryGBHours"] / total_mem, 1),
            "vcoreHours": round(g["vcoreHours"], 2),
            "vcoreSharePct": round(100 * g["vcoreHours"] / total_vc, 1),
            "currentMemoryGB": round(g["currentMemoryGB"], 1),
            "currentVcores": g["currentVcores"],
            f"top{other_dim.capitalize()}s": [k for k, _ in others],
            "heaviestApp": {"id": heaviest[0], "name": heaviest[2],
                            "memoryGBHours": round(heaviest[1], 2)} if heaviest else None,
        })
    rows.sort(key=lambda r: r[rank_keys[rank_by]], reverse=True)
    return _out({
        "service": svc,
        "window": f"últimas {hours}h",
        "groupBy": group_by,
        "rankBy": rank_by,
        "appsAnalyzed": len(seen),
        "truncated": t1 or t2,
        "totals": {"memoryGBHours": round(total_mem, 2) if agg else 0,
                   "vcoreHours": round(total_vc, 2) if agg else 0},
        "ranking": rows[:top],
    })


@mcp.tool()
def yarn_application_attributes(cluster: str, service: str | None = None) -> str:
    """Atributos aceitos no filtro de aplicações YARN do CM (para usar em extra_filter)."""
    svc = yarn_service_name(cluster, service)
    attrs = _items(cm().get(f"clusters/{cluster}/services/{svc}/yarnApplications/attributes"))
    return _out([
        {k: v for k, v in {
            "name": a.get("name"), "type": a.get("type"), "displayName": a.get("displayName"),
            "description": (a.get("description") or "")[:160],
        }.items() if v}
        for a in attrs
    ])


@mcp.tool()
def yarn_get_application(cluster: str, application_id: str, service: str | None = None) -> str:
    """
    Detalhe de uma aplicação direto do ResourceManager: estado, fila, recursos, tempo de espera
    e 'diagnostics' (a mensagem de erro do YARN quando o job falha).
    Requer acesso ao RM (veja YARN_RM_* no README).
    """
    app = rm(cluster, service).get(f"cluster/apps/{application_id}").get("app", {})
    keep = ("id", "name", "user", "queue", "state", "finalStatus", "progress", "applicationType",
            "startedTime", "finishedTime", "elapsedTime", "allocatedMB", "allocatedVCores",
            "runningContainers", "memorySeconds", "vcoreSeconds", "queueUsagePercentage",
            "clusterUsagePercentage", "priority", "amHostHttpAddress", "trackingUrl", "diagnostics")
    live = ("allocatedMB", "allocatedVCores", "runningContainers")
    return _out({k: app.get(k) for k in keep
                 if app.get(k) not in (None, "") and not (k in live and _live(app.get(k)) is None)})


# ---------------------------------------------------------------------------
# Filas
# ---------------------------------------------------------------------------

POOL_METRICS_CORE = ["allocated_memory_mb", "allocated_vcores", "allocated_containers",
                     "pending_memory_mb", "pending_vcores", "pending_containers",
                     "apps_running", "apps_pending", "available_memory_mb", "available_vcores"]
POOL_METRICS_FAIR = ["fair_share_mb", "fair_share_vcores", "steady_fair_share_mb",
                     "steady_fair_share_vcores", "min_share_mb", "max_share_mb"]


def _pool_timeseries(service: str, metrics: list[str], minutes: int) -> tuple[dict, list]:
    now = datetime.now(timezone.utc)
    resp = cm().get("timeseries", {
        "query": f"select {', '.join(metrics)} where category = YARN_POOL and serviceName = {service}",
        "from": _iso(now - timedelta(minutes=minutes)),
        "to": _iso(now),
        "desiredRollup": "RAW",
        "mustUseDesiredRollup": "false",
    })
    pools: dict[str, dict] = defaultdict(dict)
    warnings: list = []
    for item in _items(resp):
        warnings.extend(item.get("warnings") or [])
        warnings.extend(item.get("errors") or [])
        for ts in item.get("timeSeries", []):
            md = ts.get("metadata", {})
            attrs = md.get("attributes") or {}
            name = attrs.get("poolName") or attrs.get("queueName") or md.get("entityName") or "?"
            vals = [p.get("value") for p in ts.get("data", []) if p.get("value") is not None]
            if vals:
                pools[name][md.get("metricName")] = {"last": vals[-1], "max": max(vals)}
    return pools, warnings


@mcp.tool()
def yarn_queue_usage(cluster: str, minutes: int = 60, service: str | None = None) -> str:
    """
    Uso atual e pico de cada fila YARN (pela série temporal do CM, categoria YARN_POOL):
    memória e vcores alocados, pendentes e disponíveis, apps rodando/pendentes e, se o
    Fair Scheduler estiver em uso, fair share. 'pending' > 0 indica fila sob contenção.
    minutes: janela para calcular o pico.
    """
    svc = yarn_service_name(cluster, service)
    pools, warnings = _pool_timeseries(svc, POOL_METRICS_CORE, minutes)
    try:  # métricas de fair share só existem com Fair Scheduler
        fair, _ = _pool_timeseries(svc, POOL_METRICS_FAIR, minutes)
        for name, m in fair.items():
            pools[name].update(m)
    except Exception:  # noqa: BLE001
        pass

    rows = []
    for name, m in pools.items():
        def last(k: str) -> Any:
            return (m.get(k) or {}).get("last")

        def peak(k: str) -> Any:
            return (m.get(k) or {}).get("max")

        row = {
            "queue": name,
            "allocatedMemoryGB": round(_num(last("allocated_memory_mb")) / 1024, 1),
            "peakAllocatedMemoryGB": round(_num(peak("allocated_memory_mb")) / 1024, 1),
            "allocatedVcores": last("allocated_vcores"),
            "peakAllocatedVcores": peak("allocated_vcores"),
            "pendingMemoryGB": round(_num(last("pending_memory_mb")) / 1024, 1),
            "pendingContainers": last("pending_containers"),
            "peakPendingContainers": peak("pending_containers"),
            "appsRunning": last("apps_running"),
            "appsPending": last("apps_pending"),
            "availableMemoryGB": round(_num(last("available_memory_mb")) / 1024, 1)
            if last("available_memory_mb") is not None else None,
        }
        if last("fair_share_mb") is not None:
            fair_mb = _num(last("fair_share_mb"))
            row["fairShareMemoryGB"] = round(fair_mb / 1024, 1)
            row["usageVsFairSharePct"] = round(100 * _num(last("allocated_memory_mb")) / fair_mb, 1) if fair_mb else None
        if last("max_share_mb") is not None:
            row["maxShareMemoryGB"] = round(_num(last("max_share_mb")) / 1024, 1)
        rows.append({k: v for k, v in row.items() if v is not None})
    rows.sort(key=lambda r: r.get("allocatedMemoryGB", 0), reverse=True)
    return _out({
        "service": svc,
        "window": f"últimos {minutes} min",
        "queues": rows or "Sem dados de YARN_POOL na janela (o Service Monitor coleta essas métricas?).",
        **({"warnings": warnings[:5]} if warnings else {}),
    })


def _flatten_capacity(q: dict, out: list, parent: str = "") -> None:
    path = q.get("queuePath") or (f"{parent}.{q.get('queueName')}" if parent else q.get("queueName", "root"))
    used = q.get("resourcesUsed") or {}
    users = ((q.get("users") or {}).get("user")) or []
    children = ((q.get("queues") or {}).get("queue")) or []
    row = {k: v for k, v in {
        "queue": path,
        "leaf": not children,
        "state": q.get("state"),
        "capacityPct": q.get("capacity"),
        "absoluteCapacityPct": q.get("absoluteCapacity"),
        "absoluteMaxCapacityPct": q.get("absoluteMaxCapacity"),
        "usedCapacityPct": q.get("usedCapacity"),
        "absoluteUsedCapacityPct": q.get("absoluteUsedCapacity"),
        "usedMemoryGB": round(_num(used.get("memory")) / 1024, 1) if used else None,
        "usedVcores": used.get("vCores"),
        "apps": q.get("numApplications"),
        "activeApps": q.get("numActiveApplications"),
        "pendingApps": q.get("numPendingApplications"),
        "containers": q.get("numContainers"),
        "userLimitPct": q.get("userLimit"),
        "userLimitFactor": q.get("userLimitFactor"),
        "users": [
            {"user": u.get("username"),
             "memoryGB": round(_num((u.get("resourcesUsed") or {}).get("memory")) / 1024, 1),
             "vcores": (u.get("resourcesUsed") or {}).get("vCores"),
             "activeApps": u.get("numActiveApplications"),
             "pendingApps": u.get("numPendingApplications")}
            for u in sorted(users, key=lambda u: _num((u.get("resourcesUsed") or {}).get("memory")), reverse=True)
        ] or None,
    }.items() if v is not None}
    out.append(row)
    for child in children:
        _flatten_capacity(child, out, path)


def _flatten_fair(q: dict, out: list) -> None:
    def gb(r: dict | None) -> float | None:
        return round(_num((r or {}).get("memory")) / 1024, 1) if r else None

    children = ((q.get("childQueues") or {}).get("queue")) or []
    used, fair = q.get("usedResources"), q.get("fairResources")
    out.append({k: v for k, v in {
        "queue": q.get("queueName"),
        "leaf": not children,
        "usedMemoryGB": gb(used),
        "usedVcores": (used or {}).get("vCores"),
        "fairShareMemoryGB": gb(fair),
        "steadyFairShareMemoryGB": gb(q.get("steadyFairResources")),
        "minMemoryGB": gb(q.get("minResources")),
        "maxMemoryGB": gb(q.get("maxResources")),
        "usageVsFairSharePct": round(100 * _num((used or {}).get("memory")) / _num(fair.get("memory")), 1)
        if fair and _num(fair.get("memory")) else None,
        "activeApps": q.get("numActiveApps"),
        "pendingApps": q.get("numPendingApps"),
        "schedulingPolicy": q.get("schedulingPolicy"),
    }.items() if v is not None})
    for child in children:
        _flatten_fair(child, out)


@mcp.tool()
def yarn_scheduler_queues(cluster: str, only_leaf: bool = False, service: str | None = None) -> str:
    """
    Árvore de filas direto do ResourceManager: capacidade configurada x usada (Capacity Scheduler,
    padrão no CDP 7) ou fair share x uso (Fair Scheduler), apps ativos/pendentes e o consumo de
    cada usuário dentro de cada fila. Requer acesso ao RM (veja YARN_RM_* no README).
    only_leaf: mostra só as filas folha (onde os jobs rodam).
    """
    info = rm(cluster, service).get("cluster/scheduler").get("scheduler", {}).get("schedulerInfo", {})
    stype = info.get("type", "")
    rows: list[dict] = []
    if stype == "fairScheduler":
        _flatten_fair(info.get("rootQueue") or {}, rows)
    else:
        _flatten_capacity(info, rows)
    if only_leaf:
        rows = [r for r in rows if r.get("leaf")]
    return _out({"schedulerType": stype or "capacityScheduler", "queues": rows})


@mcp.tool()
def yarn_cluster_metrics(cluster: str, service: str | None = None) -> str:
    """
    Fotografia do cluster YARN no ResourceManager: memória/vcores totais, alocados e livres (%),
    apps rodando/pendentes, containers pendentes e NodeManagers ativos/com problema.
    Requer acesso ao RM (veja YARN_RM_* no README).
    """
    m = rm(cluster, service).get("cluster/metrics").get("clusterMetrics", {})
    total_mb, total_vc = _num(m.get("totalMB")), _num(m.get("totalVirtualCores"))
    return _out({
        "memory": {"totalGB": round(total_mb / 1024, 1),
                   "allocatedGB": round(_num(m.get("allocatedMB")) / 1024, 1),
                   "reservedGB": round(_num(m.get("reservedMB")) / 1024, 1),
                   "availableGB": round(_num(m.get("availableMB")) / 1024, 1),
                   "usedPct": round(100 * _num(m.get("allocatedMB")) / total_mb, 1) if total_mb else None},
        "vcores": {"total": m.get("totalVirtualCores"),
                   "allocated": m.get("allocatedVirtualCores"),
                   "available": m.get("availableVirtualCores"),
                   "usedPct": round(100 * _num(m.get("allocatedVirtualCores")) / total_vc, 1) if total_vc else None},
        "apps": {k: m.get(k) for k in ("appsRunning", "appsPending", "appsSubmitted",
                                        "appsCompleted", "appsFailed", "appsKilled")},
        "containers": {"allocated": m.get("containersAllocated"),
                       "pending": m.get("containersPending"),
                       "reserved": m.get("containersReserved")},
        "nodes": {k: m.get(k) for k in ("activeNodes", "unhealthyNodes", "lostNodes",
                                         "decommissionedNodes", "rebootedNodes")},
    })


# ---------------------------------------------------------------------------
# Relatório de utilização (CM)
# ---------------------------------------------------------------------------

@mcp.tool()
def yarn_utilization_report(cluster: str, days: int = 7, tenant_type: str = "POOL",
                            start_hour: int = 0, end_hour: int = 23,
                            days_of_week: str | None = None, service: str | None = None) -> str:
    """
    Relatório de utilização do YARN pelo CM (o mesmo do 'Cluster Utilization Report'):
    uso médio e de pico do cluster e, por fila (tenant_type='POOL') ou usuário ('USER'),
    alocação e uso médios de memória/vcores, capacidade ociosa e espera em períodos de contenção.
    start_hour/end_hour: recorte do horário (0-23), ex.: horário comercial 8 a 18.
    days_of_week: ex.: '1,2,3,4,5' (1 = segunda) para só dias úteis.
    """
    tenant_type = tenant_type.upper()
    if tenant_type not in ("POOL", "USER"):
        return "tenant_type deve ser 'POOL' ou 'USER'."
    svc = yarn_service_name(cluster, service)
    now = datetime.now(timezone.utc)
    params: dict[str, Any] = {
        "from": _iso(now - timedelta(days=days)),
        "to": _iso(now),
        "tenantType": tenant_type,
        "startHourOfDay": start_hour,
        "endHourOfDay": end_hour,
    }
    if days_of_week:
        # o CM aceita o parâmetro repetido: daysOfWeek=1&daysOfWeek=2...
        params["daysOfWeek"] = [d.strip() for d in days_of_week.split(",") if d.strip()]
    r = cm().get(f"clusters/{cluster}/services/{svc}/yarnUtilization", params)
    if r.get("errorMessage"):
        return _out({"errorMessage": r["errorMessage"],
                     "hint": "O relatório depende do Cluster Utilization Report do CM estar habilitado "
                             "(licença Enterprise / Service Monitor coletando YARN)."})
    tenants = []
    for t in (r.get("tenantUtilizations") or {}).get("tenantUtilizationList") or \
             _items(r.get("tenantUtilizations") or {}) or []:
        tenants.append({k: (round(v, 2) if isinstance(v, float) else v) for k, v in t.items()})
    tenants.sort(key=lambda t: _num(t.get("avgYarnMemoryAllocation")), reverse=True)
    summary = {k: (round(v, 2) if isinstance(v, float) else v) for k, v in r.items()
               if k not in ("tenantUtilizations", "errorMessage")}
    return _out({
        "service": svc,
        "window": f"últimos {days} dias, {start_hour}h–{end_hour}h"
                  + (f", dias {days_of_week}" if days_of_week else ""),
        "tenantType": tenant_type,
        "cluster": summary,
        "tenants": tenants,
        "notes": "Memória em MB. 'DuringContention' = médias nos momentos com containers pendentes; "
                 "avgYarnContainerWaitRatio alto indica fila sem recurso suficiente.",
    })
