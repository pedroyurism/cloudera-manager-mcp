"""
Tools de análise de queries Impala (somente leitura).

Fonte: Cloudera Manager  /clusters/{c}/services/{impala}/impalaQueries
  - lista com atributos por query (duração, memória, spill, OOM, estatísticas, tempos de espera)
  - /impalaQueries/{queryId} → profile completo (plano, ExecSummary, contadores por nó)

Foco: identificar e ranquear queries com
  1. alta duração (e onde o tempo foi gasto),
  2. consumo excessivo de memória (incluindo OOM e estimativa errada),
  3. spill-to-disk,
  4. falhas e problemas de planejamento (análise/sintaxe/permissão, rejeição na admissão,
     estatísticas ausentes, planejamento lento, estimativa de memória muito distante do real).
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from cm_mcp_server.core import _items, _iso, _out, cm, mcp

PAGE_SIZE = 1000
GB = 1024 ** 3
MB = 1024 ** 2

# Mensagens de query_status que indicam falha antes da execução (análise, plano ou admissão).
PLANNING_FAILURE_RE = re.compile(
    r"AnalysisException|ParseException|Syntax error|AuthorizationException|"
    r"does not have privileges|UnsupportedFeatureException|TableNotFound|"
    r"Could not resolve (table|column|path)|(Table|Database) does not exist|"
    r"Rejected query from pool|Admission for query exceeded timeout|queue (is )?full",
    re.IGNORECASE,
)
ADMISSION_RE = re.compile(r"Rejected query from pool|Admission for query exceeded timeout|queue (is )?full",
                          re.IGNORECASE)
OOM_RE = re.compile(r"Memory limit exceeded|Failed to (allocate|get minimum memory reservation)|"
                    r"could not allocate|Out of memory", re.IGNORECASE)

# Onde a query passou o tempo: atributo de % do CM → rótulo legível.
TIME_BREAKDOWN = {
    "planning_wait_time_percentage": "planejamento",
    "resources_reserved_wait_time_percentage": "espera por reserva de recursos",
    "client_fetch_wait_time_percentage": "cliente buscando resultados",
    "thread_storage_wait_time_percentage": "leitura de storage",
    "thread_network_receive_wait_time_percentage": "rede (recebendo dados)",
    "thread_network_send_wait_time_percentage": "rede (enviando dados)",
    "thread_cpu_time_percentage": "CPU",
}

SEVERITY_ORDER = {"alta": 0, "media": 1, "baixa": 2}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _num(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _bool(v: Any) -> bool:
    return str(v).lower() == "true"


def _gb(b: float) -> float:
    return round(b / GB, 3)


def _one_line(text: str | None, limit: int = 300) -> str:
    s = re.sub(r"\s+", " ", text or "").strip()
    return s if len(s) <= limit else s[:limit] + "…"


def impala_service_name(cluster: str, service: str | None = None) -> str:
    """Nome do serviço Impala do cluster (autodetecta pelo tipo se não informado)."""
    if service:
        return service
    for s in _items(cm().get(f"clusters/{cluster}/services")):
        if s.get("type") == "IMPALA":
            return s["name"]
    raise RuntimeError(f"Nenhum serviço IMPALA encontrado no cluster {cluster}.")


def _build_filter(*parts: str | None) -> str | None:
    p = [x for x in parts if x]
    return " and ".join(f"({x})" if " or " in x.lower() else x for x in p) or None


def _fetch_queries(cluster: str, service: str, *, hours: float, flt: str | None,
                   max_queries: int) -> tuple[list[dict], bool, list[str]]:
    """Busca queries no CM com paginação. Retorna (queries, truncado, avisos do CM)."""
    now = datetime.now(timezone.utc)
    out: list[dict] = []
    warnings: list[str] = []
    offset = 0
    while len(out) < max_queries:
        page = cm().get(f"clusters/{cluster}/services/{service}/impalaQueries", {
            "filter": flt,
            "from": _iso(now - timedelta(hours=hours)),
            "to": _iso(now),
            "limit": min(PAGE_SIZE, max_queries - len(out)),
            "offset": offset,
        })
        batch = page.get("queries", []) if isinstance(page, dict) else []
        for w in (page.get("warnings") or []) if isinstance(page, dict) else []:
            if w not in warnings:
                warnings.append(w)
        out.extend(batch)
        if len(batch) < PAGE_SIZE:
            return out, False, warnings
        offset += len(batch)
    return out, True, warnings


def _bottleneck(attrs: dict, duration_ms: float) -> str | None:
    """Maior parcela do tempo da query, pelos percentuais que o CM calcula."""
    parts = {label: _num(attrs.get(k)) for k, label in TIME_BREAKDOWN.items() if attrs.get(k) is not None}
    if duration_ms > 0 and attrs.get("admission_wait") is not None:
        parts["fila de admissão"] = 100 * _num(attrs.get("admission_wait")) / duration_ms
    if not parts:
        return None
    label, pct = max(parts.items(), key=lambda kv: kv[1])
    # abaixo de 20% nenhuma parcela domina: não aponta gargalo
    return f"{label} ({round(pct)}%)" if pct >= 20 else None


def _analyze(q: dict, *, slow_seconds: float, memory_gb_per_node: float,
             planning_seconds: float) -> dict:
    """Resume uma query e lista os problemas encontrados, com severidade e pontuação."""
    a = q.get("attributes") or {}
    duration_ms = _num(q.get("durationMillis"))
    state = q.get("queryState")
    status = a.get("query_status") or ""
    failed = state == "EXCEPTION"
    peak = _num(a.get("memory_per_node_peak"))
    agg_peak = _num(a.get("memory_aggregate_peak"))
    est = _num(a.get("estimated_per_node_peak_memory"))
    spilled = _num(a.get("memory_spilled"))
    planning_ms = _num(a.get("planning_wait_time"))
    oom = _bool(a.get("oom")) or (failed and bool(OOM_RE.search(status)))
    admission_result = a.get("admission_result") or ""
    issues: list[dict] = []

    def add(kind: str, code: str, severity: str, detail: str, points: int) -> None:
        issues.append({"type": kind, "code": code, "severity": severity, "detail": detail, "_p": points})

    # 1. Duração
    if duration_ms >= slow_seconds * 1000:
        bott = _bottleneck(a, duration_ms)
        add("duracao", "slow", "alta" if duration_ms >= 10 * slow_seconds * 1000 else "media",
            f"durou {round(duration_ms / 1000, 1)}s (limite {slow_seconds:g}s)"
            + (f"; maior parte em {bott}" if bott else ""),
            3 if duration_ms >= 10 * slow_seconds * 1000 else 2)

    # 2. Memória
    if oom:
        add("memoria", "oom", "alta", "falhou por falta de memória (OOM / memory limit exceeded)", 4)
    elif peak >= memory_gb_per_node * GB:
        add("memoria", "high_memory", "alta" if peak >= 2 * memory_gb_per_node * GB else "media",
            f"pico de {_gb(peak)} GB por nó"
            + (f" em {a['memory_per_node_peak_node']}" if a.get("memory_per_node_peak_node") else "")
            + f" (limite {memory_gb_per_node:g} GB)", 2)
    if est > 0 and peak >= 256 * MB and peak > 3 * est and not failed:
        add("memoria", "memory_underestimated", "media",
            f"usou {_gb(peak)} GB por nó, {round(peak / est, 1)}x o estimado pelo planner "
            f"({_gb(est)} GB): estimativa subestimada, risco de OOM sob concorrência", 1)

    # 3. Spill-to-disk
    if spilled > 0:
        add("spill", "spill", "alta" if spilled >= GB else "media",
            f"{_gb(spilled)} GB derramados em disco (spill-to-disk)", 3 if spilled >= GB else 2)

    # 4. Planejamento
    if failed and PLANNING_FAILURE_RE.search(status) and not OOM_RE.search(status):
        if ADMISSION_RE.search(status):
            add("planejamento", "admission_rejected", "alta",
                f"rejeitada na admissão: {_one_line(status, 200)}", 3)
        else:
            add("planejamento", "planning_failure", "alta",
                f"falha de análise/planejamento: {_one_line(status, 200)}", 3)
    elif admission_result.lower().startswith("rejected"):
        add("planejamento", "admission_rejected", "alta", f"rejeitada na admissão: {admission_result}", 3)
    if _bool(a.get("stats_missing")) or _bool(a.get("stats_corrupt")):
        what = "corrompidas" if _bool(a.get("stats_corrupt")) else "ausentes"
        add("planejamento", "stats_missing", "media",
            f"estatísticas de tabela/coluna {what}: o plano pode estar errado "
            "(use impala_query_details para ver as tabelas)", 1)
    if planning_ms >= planning_seconds * 1000:
        add("planejamento", "slow_planning", "media",
            f"planejamento levou {round(planning_ms / 1000, 1)}s (limite {planning_seconds:g}s): "
            "metadados grandes/não carregados ou muitas partições", 1)
    if est >= GB and peak > 0 and est > 10 * peak and state == "FINISHED":
        add("planejamento", "memory_overestimated", "baixa",
            f"planner estimou {_gb(est)} GB por nó e usou {_gb(peak)} GB: superestimativa "
            "prende memória na admissão e pode enfileirar outras queries", 1)

    # Falha de execução que não é de planejamento nem de memória
    if failed and not any(i["code"] in ("oom", "planning_failure", "admission_rejected") for i in issues):
        add("falha", "execution_failure", "media",
            f"falhou na execução: {_one_line(status, 200) or 'sem mensagem'}", 2)

    issues.sort(key=lambda i: SEVERITY_ORDER[i["severity"]])
    score = sum(i.pop("_p") for i in issues)
    row = {
        "queryId": q.get("queryId"),
        "user": q.get("user"),
        "pool": a.get("pool"),
        "database": q.get("database"),
        "type": q.get("queryType"),
        "state": state,
        "startTime": q.get("startTime"),
        "durationSec": round(duration_ms / 1000, 1),
        "perNodePeakMemGB": _gb(peak) if a.get("memory_per_node_peak") is not None else None,
        "aggregatePeakMemGB": _gb(agg_peak) if a.get("memory_aggregate_peak") is not None else None,
        "estimatedPerNodeMemGB": _gb(est) if a.get("estimated_per_node_peak_memory") is not None else None,
        "spilledGB": _gb(spilled) if spilled else None,
        "planningSec": round(planning_ms / 1000, 1) if a.get("planning_wait_time") is not None else None,
        "admissionWaitSec": round(_num(a.get("admission_wait")) / 1000, 1) if a.get("admission_wait") else None,
        "bottleneck": _bottleneck(a, duration_ms),
        "status": _one_line(status, 200) if failed else None,
        "statement": _one_line(q.get("statement")),
        "score": score,
        "issues": issues,
    }
    return {k: v for k, v in row.items() if v not in (None, "", [])}


def _keep(q: dict, include_ddl: bool) -> bool:
    t = (q.get("queryType") or "").upper()
    if t == "SET":
        return False
    return include_ddl or t != "DDL" or q.get("queryState") == "EXCEPTION"


def _normalize_sql(sql: str | None) -> str:
    s = (sql or "").lower()
    s = re.sub(r"'(?:[^'\\]|\\.)*'", "?", s)          # literais de texto
    s = re.sub(r"\b\d+(\.\d+)?\b", "?", s)              # números
    s = re.sub(r"\(\s*\?(\s*,\s*\?)+\s*\)", "(?)", s)   # listas IN (?, ?, ?)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:200]




# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def impala_query_ranking(cluster: str, rank_by: str = "score", hours: float = 24, top: int = 15,
                         user: str | None = None, pool: str | None = None, database: str | None = None,
                         slow_seconds: float = 60, memory_gb_per_node: float = 4,
                         planning_seconds: float = 10, include_ddl: bool = False,
                         extra_filter: str | None = None, max_queries: int = 5000,
                         service: str | None = None) -> str:
    """
    Ranking de queries Impala problemáticas na janela, com o motivo de cada uma.

    rank_by define a ordem. 'score' (padrão): pior primeiro, somando todos os problemas, só
    queries com problema. 'duration': mais longas, com o gargalo de tempo de cada uma.
    'memory': maior pico de memória por nó, OOM no topo. 'spill': maior volume derramado em
    disco, só as que fizeram spill. 'planning': falhas de análise/planejamento, rejeições na
    admissão, estatísticas ausentes, planejamento lento e estimativa de memória muito errada.
    hours: janela para trás a partir de agora.
    user / pool / database: filtros exatos.
    include_ddl: inclui DDL bem-sucedidas (por padrão só QUERY/DML e falhas).
    extra_filter: filtro adicional na sintaxe do CM, ex.: "hdfs_bytes_read > 10g".
    slow_seconds: a partir de quantos segundos a query é considerada longa (padrão 60).
    memory_gb_per_node: pico de memória por nó considerado excessivo, em GB (padrão 4).
    planning_seconds: tempo de planejamento considerado lento, em segundos (padrão 10).
    """
    rank_by = rank_by.lower()
    if rank_by not in ("score", "duration", "memory", "spill", "planning"):
        return "rank_by deve ser um de: score, duration, memory, spill, planning."
    svc = impala_service_name(cluster, service)
    prefilter = {"spill": "memory_spilled > 0"}.get(rank_by)
    flt = _build_filter(f"user = {user}" if user else None, f"pool = {pool}" if pool else None,
                        f"database = {database}" if database else None, prefilter, extra_filter)
    queries, truncated, warnings = _fetch_queries(cluster, svc, hours=hours, flt=flt, max_queries=max_queries)
    th = dict(slow_seconds=slow_seconds, memory_gb_per_node=memory_gb_per_node,
              planning_seconds=planning_seconds)
    rows = [_analyze(q, **th) for q in queries if _keep(q, include_ddl)]

    def has(r: dict, kind: str) -> bool:
        return any(i["type"] == kind for i in r.get("issues", []))

    if rank_by == "score":
        rows = [r for r in rows if r.get("issues")]
        rows.sort(key=lambda r: (r["score"], r.get("durationSec", 0)), reverse=True)
    elif rank_by == "duration":
        rows.sort(key=lambda r: r.get("durationSec", 0), reverse=True)
    elif rank_by == "memory":
        rows.sort(key=lambda r: (any(i["code"] == "oom" for i in r.get("issues", [])),
                                 r.get("perNodePeakMemGB") or 0), reverse=True)
    elif rank_by == "spill":
        rows = [r for r in rows if r.get("spilledGB")]
        rows.sort(key=lambda r: r["spilledGB"], reverse=True)
    else:  # planning
        rows = [r for r in rows if has(r, "planejamento")]
        rows.sort(key=lambda r: (
            min(SEVERITY_ORDER[i["severity"]] for i in r["issues"] if i["type"] == "planejamento"),
            -(r.get("planningSec") or 0)))

    return _out({
        "service": svc,
        "window": f"últimas {hours:g}h",
        "rankBy": rank_by,
        "thresholds": th,
        "queriesScanned": len(queries),
        "truncated": truncated,
        "matched": len(rows),
        "showing": min(top, len(rows)),
        "ranking": rows[:top] or "Nenhuma query encontrada com esse critério na janela.",
        **({"cmWarnings": warnings} if warnings else {}),
    })


@mcp.tool()
def impala_query_issues_summary(cluster: str, hours: float = 24, user: str | None = None,
                                pool: str | None = None, slow_seconds: float = 60,
                                memory_gb_per_node: float = 4, planning_seconds: float = 10,
                                top: int = 10, max_queries: int = 5000,
                                service: str | None = None) -> str:
    """
    Visão geral dos problemas de queries Impala na janela: quantas queries longas, com memória
    excessiva/OOM, com spill e com falhas/problemas de planejamento; quais usuários e pools
    concentram os problemas; queries recorrentes (mesmo texto, literais trocados por ?) que
    mais somam tempo; e os motivos de falha mais comuns.
    slow_seconds: a partir de quantos segundos a query é considerada longa (padrão 60).
    memory_gb_per_node: pico de memória por nó considerado excessivo, em GB (padrão 4).
    planning_seconds: tempo de planejamento considerado lento, em segundos (padrão 10).
    """
    svc = impala_service_name(cluster, service)
    flt = _build_filter(f"user = {user}" if user else None, f"pool = {pool}" if pool else None)
    queries, truncated, warnings = _fetch_queries(cluster, svc, hours=hours, flt=flt, max_queries=max_queries)
    th = dict(slow_seconds=slow_seconds, memory_gb_per_node=memory_gb_per_node,
              planning_seconds=planning_seconds)

    counts: Counter = Counter()
    by_user: dict[str, Counter] = defaultdict(Counter)
    by_pool: dict[str, Counter] = defaultdict(Counter)
    patterns: dict[str, dict[str, Any]] = {}
    failures: Counter = Counter()
    analyzed = 0
    for q in queries:
        if not _keep(q, include_ddl=False):
            continue
        analyzed += 1
        r = _analyze(q, **th)
        kinds = {i["type"] for i in r.get("issues", [])}
        codes = {i["code"] for i in r.get("issues", [])}
        flags = {
            "longas": "slow" in codes,
            "memoriaExcessiva": bool(codes & {"high_memory", "memory_underestimated"}),
            "oom": "oom" in codes,
            "spill": "spill" in codes,
            "falhaDePlanejamento": "planning_failure" in codes,
            "rejeitadaNaAdmissao": "admission_rejected" in codes,
            "estatisticasAusentes": "stats_missing" in codes,
            "planejamentoLento": "slow_planning" in codes,
            "memoriaSuperestimada": "memory_overestimated" in codes,
            "falhaDeExecucao": "execution_failure" in codes,
        }
        for k, v in flags.items():
            if v:
                counts[k] += 1
        if kinds:
            counts["comAlgumProblema"] += 1
        for key, bucket in ((r.get("user") or "?", by_user), (r.get("pool") or "?", by_pool)):
            b = bucket[key]
            b["queries"] += 1
            b["comProblema"] += 1 if kinds else 0
            b["duracaoTotalSec"] += r.get("durationSec", 0)
            b["spillGB"] += r.get("spilledGB", 0)
            b["falhas"] += 1 if r.get("state") == "EXCEPTION" else 0
        if r.get("status"):
            failures[re.sub(r"\b[0-9a-f]{16}:[0-9a-f]{16}\b", "<id>", r["status"])[:160]] += 1
        if kinds:
            key = _normalize_sql(q.get("statement"))
            p = patterns.setdefault(key, {"pattern": key, "executions": 0, "totalSec": 0.0,
                                          "maxSec": 0.0, "maxPerNodePeakMemGB": 0.0, "spillGB": 0.0,
                                          "failures": 0, "users": set(), "issues": Counter(),
                                          "exampleQueryId": r.get("queryId")})
            p["executions"] += 1
            p["totalSec"] += r.get("durationSec", 0)
            p["maxSec"] = max(p["maxSec"], r.get("durationSec", 0))
            p["maxPerNodePeakMemGB"] = max(p["maxPerNodePeakMemGB"], r.get("perNodePeakMemGB") or 0)
            p["spillGB"] += r.get("spilledGB", 0)
            p["failures"] += 1 if r.get("state") == "EXCEPTION" else 0
            p["users"].add(r.get("user"))
            p["issues"].update(kinds)

    def table(bucket: dict[str, Counter], name: str) -> list[dict]:
        rows = [{name: k, **{m: round(v, 2) if isinstance(v, float) else v for m, v in c.items()}}
                for k, c in bucket.items() if c["comProblema"]]
        rows.sort(key=lambda r: (r["comProblema"], r.get("duracaoTotalSec", 0)), reverse=True)
        return rows[:top]

    pats = sorted(patterns.values(), key=lambda p: p["totalSec"], reverse=True)[:top]
    for p in pats:
        p["users"] = sorted(u for u in p["users"] if u)
        p["issues"] = dict(p["issues"])
        for k in ("totalSec", "maxSec", "spillGB"):
            p[k] = round(p[k], 2)

    return _out({
        "service": svc,
        "window": f"últimas {hours:g}h",
        "thresholds": th,
        "queriesScanned": len(queries),
        "queriesAnalyzed": analyzed,
        "truncated": truncated,
        "counts": dict(counts) or "Nenhum problema encontrado na janela.",
        "topUsers": table(by_user, "user"),
        "topPools": table(by_pool, "pool"),
        "recurringProblemQueries": pats,
        "topFailureReasons": [{"reason": k, "count": v} for k, v in failures.most_common(top)],
        "next": "Use impala_query_ranking para ver as queries de cada categoria e "
                "impala_query_details para o diagnóstico de uma query.",
        **({"cmWarnings": warnings} if warnings else {}),
    })


# --- profile -----------------------------------------------------------------

SUMMARY_FIELDS = ("Query Type", "Query State", "Query Status", "Duration", "User", "Default Db",
                  "Request Pool", "Admission result", "Coordinator", "Tables Queried",
                  "Tables Missing Stats", "Tables With Corrupt Table Stats", "Estimated Per-Host Mem",
                  "Per Node Peak Memory Usage", "Cluster Memory Admitted")
SPILL_COUNTERS = ("ScratchBytesWritten", "SpilledPartitions", "SpilledRuns", "NumSpilledRanges")


def _parse_profile(text: str, include_plan: bool, exec_summary_rows: int) -> dict:
    lines = text.splitlines()
    summary: dict[str, str] = {}
    for line in lines[:800]:
        m = re.match(r"^\s{2,6}([A-Z][A-Za-z\- ]+?):\s?(.*)$", line)
        if m and m.group(1) in SUMMARY_FIELDS and m.group(1) not in summary and m.group(2).strip():
            summary[m.group(1)] = m.group(2).strip()

    plan_warnings = [ln.strip() + " " + lines[i + 1].strip()
                     for i, ln in enumerate(lines[:-1]) if ln.startswith("WARNING:")]
    columns_missing = sorted({c.strip() for ln in lines if "columns missing stats:" in ln
                              for c in ln.split(":", 1)[1].split(",") if c.strip()})

    # ExecSummary: tabela de operadores (tempo, linhas, memória real x estimada)
    exec_summary: list[str] = []
    for i, ln in enumerate(lines):
        if ln.strip().startswith("ExecSummary:"):
            for row in lines[i + 1:]:
                if not row.strip() or row.lstrip().startswith(("Errors:", "- ")) or re.match(r"^\s{4}\S", row):
                    break
                exec_summary.append(row.rstrip())
            break

    # Contadores de spill por instância (ignora os blocos "Averaged Fragment")
    spill: Counter = Counter()
    spill_ops: Counter = Counter()
    averaged = False
    operator = None
    for ln in lines:
        s = ln.strip()
        if s.startswith("Averaged Fragment"):
            averaged = True
        elif s.startswith(("Fragment ", "Coordinator Fragment")):
            averaged = False
        m_op = re.match(r"^\s*([A-Z_ ]+_NODE|[A-Z ]+) \(id=(\d+)\)", ln)
        if m_op:
            operator = f"{m_op.group(2)}:{m_op.group(1).strip()}"
        m = re.match(r"^\s*- (\w+): .*\((\d+)\)\s*$", ln)
        if m and m.group(1) in SPILL_COUNTERS and not averaged:
            v = int(m.group(2))
            spill[m.group(1)] += v
            if v and operator:
                spill_ops[operator] += v if m.group(1) == "ScratchBytesWritten" else 0

    # "Errors:" do resumo; continuações só contam se estiverem mais recuadas que o rótulo
    errors = []
    for i, ln in enumerate(lines):
        if ln.strip().startswith("Errors:"):
            indent = len(ln) - len(ln.lstrip())
            first = ln.split(":", 1)[1].strip()
            if first:
                errors.append(first)
            for nxt in lines[i + 1:i + 6]:
                if nxt.strip() and len(nxt) - len(nxt.lstrip()) > indent and not nxt.strip().startswith("- "):
                    errors.append(nxt.strip())
                else:
                    break
            break

    # Timelines do planner e da query: eventos mais recuados que o cabeçalho,
    # com ("- Evento: t") ou sem ("Evento: t", Impala 4.x) o hífen
    timeline = []
    seen_headers: set[str] = set()
    for i, ln in enumerate(lines):
        head = re.match(r"^(\s*)(Planner Timeline|Query Timeline)\b", ln)
        if not head or head.group(2) in seen_headers:
            continue
        seen_headers.add(head.group(2))
        indent = len(head.group(1))
        for nxt in lines[i + 1:i + 30]:
            m = re.match(r"^(\s*)(?:- )?([A-Z][^:]{2,60}): (.+)$", nxt)
            if not m or len(m.group(1)) <= indent:
                break
            if len(m.group(1)) == indent + 2 or nxt.strip().startswith("- "):
                timeline.append(f"{m.group(2)}: {m.group(3)}")

    out: dict[str, Any] = {
        "summary": summary,
        "planWarnings": plan_warnings or None,
        "columnsMissingStats": columns_missing or None,
        "errors": errors or None,
        "spillCounters": {k: v for k, v in spill.items() if v} or None,
        "spillByOperatorBytes": dict(spill_ops.most_common(5)) or None,
        "execSummary": exec_summary[:exec_summary_rows + 2] or None,
        "timeline": timeline[:40] or None,
    }
    if include_plan:
        try:
            a = lines.index("----------------")
            b = lines.index("----------------", a + 1)
            out["plan"] = "\n".join(lines[a + 1:b][:150])
        except ValueError:
            pass
    return {k: v for k, v in out.items() if v}


def _recommendations(summary: dict, parsed: dict, row: dict | None) -> list[str]:
    recs = []
    missing = summary.get("Tables Missing Stats") or (
        parsed.get("planWarnings") and parsed["planWarnings"][0].split("statistics.", 1)[-1].strip())
    if missing:
        recs.append("Rode COMPUTE STATS (ou COMPUTE INCREMENTAL STATS em tabelas particionadas) em: "
                    f"{missing}. Sem estatísticas o planner erra a ordem dos joins e a memória.")
    codes = {i["code"] for i in (row or {}).get("issues", [])}
    status = summary.get("Query Status", "")
    if parsed.get("spillCounters") or "spill" in codes:
        ops = ", ".join(parsed.get("spillByOperatorBytes") or {}) or "HASH JOIN/AGGREGATE/SORT"
        recs.append(f"Spill-to-disk ({ops}): filtre antes de juntar, reduza colunas, confirme as "
                    "estatísticas e, se necessário, aumente MEM_LIMIT da query ou o limite do pool.")
    if "oom" in codes or OOM_RE.search(status):
        recs.append("Falta de memória: compare 'Peak Mem' x 'Est. Peak Mem' no ExecSummary para achar o "
                    "operador que estourou; considere MEM_LIMIT maior, partition pruning e joins menores.")
    if "memory_underestimated" in codes:
        recs.append("A memória real ficou bem acima da estimada: atualize as estatísticas; sob concorrência "
                    "essa query pode causar OOM em outras.")
    if "memory_overestimated" in codes:
        recs.append("A estimativa de memória ficou muito acima do uso real: atualize estatísticas ou defina "
                    "MEM_LIMIT para a query não reter memória de admissão à toa.")
    if codes & {"planning_failure", "admission_rejected"} or (
            PLANNING_FAILURE_RE.search(status) and not OOM_RE.search(status)):
        recs.append("A query falhou antes de executar (análise/planejamento/admissão): corrija o SQL, as "
                    "permissões ou o limite do pool conforme a mensagem em 'Query Status'.")
    if "execution_failure" in codes:
        if re.search(r"unreachable impalad", status, re.IGNORECASE):
            recs.append("Um impalad ficou inacessível durante a query: verifique a saúde das roles IMPALAD "
                        "(list_roles com only_unhealthy=True), eventos e memória/CPU do host citado, e "
                        "reexecute depois que o daemon estabilizar.")
        else:
            recs.append("A query falhou durante a execução: veja 'Query Status' e 'errors' acima e confira a "
                        "saúde do Impala com cluster_health_summary.")
    if "slow_planning" in codes:
        recs.append("Planejamento lento: costuma ser metadado não carregado/grande (muitas partições ou "
                    "arquivos). Avalie INVALIDATE/REFRESH direcionados e compactar arquivos pequenos.")
    bott = (row or {}).get("bottleneck", "")
    if "cliente buscando" in bott:
        recs.append("A maior parte do tempo foi o cliente buscando resultados: o problema está no "
                    "consumidor (BI/notebook), não no Impala; limite as linhas ou exporte via CTAS.")
    if "fila de admissão" in bott or "reserva de recursos" in bott:
        recs.append("A query esperou muito na admissão: o pool está saturado; revise concorrência e "
                    "memória máxima do pool ou distribua a carga.")
    return recs


@mcp.tool()
def impala_query_details(cluster: str, query_id: str, include_plan: bool = False,
                         exec_summary_rows: int = 30, lookback_days: int = 7,
                         slow_seconds: float = 60, memory_gb_per_node: float = 4,
                         planning_seconds: float = 10, service: str | None = None) -> str:
    """
    Diagnóstico de uma query Impala pelo profile do CM: resumo (estado, status/erro, duração,
    pool, admissão), tabelas e colunas sem estatísticas, avisos do planner, ExecSummary
    (tempo, linhas e memória real x estimada por operador), contadores de spill por operador,
    timeline de planejamento/execução, os problemas detectados e recomendações.
    include_plan: inclui o plano textual (até 150 linhas).
    lookback_days: até quantos dias atrás procurar os atributos da query na lista do CM.
    """
    svc = impala_service_name(cluster, service)
    base = f"clusters/{cluster}/services/{svc}/impalaQueries"
    detail = cm().get(f"{base}/{query_id}", {"format": "text"})
    text = detail.get("details", "") if isinstance(detail, dict) else str(detail)
    if not text:
        return _out({"queryId": query_id, "error": "O CM não retornou o profile dessa query "
                     "(pode ter expirado do Service Monitor)."})
    parsed = _parse_profile(text, include_plan, exec_summary_rows)

    row = None
    try:
        found, _, _ = _fetch_queries(cluster, svc, hours=lookback_days * 24,
                                     flt=f"query_id = {query_id}", max_queries=1)
        if found:
            row = _analyze(found[0], slow_seconds=slow_seconds, memory_gb_per_node=memory_gb_per_node,
                           planning_seconds=planning_seconds)
    except Exception:  # noqa: BLE001 – o profile sozinho já é útil
        row = None

    return _out({
        "queryId": query_id,
        "service": svc,
        **({"analysis": {k: row[k] for k in ("durationSec", "perNodePeakMemGB", "estimatedPerNodeMemGB",
                                             "spilledGB", "planningSec", "admissionWaitSec",
                                             "bottleneck", "score", "issues") if k in row}} if row else {}),
        **parsed,
        "recommendations": _recommendations(parsed.get("summary", {}), parsed, row) or None,
    })
