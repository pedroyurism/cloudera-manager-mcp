"""
Testes das tools de queries Impala contra o CM simulado (tests/mock_impala.py).

Rodar:  pip install -e '.[dev]'  &&  pytest -q
"""

from __future__ import annotations

from tests import mock_servers
from tests.test_tools import call, ok  # importa o ambiente e sobe os servidores simulados


def ids(data: dict) -> list[str]:
    return [r["queryId"] for r in data["ranking"]]


def codes(row: dict) -> set[str]:
    return {i["code"] for i in row.get("issues", [])}


def by_id(data: dict) -> dict[str, dict]:
    return {r["queryId"]: r for r in data["ranking"]}


def test_ranking_score_lists_only_problems_worst_first():
    data = ok("impala_query_ranking", cluster="Cluster1", top=50)
    assert data["service"] == "impala"                      # autodetectado pelo tipo
    assert data["queriesScanned"] == 1012                    # >1000 → exigiu 2 páginas
    assert any("impalaQueries" in p and "offset=1000" in p for p in mock_servers.SEEN)
    assert data["cmWarnings"] == ["Impala query scan limit reached."]
    got = ids(data)
    assert not any(i.startswith(("q_ok", "q_set", "q_ddl")) for i in got)  # saudáveis, SET e DDL fora
    assert len(got) == 10
    scores = [r["score"] for r in data["ranking"]]
    assert scores == sorted(scores, reverse=True)
    assert got[0] == "q_spill:1"                             # spill alto + longa + sem estatísticas


def test_each_problem_gets_the_right_code():
    rows = by_id(ok("impala_query_ranking", cluster="Cluster1", top=50))
    assert "slow" in codes(rows["q_slow:1"])
    assert "cliente buscando resultados" in rows["q_slow:1"]["bottleneck"]
    assert codes(rows["q_mem:1"]) >= {"high_memory", "memory_underestimated", "slow"}
    assert codes(rows["q_oom:1"]) == {"oom"}                 # OOM não vira também "falha de execução"
    assert codes(rows["q_spill:1"]) >= {"spill", "stats_missing", "slow"}
    assert rows["q_spill:1"]["spilledGB"] == 3.0
    assert codes(rows["q_analysis:1"]) == {"planning_failure"}
    assert codes(rows["q_rejected:1"]) == {"admission_rejected"}
    assert codes(rows["q_stats:1"]) == {"stats_missing", "slow_planning"}
    assert codes(rows["q_over:1"]) == {"memory_overestimated"}
    assert codes(rows["q_runtime:1"]) == {"execution_failure"}
    assert "bottleneck" not in rows["q_spill:1"]             # nenhuma parcela >= 20%


def test_rank_by_memory_puts_oom_first():
    data = ok("impala_query_ranking", cluster="Cluster1", rank_by="memory", top=3)
    assert ids(data) == ["q_oom:1", "q_mem:1", "q_spill:1"]


def test_rank_by_spill_uses_server_filter():
    data = ok("impala_query_ranking", cluster="Cluster1", rank_by="spill")
    assert ids(data) == ["q_spill:1", "q_spill:2"]
    assert data["queriesScanned"] == 2                       # o CM já filtrou memory_spilled > 0
    assert any("memory_spilled" in p for p in mock_servers.SEEN)


def test_rank_by_planning_failures_first():
    data = ok("impala_query_ranking", cluster="Cluster1", rank_by="planning")
    got = ids(data)
    assert set(got[:2]) == {"q_analysis:1", "q_rejected:1"}   # severidade alta
    assert set(got) == {"q_analysis:1", "q_rejected:1", "q_stats:1", "q_spill:1", "q_over:1"}
    assert got[-1] == "q_over:1"                               # severidade baixa por último


def test_rank_by_duration_and_filters():
    data = ok("impala_query_ranking", cluster="Cluster1", rank_by="duration", top=3)
    assert ids(data) == ["q_slow:1", "q_spill:1", "q_mem:1"]
    etl = ok("impala_query_ranking", cluster="Cluster1", user="etl", top=50)
    assert {r["user"] for r in etl["ranking"]} == {"etl"}
    ddl = ok("impala_query_ranking", cluster="Cluster1", rank_by="duration", include_ddl=True, top=4)
    assert ids(ddl)[3] == "q_ddl:1"                           # DDL de 90s entra em 4º


def test_thresholds_are_configurable():
    data = ok("impala_query_ranking", cluster="Cluster1", slow_seconds=1000, memory_gb_per_node=16, top=50)
    rows = by_id(data)
    assert "slow" not in codes(rows.get("q_slow:1", {}))
    assert "high_memory" not in codes(rows["q_mem:1"])         # 12 GB < 16 GB
    assert data["thresholds"]["memory_gb_per_node"] == 16


def test_ranking_rejects_bad_rank_by():
    _, msg = call("impala_query_ranking", cluster="Cluster1", rank_by="cpu")
    assert "rank_by" in msg


def test_issues_summary_counts_and_patterns():
    data = ok("impala_query_issues_summary", cluster="Cluster1")
    c = data["counts"]
    assert c["oom"] == 1                                       # "risco de OOM" da q_mem não conta como OOM
    assert c["spill"] == 2 and c["longas"] == 3
    assert c["falhaDePlanejamento"] == 1 and c["rejeitadaNaAdmissao"] == 1
    assert c["estatisticasAusentes"] == 2 and c["planejamentoLento"] == 1
    assert c["memoriaExcessiva"] == 1 and c["memoriaSuperestimada"] == 1
    assert c["falhaDeExecucao"] == 1 and c["comAlgumProblema"] == 10
    spill_pattern = next(p for p in data["recurringProblemQueries"] if "vendas.clientes c" in p["pattern"])
    assert spill_pattern["executions"] == 2 and spill_pattern["spillGB"] == 3.5
    assert "where v.dt = ? group by" in spill_pattern["pattern"]   # literais viraram ?
    assert data["topUsers"][0]["user"] == "ana"
    reasons = {r["reason"] for r in data["topFailureReasons"]}
    assert any("AnalysisException" in r for r in reasons)


def test_details_parses_profile_and_recommends():
    data = ok("impala_query_details", cluster="Cluster1", query_id="q_spill:1")
    assert data["summary"]["Tables Missing Stats"] == "vendas.clientes"
    assert data["summary"]["Request Pool"] == "root.bi"
    assert data["columnsMissingStats"] == ["cli", "valor"]
    # soma só das instâncias (o bloco "Averaged Fragment" fica de fora)
    assert data["spillCounters"] == {"ScratchBytesWritten": 3 * 1024 ** 3, "SpilledPartitions": 16}
    assert data["spillByOperatorBytes"] == {"2:HASH_JOIN_NODE": 3 * 1024 ** 3}
    assert any("02:HASH JOIN" in r for r in data["execSummary"])
    assert "errors" not in data                                # "Errors:" vazio não pega a linha seguinte
    # timeline aceita o formato antigo ("- Evento") e o do Impala 4.x (sem hífen), sem os eventos de fragmento
    assert data["timeline"][0] == "Analysis finished: 300.000ms (300000000)"
    assert "Planning finished: 1.200s (1200000000)" in data["timeline"]
    assert "Last row fetched: 5m0s (300000000000)" in data["timeline"]
    assert not any("Prepare Finished" in t for t in data["timeline"])
    assert codes(data["analysis"]) >= {"spill", "stats_missing"}
    recs = " ".join(data["recommendations"])
    assert "COMPUTE STATS" in recs and "vendas.clientes" in recs and "2:HASH_JOIN_NODE" in recs
    assert "plan" not in data
    with_plan = ok("impala_query_details", cluster="Cluster1", query_id="q_spill:1", include_plan=True)
    assert "02:HASH JOIN" in with_plan["plan"]


def test_details_planning_failure_and_missing_profile():
    data = ok("impala_query_details", cluster="Cluster1", query_id="q_analysis:1")
    assert "AnalysisException" in data["summary"]["Query Status"]
    assert codes(data["analysis"]) == {"planning_failure"}
    assert any("antes de executar" in r for r in data["recommendations"])
    empty = ok("impala_query_details", cluster="Cluster1", query_id="q_empty:1")
    assert "expirado" in empty["error"]
    rt = ok("impala_query_ranking", cluster="Cluster1", top=50)
    assert "q_runtime:1" in ids(rt)
    err, msg = call("impala_query_details", cluster="Cluster1", query_id="nao_existe:1")
    assert err and "404" in msg
