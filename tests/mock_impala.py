"""Queries e profiles Impala simulados, no formato da API do CM (atributos chegam como texto)."""

from __future__ import annotations

import re

GB = 1024 ** 3


def _q(qid: str, *, user: str = "ana", pool: str = "root.bi", db: str = "vendas", qtype: str = "QUERY",
       state: str = "FINISHED", ms: int = 1000, sql: str = "select 1", **attrs) -> dict:
    base = {"pool": pool, "query_status": "OK" if state == "FINISHED" else "", "oom": "false",
            "stats_missing": "false", "stats_corrupt": "false", "memory_spilled": "0",
            "admission_result": "Admitted immediately", "admission_wait": "5",
            "planning_wait_time": "200", "planning_wait_time_percentage": "5"}
    base.update({k: str(v) for k, v in attrs.items()})
    return {"queryId": qid, "user": user, "database": db, "queryType": qtype, "queryState": state,
            "durationMillis": ms, "statement": sql, "startTime": "2026-11-05T10:00:00.000Z",
            "endTime": "2026-11-05T10:15:00.000Z", "rowsProduced": 10, "detailsAvailable": True,
            "coordinator": {"hostId": "h1"}, "attributes": base}


SPILL_SQL = "SELECT c.id, sum(v.valor) FROM vendas.itens v JOIN vendas.clientes c ON v.cli = c.id " \
            "WHERE v.dt = '{}' GROUP BY c.id"

QUERIES = [
    # 1. longa: 15 min, quase tudo com o cliente buscando resultados
    _q("q_slow:1", ms=900_000, sql="select * from vendas.itens", client_fetch_wait_time_percentage=80,
       thread_cpu_time_percentage=10, memory_per_node_peak=1 * GB, estimated_per_node_peak_memory=1 * GB),
    # 2. memória: 12 GB por nó, planner estimou 2 GB
    _q("q_mem:1", user="etl", pool="root.etl", ms=120_000, sql="insert into x select * from y",
       qtype="DML", memory_per_node_peak=12 * GB, memory_per_node_peak_node="worker2:27000",
       memory_aggregate_peak=30 * GB, estimated_per_node_peak_memory=2 * GB, thread_cpu_time_percentage=70),
    # 3. OOM
    _q("q_oom:1", sql="select * from vendas.itens a join vendas.itens b on a.id = b.id", user="etl", pool="root.etl", state="EXCEPTION", ms=30_000, oom="true",
       query_status="Memory limit exceeded: Failed to allocate row batch. HASH_JOIN_NODE (id=2) could "
                    "not allocate 8.00 MB without exceeding limit.",
       memory_per_node_peak=7.9 * GB, estimated_per_node_peak_memory=1 * GB),
    # 4 e 5. spill, mesma query com literais diferentes (padrão recorrente)
    _q("q_spill:1", user="bob", ms=300_000, sql=SPILL_SQL.format("2026-11-01"), memory_spilled=3 * GB,
       memory_per_node_peak=3.5 * GB, estimated_per_node_peak_memory=3 * GB, stats_missing="true"),
    _q("q_spill:2", user="bob", ms=40_000, sql=SPILL_SQL.format("2026-11-02"), memory_spilled=0.5 * GB,
       memory_per_node_peak=3 * GB, estimated_per_node_peak_memory=3 * GB),
    # 6. falha de análise (não chegou a executar)
    _q("q_analysis:1", state="EXCEPTION", ms=150, sql="select * from vendas.nao_existe",
       query_status="AnalysisException: Could not resolve table reference: 'vendas.nao_existe'",
       admission_result="Unknown"),
    # 7. rejeitada na admissão
    _q("q_rejected:1", sql="select * from vendas.historico order by dt", user="etl", pool="root.etl", state="EXCEPTION", ms=50, admission_result="Rejected",
       query_status="Rejected query from pool root.etl: request memory needed 20.00 GB per node is "
                    "greater than memory available for admission 8.00 GB of worker1:27000."),
    # 8. sem estatísticas e planejamento lento (15 s)
    _q("q_stats:1", sql="select count(*) from vendas.particionada where ano = 2026", ms=20_000, stats_missing="true", planning_wait_time=15_000,
       planning_wait_time_percentage=75, memory_per_node_peak=0.2 * GB, estimated_per_node_peak_memory=0.3 * GB),
    # 9. superestimativa: planner pediu 20 GB e usou 0,5 GB
    _q("q_over:1", sql="select max(id) from vendas.clientes", ms=5_000, memory_per_node_peak=0.5 * GB, estimated_per_node_peak_memory=20 * GB),
    # 10. falha de execução (impalad inacessível)
    _q("q_runtime:1", sql="select * from vendas.clientes limit 10", state="EXCEPTION", ms=12_000,
       query_status="Failed due to unreachable impalad(s): worker1:27000"),
    # ruído que deve ser ignorado
    _q("q_set:1", qtype="SET", sql="set mem_limit=1g"),
    _q("q_ddl:1", qtype="DDL", ms=90_000, sql="create table t (a int)"),
]
# 1000 queries rápidas e saudáveis: força a paginação (página de 1000)
QUERIES += [_q(f"q_ok:{i}", user="carla", ms=800, memory_per_node_peak=50 * 1024 ** 2,
               estimated_per_node_peak_memory=64 * 1024 ** 2) for i in range(1000)]


def filter_queries(flt: str) -> list[dict]:
    out = QUERIES
    for cond in [c.strip(" ()") for c in re.split(r"\s+and\s+", flt or "") if c.strip()]:
        m = re.match(r"(\w+)\s*(=|>)\s*(.+)", cond)
        if not m:
            continue
        key, op, val = m.groups()
        val = val.strip().strip('"')
        if key == "memory_spilled" and op == ">":
            out = [q for q in out if float(q["attributes"]["memory_spilled"]) > float(val)]
        elif key == "query_id":
            out = [q for q in out if q["queryId"] == val]
        elif key == "user":
            out = [q for q in out if q["user"] == val]
        elif key == "pool":
            out = [q for q in out if q["attributes"]["pool"] == val]
        elif key == "database":
            out = [q for q in out if q["database"] == val]
    return out


SPILL_PROFILE = """Query (id=q_spill:1)
  Summary
    Session ID: abc
    Duration: 5m0s (300000000000 us)
    Query Type: QUERY
    Query State: FINISHED
    Query Status: OK
    User: bob
    Default Db: vendas
    Sql Statement: SELECT c.id, sum(v.valor) FROM vendas.itens v JOIN vendas.clientes c ON v.cli = c.id
    Coordinator: worker1:27000
    Tables Queried: vendas.itens,vendas.clientes
    Plan:
----------------
Max Per-Host Resource Reservation: Memory=40.00MB Threads=6
Per-Host Resource Estimates: Memory=3.00GB
WARNING: The following tables are missing relevant table and/or column statistics.
vendas.clientes
F00:PLAN FRAGMENT [RANDOM] hosts=2 instances=2
02:HASH JOIN [INNER JOIN, BROADCAST]
|  mem-estimate=2.00GB mem-reservation=34.00MB spill-buffer=2.00MB
00:SCAN HDFS [vendas.itens v, RANDOM]
   stored statistics:
     columns missing stats: cli, valor
----------------
    Estimated Per-Host Mem: 3221225472
    Tables Missing Stats: vendas.clientes
    Request Pool: root.bi
    Admission result: Admitted immediately
    ExecSummary:
Operator          #Hosts  #Inst  Avg Time  Max Time  #Rows  Est. #Rows  Peak Mem  Est. Peak Mem  Detail
----------------------------------------------------------------------------------------------------------
F00:ROOT               1      1   0.000ns   0.000ns                       4.00 MB        4.00 MB
02:HASH JOIN           2      2    4m10s     4m50s   10M         1K       3.50 GB        2.00 GB  INNER JOIN
|--01:SCAN HDFS        2      2   1.000s    1.200s    5M         1K      64.00 MB      128.00 MB  vendas.clientes c
00:SCAN HDFS           2      2  20.000s   25.000s   80M        80M     128.00 MB      128.00 MB  vendas.itens v
    Errors:
    Query Compilation
      Planner Timeline: 1.100s
         - Analysis finished: 300.000ms (300000000)
         - Single node plan created: 900.000ms (900000000)
    Query Timeline
      Query submitted: 30.000us (30000)
      Planning finished: 1.200s (1200000000)
      Submit for admission: 1.300s (1300000000)
      Completed admission: 1.301s (1301000000)
      Last row fetched: 5m0s (300000000000)
        Fragment Instance Lifecycle Event Timeline
          Prepare Finished: 0ns (0)
  Execution Profile q_spill:1
    Per Node Peak Memory Usage: worker1:27000(3.50 GB) worker2:27000(3.40 GB)
    Averaged Fragment F00
      HASH_JOIN_NODE (id=2)
         - ScratchBytesWritten: 1.50 GB (1610612736)
         - SpilledPartitions: 8 (8)
    Fragment F00
      Instance q_spill:1_1 (host=worker1:27000)
        HASH_JOIN_NODE (id=2)
           - ScratchBytesWritten: 2.00 GB (2147483648)
           - SpilledPartitions: 10 (10)
        HDFS_SCAN_NODE (id=0)
           - ScratchBytesWritten: 0 (0)
      Instance q_spill:1_2 (host=worker2:27000)
        HASH_JOIN_NODE (id=2)
           - ScratchBytesWritten: 1.00 GB (1073741824)
           - SpilledPartitions: 6 (6)
"""

ANALYSIS_PROFILE = """Query (id=q_analysis:1)
  Summary
    Session ID: def
    Duration: 150ms (150000000 us)
    Query Type: QUERY
    Query State: EXCEPTION
    Query Status: AnalysisException: Could not resolve table reference: 'vendas.nao_existe'
    User: ana
    Default Db: vendas
    Sql Statement: select * from vendas.nao_existe
"""

PROFILES = {"q_spill:1": SPILL_PROFILE, "q_analysis:1": ANALYSIS_PROFILE, "q_empty:1": ""}
