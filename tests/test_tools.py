"""
Testes das tools contra um CM e um RM simulados (tests/mock_servers.py).

Rodar:  pip install -e '.[dev]'  &&  pytest -q
"""

from __future__ import annotations

import asyncio
import json
import os

from tests import mock_servers

# A configuração é lida na importação do pacote, então o ambiente vem antes.
os.environ.update(
    CM_HOST="127.0.0.1", CM_PORT=str(mock_servers.CM_PORT), CM_USE_TLS="false",
    CM_USERNAME="u", CM_PASSWORD="p", YARN_RM_TLS="false",
    NO_PROXY="127.0.0.1,localhost", no_proxy="127.0.0.1,localhost",
)
mock_servers.start()

from fastmcp import Client  # noqa: E402

from cm_mcp_server.server import mcp  # noqa: E402


def call(name: str, **args):
    async def run():
        async with Client(mcp) as c:
            r = await c.call_tool(name, args, raise_on_error=False)
            return r
    r = asyncio.run(run())
    text = r.content[0].text if r.content else ""
    try:
        return r.is_error, json.loads(text)
    except json.JSONDecodeError:
        return r.is_error, text


def ok(name: str, **args):
    err, data = call(name, **args)
    assert not err, f"{name} falhou: {data}"
    return data


def test_tool_registry():
    async def run():
        async with Client(mcp) as c:
            return {t.name for t in await c.list_tools()}
    names = asyncio.run(run())
    assert len(names) == 31
    assert {"yarn_list_applications", "yarn_top_consumers", "yarn_queue_usage",
            "yarn_scheduler_queues", "yarn_cluster_metrics", "yarn_utilization_report",
            "yarn_get_application", "yarn_application_attributes"} <= names


def test_cm_tools_still_work():
    assert ok("cm_info")["apiVersion"] == "v51"
    assert ok("list_clusters")[0]["name"] == "Cluster1"
    summary = ok("cluster_health_summary", cluster="Cluster1")
    assert summary[0]["service"] == "yarn"
    assert ok("get_role_log", cluster="Cluster1", service="yarn", role="yarn-RM-1", tail_lines=2) == "linha 498\nlinha 499"
    assert ok("query_timeseries", tsquery="select cpu_percent where category = HOST")[0]["max"] == 42
    err, _ = call("cm_api_get", path="naoexiste")
    assert err


def test_list_running_applications():
    data = ok("yarn_list_applications", cluster="Cluster1")
    assert data["service"] == "yarn"                    # autodetectado
    assert data["totalMatched"] == 4                    # 3 etl_user + 1 bob
    first = data["applications"][0]
    assert first["currentMemoryGB"] == 8.0 and first["currentVcores"] == 4
    assert first["type"] == "SPARK"


def test_list_finished_paginates_and_sorts():
    data = ok("yarn_list_applications", cluster="Cluster1", state="finished", sort_by="memory", limit=5)
    assert data["totalMatched"] == 126                  # >100 → exigiu 2 páginas
    assert any("offset=100" in p for p in mock_servers.SEEN)
    mems = [a["memoryGBHours"] for a in data["applications"]]
    assert mems == sorted(mems, reverse=True) and mems[0] == 50.0


def test_list_filters_user():
    data = ok("yarn_list_applications", cluster="Cluster1", state="all", user="ana")
    assert data["totalMatched"] == 6
    assert {a["user"] for a in data["applications"]} == {"ana"}
    for a in data["applications"]:  # terminadas: -1 do CM não deve aparecer
        assert not {"currentMemoryGB", "currentVcores", "runningContainers"} & a.keys()


def test_top_users():
    data = ok("yarn_top_consumers", cluster="Cluster1", hours=24)
    top = data["ranking"][0]
    assert top["user"] == "etl_user"
    assert top["apps"] == 123 and top["running"] == 3
    assert top["memoryGBHours"] == 6300.0               # 3*100 + 120*50
    assert top["currentMemoryGB"] == 24.0
    assert top["topQueues"] == ["root.etl"]
    ana = next(r for r in data["ranking"] if r["user"] == "ana")
    assert ana["failedOrKilled"] == 1
    assert abs(sum(r["memorySharePct"] for r in data["ranking"]) - 100) < 0.5


def test_top_by_queue_and_current_memory():
    data = ok("yarn_top_consumers", cluster="Cluster1", group_by="queue", rank_by="current_memory")
    assert data["ranking"][0]["queue"] == "root.etl"
    assert data["ranking"][0]["topUsers"] == ["etl_user"]


def test_top_rejects_bad_args():
    _, msg = call("yarn_top_consumers", cluster="Cluster1", group_by="host")
    assert "group_by" in msg


def test_queue_usage_from_cm_without_fair_share():
    data = ok("yarn_queue_usage", cluster="Cluster1")
    etl = next(q for q in data["queues"] if q["queue"] == "root.etl")
    assert etl["allocatedMemoryGB"] == 24.0 and etl["peakAllocatedMemoryGB"] == 40.0
    assert etl["pendingContainers"] == 3 and etl["peakPendingContainers"] == 12
    assert "fairShareMemoryGB" not in etl              # Capacity Scheduler: fallback silencioso
    assert data["queues"][0]["queue"] == "root.etl"     # ordenado por memória


def test_scheduler_queues_discovers_active_rm():
    data = ok("yarn_scheduler_queues", cluster="Cluster1")
    assert data["schedulerType"] == "capacityScheduler"
    etl = next(q for q in data["queues"] if q["queue"] == "root.etl")
    assert etl["usedCapacityPct"] == 80.0 and etl["usedMemoryGB"] == 24.0
    assert etl["users"][0] == {"user": "etl_user", "memoryGB": 24.0, "vcores": 12,
                               "activeApps": 3, "pendingApps": 0}
    leaf = ok("yarn_scheduler_queues", cluster="Cluster1", only_leaf=True)
    assert all(q["leaf"] for q in leaf["queues"]) and len(leaf["queues"]) == 2


def test_cluster_metrics():
    data = ok("yarn_cluster_metrics", cluster="Cluster1")
    assert data["memory"]["usedPct"] == 50.0 and data["vcores"]["usedPct"] == 25.0
    assert data["nodes"]["unhealthyNodes"] == 1


def test_get_application_diagnostics():
    data = ok("yarn_get_application", cluster="Cluster1", application_id="application_1700000000000_0400")
    assert data["finalStatus"] == "FAILED" and "OOM" in data["diagnostics"]
    assert not {"allocatedMB", "allocatedVCores", "runningContainers"} & data.keys()


def test_unreachable_rm_explains_yarn_rm_url():
    import pytest

    from cm_mcp_server.core import RMClient

    with pytest.raises(RuntimeError, match="YARN_RM_URL") as e:
        RMClient("http://rm-inexistente.invalid:8088").get("cluster/metrics")
    assert "rm-inexistente.invalid" in str(e.value)


def test_utilization_report():
    data = ok("yarn_utilization_report", cluster="Cluster1", days=7, days_of_week="1,2,3,4,5",
              start_hour=8, end_hour=18)
    assert data["tenants"][0]["tenantName"] == "root.etl"
    assert data["tenants"][0]["avgYarnMemoryAllocation"] == 90000.12
    assert any("daysOfWeek=1&daysOfWeek=2" in p for p in mock_servers.SEEN)


def test_application_attributes():
    data = ok("yarn_application_attributes", cluster="Cluster1")
    assert {a["name"] for a in data} == {"user", "application_duration"}
