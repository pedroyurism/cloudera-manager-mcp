"""
Testes do relatório de saúde (cluster_health_report, list_events only_alerts e cm-health-report).

O cenário do CM simulado (tests/mock_servers.py) tem um problema de cada tipo:
yarn BAD com o ResourceManager ativo parado, DataNode parado, Impala daemon CONCERNING,
host com heartbeat atrasado e clock offset, host sem heartbeat informado, host e serviço em
manutenção, Service Monitor parado, Event Server CONCERNING, 130 alertas e restart pendente.
"""

from __future__ import annotations

import json

import pytest

from tests import mock_servers
from tests.test_tools import call, ok  # importa o ambiente e sobe os servidores simulados

from cm_mcp_server import cli, commands, health  # noqa: E402


def findings(report: dict, level: str | None = None, area: str | None = None) -> list[dict]:
    return [f for f in report["findings"]
            if (level is None or f["level"] == level) and (area is None or f["area"] == area)]


def has(report: dict, level: str, area: str, *needles: str) -> bool:
    return any(all(n in f"{f['entity']} {f['message']}" for n in needles) for f in findings(report, level, area))


@pytest.fixture(scope="module")
def report() -> dict:
    return ok("cluster_health_report", cluster="Cluster1")


def test_overall_status_and_counters(report):
    assert report["overallStatus"] == "CRITICO"
    assert report["counters"]["findings"] == {"critico": 6, "atencao": 8, "info": 2}   # +1 comando travado
    c = report["counters"]
    assert c["services"] == {"total": 4, "GOOD": 2, "BAD": 1, "maintenance": 1}
    assert c["roles"]["stopped"] == 2 and c["roles"]["unhealthy"] == 1
    assert c["hosts"]["staleHeartbeat"] == 1 and c["hosts"]["maintenance"] == 1
    assert c["alerts"] == {"CRITICAL": 125, "IMPORTANT": 5}
    levels = [f["level"] for f in report["findings"]]
    assert levels == sorted(levels, key=health.LEVELS.index)          # críticos primeiro


def test_services_and_roles(report):
    assert has(report, "critico", "servicos", "Cluster1/yarn", "YARN_RESOURCEMANAGERS_HEALTH")
    assert has(report, "critico", "roles", "hdfs-DN-2", "role parada", "DATA_NODE_HOST_HEALTH")
    assert has(report, "critico", "roles", "yarn-RM-1", "role parada")
    assert has(report, "atencao", "roles", "impala-IMPALAD-1", "CONCERNING")
    assert not any("GW" in f["entity"] for f in report["findings"])   # gateway (NA) ignorado
    # manutenção vira só informativo
    assert has(report, "info", "servicos", "oozie", "manutenção")
    assert has(report, "info", "roles", "oozie-SERVER-1", "manutenção")


def test_high_availability(report):
    ha = {(h["service"], h["roleType"]): h for h in report["clusters"][0]["highAvailability"]}
    assert ha[("hdfs", "NAMENODE")]["active"] == ["h1.local"] and ha[("hdfs", "NAMENODE")]["standby"] == ["h2.local"]
    assert not any("hdfs" in f["entity"] for f in findings(report, area="ha"))   # par ACTIVE/STANDBY ok
    assert ha[("yarn", "RESOURCEMANAGER")]["active"] == []
    assert has(report, "critico", "ha", "yarn RESOURCEMANAGER", "nenhuma instância ACTIVE")


def test_hosts(report):
    assert has(report, "critico", "hosts", "node02.local", "sem heartbeat há")
    assert has(report, "atencao", "hosts", "node02.local", "HOST_CLOCK_OFFSET=CONCERNING")
    assert has(report, "atencao", "hosts", "node04.local", "não informa o último heartbeat")
    assert not any("node03" in f["entity"] for f in report["findings"])   # em manutenção
    key = report["clusters"][0]["hostKeyChecks"]
    assert key["clock"] == {"GOOD": 2, "CONCERNING": 1}
    assert key["swap"] == {"GOOD": 3}                                    # o BAD do host em manutenção fica fora


def test_management_service_blind(report):
    assert has(report, "critico", "management", "SERVICEMONITOR", "Monitoramento cego")
    assert has(report, "atencao", "management", "EVENTSERVER", "EVENT_SERVER_EVENT_STORE_SIZE")
    types = {r["type"]: r for r in report["managementService"]["roles"]}
    assert types["SERVICEMONITOR"]["state"] == "STOPPED"


def test_alerts_grouped_and_paginated(report):
    a = report["alerts"]
    assert a["total"] == 130 and a["analyzed"] == 130                   # 2 páginas de 100
    assert any("resultOffset=100" in p for p in mock_servers.SEEN)
    assert a["byService"][0] == {"service": "hdfs", "CRITICAL": 120, "total": 120}
    assert {r["service"] for r in a["byService"]} == {"hdfs", "yarn", "zookeeper"}
    assert len(a["latest"]) == 10 and a["latest"][0]["summary"].startswith("The health of service")
    times = [e["time"] for e in a["latest"]]
    assert times == sorted(times, reverse=True)
    assert has(report, "atencao", "alertas", "125 alerta(s) CRITICAL")


def test_pending_restart(report):
    rows = {r["service"]: r for r in report["clusters"][0]["pendingRestart"]["services"]}
    assert rows["hdfs"]["config"] == "STALE" and rows["hdfs"]["rolesNeedRestart"] == 1
    assert rows["impala"]["clientConfig"] == "STALE" and rows["impala"]["rolesNeedRefresh"] == 1
    assert has(report, "atencao", "restart", "Cluster1/hdfs", "restart (1 role(s))")
    assert has(report, "atencao", "restart", "Cluster1/impala", "refresh", "deploy client configuration")


def test_text_output_and_bad_argument():
    text = ok("cluster_health_report", cluster="Cluster1", output="text")
    assert "STATUS GERAL: CRÍTICO" in text
    assert "HA yarn RESOURCEMANAGER: ativo -; standby h2" in text
    assert "Alertas nas últimas 24h: 130 (CRITICAL 125, IMPORTANT 5)" in text
    _, msg = call("cluster_health_report", output="xml")
    assert "output" in msg


def test_list_events_only_alerts():
    data = ok("list_events", only_alerts=True, severity=None, max_results=5)
    assert len(data) == 5 and all(e["alert"] for e in data)
    assert data[0]["summary"].startswith("The health of service")


# --- status OK e falhas de acesso, com um CM falso em memória ----------------------------

class FakeCM:
    version = "v58"

    def __init__(self, fail: set[str] | None = None, alerts: list | None = None):
        self.fail = fail or set()
        self.alerts = alerts or []

    def get(self, path, params=None, raw=False):
        if path in self.fail:
            raise RuntimeError(f"CM API 403 em GET {path}: User not allowed to perform operation.")
        good = {"healthSummary": "GOOD", "healthChecks": [], "configStalenessStatus": "FRESH"}
        data = {
            "cm/version": {"version": "7.13.2"},
            "clusters": {"items": [{"name": "C"}]},
            "clusters/C/services": {"items": [{"name": "zk", "serviceState": "STARTED", **good}]},
            "clusters/C/services/zk/roles": {"items": [
                {"name": "zk-1", "type": "SERVER", "roleState": "STARTED", "hostRef": {"hostname": "a"}, **good}]},
            "clusters/C/hosts": {"items": [{"hostname": "a", "lastHeartbeat": mock_servers._hb(5), **good}]},
            "cm/service": {"serviceState": "STARTED", "healthSummary": "GOOD"},
            "cm/service/roles": {"items": [{"type": t, "roleState": "STARTED", **good}
                                           for t in ("SERVICEMONITOR", "HOSTMONITOR", "EVENTSERVER")]},
            "events": {"totalResults": len(self.alerts), "items": self.alerts},
            "cm/commands": {"items": []},
            "cm/service/commands": {"items": []},
            "clusters/C/commands": {"items": []},
            "clusters/C/services/zk/commands": {"items": []},
        }
        return data[path]


def _fake(monkeypatch, fake: FakeCM) -> None:
    """O relatório usa o cliente do CM em dois módulos (health e commands)."""
    monkeypatch.setattr(health, "cm", lambda: fake)
    monkeypatch.setattr(commands, "cm", lambda: fake)


def test_ok_when_everything_is_healthy(monkeypatch):
    _fake(monkeypatch, FakeCM())
    r = health.build_health_report()
    assert r["overallStatus"] == "OK" and r["findings"] == []
    assert "STATUS GERAL: OK" in health.render_text(r)


def test_access_errors_do_not_break_the_report(monkeypatch):
    _fake(monkeypatch, FakeCM(fail={"cm/service/roles", "events"}))
    r = health.build_health_report()
    assert r["overallStatus"] == "ATENCAO"                     # só os avisos de acesso
    assert r["managementService"] is None and r["alerts"] is None
    assert has(r, "atencao", "acesso", "Management Service", "403")
    assert has(r, "atencao", "acesso", "alertas", "403")


def test_command_list_errors_become_access_findings(monkeypatch):
    _fake(monkeypatch, FakeCM(fail={"cm/commands"}))
    r = health.build_health_report()
    assert r["overallStatus"] == "ATENCAO"
    assert has(r, "atencao", "acesso", "comandos", "cm/commands", "403")
    assert r["commands"]["running"] == 0 and r["commands"]["errors"]
    assert r["clusters"][0]["services"]["GOOD"] == 1           # o resto do relatório saiu


def test_cli_json_and_exit_codes(capsys, monkeypatch):
    assert cli.main(["--cluster", "Cluster1", "--json"]) == 2
    out = json.loads(capsys.readouterr().out)
    assert out["overallStatus"] == "CRITICO"
    _fake(monkeypatch, FakeCM())
    assert cli.main([]) == 0
    assert "STATUS GERAL: OK" in capsys.readouterr().out
    monkeypatch.setattr(health, "build_health_report", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    assert cli.main([]) == 3
