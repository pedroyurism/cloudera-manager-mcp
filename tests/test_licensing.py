"""
Testes do levantamento de licenciamento (license_report e cm-license-report).

Cenário com um CM falso em memória: cluster P com 2 workers (DataNode), 1 master com /var
cheio e discos de dados vazios, 1 edge ocioso com hyperthreading e 1 host fora do cluster.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.test_tools import call, ok  # importa o ambiente e sobe os servidores simulados

from cm_mcp_server import cli, licensing  # noqa: E402

GIB, TIB = 1024**3, 1024**4
MEM_256, MEM_128, MEM_64 = 269913153536, 134630666240, 66992615424


def _host(hid, cores, mem, physical=None, cluster="P"):
    return {"hostId": hid, "hostname": f"{hid}.corp", "numCores": cores, "numPhysicalCores": physical or cores,
            "totalPhysMemBytes": mem, "clusterRef": {"clusterName": cluster},
            "distribution": {"name": "RHEL", "version": "8.10"}}


HOSTS = [_host("w1", 64, MEM_256), _host("w2", 64, MEM_256), _host("m1", 32, MEM_128),
         _host("e1", 32, MEM_128, physical=16), _host("x1", 4, MEM_64, cluster="Outro")]

# (host, mount, capacidade, % usado)
FS = [("w1", "/", 100 * GIB, 3), ("w1", "/data1", TIB, 80), ("w1", "/data2", TIB, 60),
      ("w2", "/", 100 * GIB, 3), ("w2", "/data1", TIB, 78), ("w2", "/data2", TIB, 5),
      ("m1", "/", 100 * GIB, 2), ("m1", "/var", 300 * GIB, 85), ("m1", "/data1", TIB, 0), ("m1", "/data2", TIB, 1),
      ("e1", "/", 100 * GIB, 2)]
# (cpu pico %, memória usada mín/máx em %)
USAGE = {"w1": (3.0, (59, 62)), "w2": (2.5, (58, 61)), "m1": (7.9, (45, 46)), "e1": (2.0, (8, 10))}


def _series(metric, values, **attrs):
    return {"metadata": {"metricName": metric, "entityName": attrs.get("hostId", "svc"), "attributes": attrs},
            "data": [{"timestamp": "t", "value": v} for v in values]}


class FakeCM:
    version = "v58"

    def __init__(self, fail: set[str] | None = None):
        self.fail = fail or set()
        self.queries: list[str] = []

    def _ts(self, q: str) -> dict:
        self.queries.append(q)
        ts = []
        if "category = HOST" in q:
            for hid, (cpu, (lo, hi)) in USAGE.items():
                mem = next(h["totalPhysMemBytes"] for h in HOSTS if h["hostId"] == hid)
                ts += [_series("cpu_percent", [1.0, cpu, 1.5], hostId=hid),
                       _series("physical_memory_used", [mem * lo / 100, mem * hi / 100], hostId=hid),
                       _series("physical_memory_total", [mem], hostId=hid),
                       _series("swap_used", [0, 0], hostId=hid)]
        elif "FILESYSTEM" in q:
            for hid, mount, cap, pct in FS:   # sem hostId: resolve pelo hostname
                ts += [_series("capacity", [cap], hostname=f"{hid}.corp", mountpoint=mount),
                       _series("capacity_used", [cap * pct / 100], hostname=f"{hid}.corp", mountpoint=mount)]
        elif "serviceType = HDFS" in q:   # HA: o serviço vem como hdfs:<nameservice>
            ns = {"serviceName": "hdfs:ns1"}
            ts = [_series("dfs_capacity", [4 * TIB], **ns), _series("dfs_capacity_used", [3 * TIB], **ns),
                  _series("dfs_capacity_used_non_hdfs", [0.1 * TIB], **ns), _series("files_total", [12345], **ns),
                  _series("dfs_capacity", [99 * TIB], serviceName="hdfs2")]   # outro HDFS: ignorado
        elif "serviceType = KUDU" in q:
            ts = [_series("total_kudu_on_disk_size_across_kudu_replicas", [3 * GIB], serviceName="kudu")]
        return {"items": [{"timeSeries": ts}]}

    def get(self, path, params=None, raw=False):
        if path in self.fail:
            raise RuntimeError(f"CM API 403 em GET {path}: User not allowed to perform operation.")
        if path == "timeseries":
            return self._ts(params["query"])
        role = lambda n, t, h: {"name": n, "type": t, "hostRef": {"hostId": h}}  # noqa: E731
        data = {
            "clusters/P": {"name": "P", "displayName": "Produção", "fullVersion": "7.1.9-1.cdh7.1.9.p0"},
            "cm/version": {"version": "7.13.1"},
            "clusters/P/services": {"items": [{"name": "hdfs", "type": "HDFS"}, {"name": "impala", "type": "IMPALA"},
                                              {"name": "kudu", "type": "KUDU"}, {"name": "ozone", "type": "OZONE"}]},
            "clusters/P/hosts": {"items": [{"hostId": h} for h in ("w1", "w2", "m1", "e1")]},
            "hosts": {"items": HOSTS},
            "clusters/P/services/hdfs/roles": {"items": [
                role("dn1", "DATANODE", "w1"), role("dn2", "DATANODE", "w2"), role("nn", "NAMENODE", "m1"),
                role("gw", "GATEWAY", "e1")]},
            "clusters/P/services/impala/roles": {"items": [role("i1", "IMPALAD", "w1"), role("i2", "IMPALAD", "w2")]},
            "clusters/P/services/kudu/roles": {"items": [role("km", "KUDU_MASTER", "m1")]},
            "clusters/P/services/ozone/roles": {"items": []},
            "clusters/P/services/hdfs/config": {"items": [{"name": "dfs_replication", "default": "3"}]},
        }
        return data[path]


@pytest.fixture
def fake(monkeypatch) -> FakeCM:
    f = FakeCM()
    monkeypatch.setattr(licensing, "cm", lambda: f)
    return f


@pytest.fixture
def report(fake) -> dict:
    return licensing.build_license_report("P")


def test_ccu_both_bases_and_rounding(report):
    hosts = {h["short"]: h for h in report["hosts"]}
    assert set(hosts) == {"w1", "w2", "m1", "e1"}
    assert hosts["w1"]["memoryNominalGB"] == 256 and hosts["m1"]["memoryNominalGB"] == 128
    assert hosts["w1"]["ccu"]["os"]["total"] == pytest.approx(64 / 6 + MEM_256 / GIB / 12, abs=1e-3)
    assert hosts["w1"]["ccu"]["nominal"]["total"] == pytest.approx(32)
    # hyperthreading: vale o maior valor entre lógicos e físicos
    assert hosts["e1"]["cores"] == 32 and hosts["e1"]["hyperthreading"]
    assert report["ccu"]["os"]["total"] == pytest.approx(94.79, abs=0.01)
    assert report["ccu"]["nominal"]["total"] == pytest.approx(96)
    assert report["ccu"]["os"]["roundedPerNode"] == 96
    assert report["totals"]["cores"] == 192 and report["totals"]["physicalCores"] == 176
    assert report["hosts"][0]["short"] in ("w1", "w2")            # maior CCU primeiro


def test_data_and_capacity_under_management(report):
    dum, cum = report["dataUnderManagement"], report["capacityUnderManagement"]
    assert dum["logicalTB"] == pytest.approx(1 + 1 / 1024, abs=1e-4)   # 3 TB ÷ 3 + 3 GB ÷ 3
    assert dum["rawTB"] == pytest.approx(3 + 3 / 1024, abs=1e-4)
    assert dum["hdfs"]["replication"] == 3 and dum["hdfs"]["files"] == 12345
    assert dum["notCounted"] == ["OZONE"]
    assert cum["hdfsTB"] == 4 and cum["hdfsUsedPct"] == 75
    assert cum["allDataVolumesTB"] == 6 and cum["dataNodeVolumesTB"] == 4


def test_usage_volumes_and_roles(report):
    hosts = {h["short"]: h for h in report["hosts"]}
    assert hosts["m1"]["cpuPeakPct"] == 7.9 and hosts["m1"]["memoryUsedPct"] == [45, 46]
    assert [m["mount"] for m in hosts["w1"]["dataVolumes"]] == ["/data1", "/data2"]
    assert [m["mount"] for m in hosts["m1"]["systemMounts"]] == ["/", "/var"]
    assert hosts["w1"]["roles"] == "DataNode, Impala Daemon"
    assert hosts["e1"]["roles"] == "gateways (clientes)"
    assert report["totals"]["cpuPeakPct"] == 7.9


def test_findings_and_questions(report):
    titles = [f["title"] for f in report["findings"]]
    assert "HDFS a 75%" in titles
    assert "/var do m1 em 85%" in titles
    assert "/data2 desbalanceado" in titles and "/data1 desbalanceado" not in titles
    assert any(t.startswith("2,0 TB de discos de dados quase vazios") for t in titles)
    assert "Hosts ociosos: e1" in titles
    assert "Hyperthreading ligado" in titles
    assert {licensing._host_status(h)[1] for h in report["hosts"]} == {"disco", "/var 85%", "ocioso"}
    topics = {q["topic"]: q["text"] for q in report["questions"]}
    assert "94,79" in topics["Base de memória"] and "96,00" in topics["Base de memória"]
    assert "outros 1 hosts" in topics["Quais Nodes contam"] and "x1" in topics["Quais Nodes contam"]
    assert "Ozone" in topics["Dados brutos ou lógicos"]
    assert report["otherHosts"][0]["cluster"] == "Outro"


def test_partial_failure_becomes_warning(monkeypatch):
    f = FakeCM(fail={"clusters/P/services/hdfs/config", "clusters/P/services/kudu/roles"})
    monkeypatch.setattr(licensing, "cm", lambda: f)
    r = licensing.build_license_report("P")
    assert any("HDFS" in w for w in r["warnings"]) and any("kudu" in w for w in r["warnings"])
    assert r["capacityUnderManagement"]["hdfsTB"] == 0 and r["ccu"]["os"]["total"] > 90


def test_html_page(report, tmp_path):
    paths = licensing.write_report(report, "html", str(tmp_path))
    page = Path(paths["html"]).read_text(encoding="utf-8")
    assert Path(paths["html"]).name.startswith("licenciamento-p-")
    assert "<title>Produção Capacidade e Licenças</title>" in page
    assert "CCU = cores ÷ 6 + memória em GB ÷ 12" in page
    assert page.count('data-base="nominal" hidden') >= 3          # tile, nota e gráfico/tabela
    assert "94,79" in page and "96,00" in page and "RHEL 8.10" in page
    assert "/var do m1 em 85%" in page and "@media print" in page


def test_pdf_without_browser_explains(report, tmp_path, monkeypatch):
    monkeypatch.setattr(licensing, "find_browser", lambda: None)
    with pytest.raises(RuntimeError, match="CM_PDF_BROWSER"):
        licensing.write_report(report, "pdf", str(tmp_path))
    assert list(tmp_path.glob("*.html"))                           # o HTML fica gravado


def test_tool_outputs(fake, tmp_path):
    full = ok("license_report", cluster="P")
    assert full["ccu"]["nominal"]["total"] == 96
    res = ok("license_report", cluster="P", output="html", output_dir=str(tmp_path))
    assert Path(res["files"]["html"]).exists() and res["summary"]["ccu"]["roundedPerNodeOs"] == 96
    assert "output deve ser" in ok("license_report", cluster="P", output="xlsx")


def test_cli(fake, tmp_path, capsys):
    assert cli.license_main(["--cluster", "P", "--out", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "94.79 CCU" in out and "HTML:" in out
    assert cli.license_main(["--cluster", "P", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["totals"]["nodes"] == 4
