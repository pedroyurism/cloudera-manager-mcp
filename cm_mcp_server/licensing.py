"""
Levantamento de capacidade e licenciamento Cloudera (somente leitura).

Calcula, pela definição da página "Cloudera Licensed Metrics"
(https://www.cloudera.com/legal/commercial-terms-and-conditions/cloudera-licensed-metrics.html):
  - CCU por Node = cores ÷ 6 + memória em GB ÷ 12, sobre o recurso disponível no host
    (não o consumo), em duas bases de memória: a RAM vista pelo SO e a RAM nominal
    (a vista pelo SO arredondada para cima no múltiplo de `nominal_memory_step_gb`);
  - Core = o maior valor entre cores lógicos (com hyperthreading) e físicos;
  - Data Under Management = dados lógicos (HDFS usado ÷ replicação + Kudu ÷ 3) e brutos;
  - Capacity Under Management = capacidade do HDFS e soma de todos os volumes de dados.
1 GB = 1.024 MB e 1 TB = 1.024 GB.

Junta também o uso dos últimos `days` dias (pico de CPU, faixa de memória, swap), os
filesystems de cada host e gera achados e pontos a validar no Order Form. A saída pode ser
JSON, uma página HTML ou um PDF (impresso pelo Edge/Chrome em modo headless).
"""

from __future__ import annotations

import html
import math
import os
import re
import shutil
import subprocess
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from cm_mcp_server.core import _items, _iso, _out, cm, mcp

GIB = 1024**3
TIB = 1024**4
DEFAULT_DATA_MOUNTS = r"^/(data|dfs|grid|disk|hadoop)[\w.-]*$"
METRICS_URL = "https://www.cloudera.com/legal/commercial-terms-and-conditions/cloudera-licensed-metrics.html"

# Nome curto das roles mais comuns, para a coluna "Função principal".
ROLE_LABELS = {
    "NAMENODE": "NameNode", "SECONDARYNAMENODE": "Secondary NameNode", "JOURNALNODE": "JournalNode",
    "DATANODE": "DataNode", "HTTPFS": "HttpFS", "BALANCER": "HDFS Balancer",
    "RESOURCEMANAGER": "YARN RM", "NODEMANAGER": "NodeManager", "JOBHISTORY": "JobHistory",
    "IMPALAD": "Impala Daemon", "CATALOGSERVER": "Impala Catalog", "STATESTORE": "Impala StateStore",
    "KUDU_MASTER": "Kudu master", "KUDU_TSERVER": "Kudu tserver",
    "MASTER": "HBase Master", "REGIONSERVER": "RegionServer", "HBASERESTSERVER": "HBase REST",
    "HBASETHRIFTSERVER": "HBase Thrift", "HIVEMETASTORE": "Hive Metastore", "HIVESERVER2": "HiveServer2",
    "HUE_SERVER": "Hue", "KAFKA_BROKER": "Kafka", "KAFKA_CONNECT": "Kafka Connect", "SERVER": "ZooKeeper",
    "RANGER_ADMIN": "Ranger", "ATLAS_SERVER": "Atlas", "OOZIE_SERVER": "Oozie", "SOLR_SERVER": "Solr",
    "LIVY_SERVER_FOR_SPARK3": "Livy", "LIVY_SERVER": "Livy", "SPARK3_YARN_HISTORY_SERVER": "Spark 3 History",
    "SPARK_YARN_HISTORY_SERVER": "Spark History", "PHOENIX_QUERY_SERVER": "Phoenix QS",
    "OZONE_MANAGER": "Ozone OM", "STORAGE_CONTAINER_MANAGER": "Ozone SCM", "OZONE_RECON": "Ozone Recon",
    "OZONE_DATANODE": "Ozone DataNode", "S3_GATEWAY": "Ozone S3 Gateway", "QUEUEMANAGER_STORE": "Queue Manager",
    "NIFI_NODE": "NiFi", "SCHEMA_REGISTRY_SERVER": "Schema Registry", "STREAMS_MESSAGING_MANAGER_SERVER": "SMM",
}
# Ordem de importância na coluna (o resto vem depois, na ordem em que aparece).
ROLE_PRIORITY = ["NAMENODE", "DATANODE", "RESOURCEMANAGER", "IMPALAD", "KUDU_TSERVER", "REGIONSERVER",
                 "MASTER", "HIVEMETASTORE", "HIVESERVER2", "CATALOGSERVER", "STATESTORE", "KUDU_MASTER",
                 "KAFKA_BROKER", "RANGER_ADMIN", "HUE_SERVER", "NODEMANAGER"]
SKIP_ROLES = {"GATEWAY", "FAILOVERCONTROLLER"}


# ---------------------------------------------------------------------------
# Coleta
# ---------------------------------------------------------------------------

def _timeseries(query: str, minutes: float, rollup: str = "HOURLY") -> list[dict]:
    now = datetime.now(timezone.utc)
    resp = cm().get("timeseries", {
        "query": query, "from": _iso(now - timedelta(minutes=minutes)), "to": _iso(now),
        "desiredRollup": rollup, "mustUseDesiredRollup": "false",
    })
    out = []
    for item in _items(resp):
        for ts in item.get("timeSeries", []):
            md = ts.get("metadata", {})
            vals = [p["value"] for p in ts.get("data", []) if p.get("value") is not None]
            if vals:
                out.append({"metric": md.get("metricName"), "entity": md.get("entityName"),
                            "attrs": md.get("attributes") or {}, "values": vals})
    return out


def _host_key(ts: dict, by_name: dict[str, str]) -> str | None:
    """Resolve a série para um hostId (atributos hostId/hostname ou entityName)."""
    a = ts["attrs"]
    if a.get("hostId"):
        return a["hostId"]
    for cand in (a.get("hostname"), ts.get("entity")):
        if cand in by_name:
            return by_name[cand]
    return ts.get("entity")


def _nominal(gib: float, step: int) -> float:
    return math.ceil(gib / step - 1e-9) * step if step > 0 else gib


def _ccu(cores: float, mem_gib: float) -> dict:
    pc, pm = cores / 6, mem_gib / 12
    return {"cores": round(pc, 4), "memory": round(pm, 4), "total": round(pc + pm, 4),
            "rounded": math.ceil(pc + pm - 1e-9)}


def _roles_by_host(cluster: str, services: list[dict]) -> tuple[dict[str, list[str]], list[str]]:
    by_host: dict[str, list[str]] = defaultdict(list)
    warnings = []
    for s in services:
        try:
            roles = _items(cm().get(f"clusters/{cluster}/services/{s['name']}/roles"))
        except Exception as e:  # noqa: BLE001
            warnings.append(f"roles de {s['name']}: {e}")
            continue
        for r in roles:
            hid = (r.get("hostRef") or {}).get("hostId")
            t = r.get("type")
            if hid and t and t not in by_host[hid]:
                by_host[hid].append(t)
    return by_host, warnings


def _role_labels(types: list[str], limit: int = 8) -> str:
    shown = [t for t in types if t not in SKIP_ROLES]
    shown.sort(key=lambda t: ROLE_PRIORITY.index(t) if t in ROLE_PRIORITY else len(ROLE_PRIORITY))
    labels: list[str] = []
    for t in shown:
        lab = ROLE_LABELS.get(t, t.replace("_", " ").title())
        if lab not in labels:
            labels.append(lab)
    if not labels:
        return "gateways (clientes)" if types else "—"
    extra = len(labels) - limit
    return ", ".join(labels[:limit]) + (f" e mais {extra}" if extra > 0 else "")


def build_license_report(cluster: str, days: float = 7, data_mount_regex: str = DEFAULT_DATA_MOUNTS,
                         nominal_memory_step_gb: int = 8) -> dict:
    c = cm()
    warnings: list[str] = []
    mount_re = re.compile(data_mount_regex)

    cl = c.get(f"clusters/{cluster}")
    try:
        cm_version = c.get("cm/version").get("version")
    except Exception:  # noqa: BLE001
        cm_version = None
    services = _items(c.get(f"clusters/{cluster}/services"))
    svc_by_type = defaultdict(list)
    for s in services:
        svc_by_type[s.get("type")].append(s["name"])

    cluster_ids = {h.get("hostId") for h in _items(c.get(f"clusters/{cluster}/hosts"))}
    all_hosts = _items(c.get("hosts", {"view": "full"}))
    by_name = {h.get("hostname"): h.get("hostId") for h in all_hosts}
    roles_by_host, w = _roles_by_host(cluster, services)
    warnings += w

    # --- uso dos últimos dias (médias horárias)
    usage: dict[str, dict] = defaultdict(dict)
    try:
        for ts in _timeseries("select cpu_percent, physical_memory_used, physical_memory_total, swap_used "
                              "where category = HOST", days * 1440):
            usage[_host_key(ts, by_name)][ts["metric"]] = ts["values"]
    except Exception as e:  # noqa: BLE001
        warnings.append(f"uso dos hosts (timeseries): {e}")

    # --- filesystems (valor mais recente)
    fs: dict[str, list[dict]] = defaultdict(list)
    try:
        raw: dict[tuple, dict] = defaultdict(dict)
        for ts in _timeseries("select capacity, capacity_used where category = FILESYSTEM", 180):
            key = (_host_key(ts, by_name), ts["attrs"].get("mountpoint") or ts.get("entity"))
            raw[key][ts["metric"]] = ts["values"][-1]
        for (hid, mount), m in raw.items():
            if m.get("capacity"):
                fs[hid].append({"mount": mount, "capacity": m["capacity"], "used": m.get("capacity_used", 0),
                                "usedPct": round(100 * m.get("capacity_used", 0) / m["capacity"], 1),
                                "data": bool(mount_re.match(mount or ""))})
    except Exception as e:  # noqa: BLE001
        warnings.append(f"filesystems (timeseries): {e}")

    # --- hosts
    hosts, others = [], []
    for h in all_hosts:
        hid = h.get("hostId")
        logical, physical = h.get("numCores") or 0, h.get("numPhysicalCores") or 0
        cores = max(logical, physical)
        mem = (h.get("totalPhysMemBytes") or 0) / GIB
        nominal = _nominal(mem, nominal_memory_step_gb)
        base = {"hostname": h.get("hostname"), "short": (h.get("hostname") or hid or "?").split(".")[0],
                "hostId": hid, "cores": cores, "logicalCores": logical, "physicalCores": physical,
                "hyperthreading": bool(physical and logical > physical),
                "memoryOsGB": round(mem, 3), "memoryNominalGB": nominal,
                "ccu": {"os": _ccu(cores, mem), "nominal": _ccu(cores, nominal)}}
        if hid not in cluster_ids:
            others.append({**base, "cluster": (h.get("clusterRef") or {}).get("clusterName")})
            continue
        u = usage.get(hid, {})
        cpu = u.get("cpu_percent") or []
        used, total = u.get("physical_memory_used") or [], u.get("physical_memory_total") or []
        mem_total = (total[-1] if total else None) or h.get("totalPhysMemBytes")
        mem_pct = [100 * v / mem_total for v in used] if mem_total else []
        mounts = sorted(fs.get(hid, []), key=lambda m: m["mount"])
        data = [m for m in mounts if m["data"]]
        sys_mounts = [m for m in mounts if not m["data"]]
        types = roles_by_host.get(hid, [])
        hosts.append({
            **base, "roleTypes": types, "roles": _role_labels(types),
            "cpuPeakPct": round(max(cpu), 1) if cpu else None,
            "memoryUsedPct": [round(min(mem_pct), 1), round(max(mem_pct), 1)] if mem_pct else None,
            "swapMaxBytes": max(u["swap_used"]) if u.get("swap_used") else None,
            "usagePoints": len(cpu),
            "dataVolumes": data, "systemMounts": sys_mounts,
            "dataCapacityBytes": sum(m["capacity"] for m in data),
        })
    hosts.sort(key=lambda x: (-x["ccu"]["os"]["total"], x["hostname"] or ""))

    # --- dados e capacidade
    def service_metrics(svc_type: str, name: str, metrics: str) -> dict[str, float]:
        """Métricas do serviço; com HA o HDFS vem como 'hdfs:<nameservice>'."""
        m: dict[str, float] = {}
        for ts in _timeseries(f"select {metrics} where category = SERVICE and serviceType = {svc_type} "
                              f"and clusterName = \"{cluster}\"", 180):
            svc = (ts["attrs"].get("serviceName") or name).split(":")[0]
            if svc == name:
                m[ts["metric"]] = max(m.get(ts["metric"], 0), ts["values"][-1])
        if not m:
            raise RuntimeError(f"sem dados de {metrics} nas séries temporais")
        return m

    hdfs = {}
    for name in svc_by_type.get("HDFS", [])[:1]:
        try:
            m = service_metrics("HDFS", name, "dfs_capacity, dfs_capacity_used, dfs_capacity_used_non_hdfs, files_total")
            cfg = _items(c.get(f"clusters/{cluster}/services/{name}/config", {"view": "full"}))
            item = next((i for i in cfg if i.get("name") == "dfs_replication"), None)
            repl = int((item or {}).get("value") or (item or {}).get("default") or 3)
            hdfs = {"service": name, "capacityBytes": m.get("dfs_capacity"),
                    "usedBytes": m.get("dfs_capacity_used"), "nonHdfsUsedBytes": m.get("dfs_capacity_used_non_hdfs"),
                    "files": m.get("files_total"), "replication": repl}
        except Exception as e:  # noqa: BLE001
            warnings.append(f"HDFS: {e}")
    kudu = {}
    for name in svc_by_type.get("KUDU", [])[:1]:
        try:
            m = service_metrics("KUDU", name, "total_kudu_on_disk_size_across_kudu_replicas")
            kudu = {"service": name, "onDiskBytes": m.get("total_kudu_on_disk_size_across_kudu_replicas"),
                    "replication": 3}
        except Exception as e:  # noqa: BLE001
            warnings.append(f"Kudu: {e}")

    hdfs_used = hdfs.get("usedBytes") or 0
    kudu_disk = kudu.get("onDiskBytes") or 0
    dum_logical = hdfs_used / hdfs.get("replication", 3) + kudu_disk / 3
    data_hosts = [h for h in hosts if "DATANODE" in h["roleTypes"]]
    cum_all = sum(h["dataCapacityBytes"] for h in hosts)

    def total(base: str) -> dict:
        t = {"cores": round(sum(h["ccu"][base]["cores"] for h in hosts), 2),
             "memory": round(sum(h["ccu"][base]["memory"] for h in hosts), 2),
             "total": round(sum(h["ccu"][base]["total"] for h in hosts), 2),
             "roundedPerNode": sum(h["ccu"][base]["rounded"] for h in hosts)}
        t["memorySharePct"] = round(100 * t["memory"] / t["total"], 1) if t["total"] else None
        return t

    report = {
        "cluster": cluster, "displayName": cl.get("displayName") or cluster,
        "runtimeVersion": cl.get("fullVersion"), "cmVersion": cm_version,
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "usageWindowDays": days, "dataMountRegex": data_mount_regex,
        "nominalMemoryStepGB": nominal_memory_step_gb,
        "services": sorted(svc_by_type), "os": _os_label(all_hosts, cluster_ids),
        "totals": {
            "nodes": len(hosts), "cores": sum(h["cores"] for h in hosts),
            "physicalCores": sum(h["physicalCores"] for h in hosts),
            "logicalCores": sum(h["logicalCores"] for h in hosts),
            "memoryOsGB": round(sum(h["memoryOsGB"] for h in hosts), 2),
            "memoryNominalGB": sum(h["memoryNominalGB"] for h in hosts),
            "dataCapacityTB": round(cum_all / TIB, 6),
            "cpuPeakPct": max((h["cpuPeakPct"] for h in hosts if h["cpuPeakPct"] is not None), default=None),
        },
        "ccu": {"os": total("os"), "nominal": total("nominal")},
        "dataUnderManagement": {
            "logicalTB": round(dum_logical / TIB, 6), "rawTB": round((hdfs_used + kudu_disk) / TIB, 6),
            "hdfs": hdfs, "kudu": kudu,
            "notCounted": [s for s in ("OZONE", "KAFKA", "SOLR") if s in svc_by_type],
        },
        "capacityUnderManagement": {
            "hdfsTB": round((hdfs.get("capacityBytes") or 0) / TIB, 6),
            "allDataVolumesTB": round(cum_all / TIB, 6),
            "dataNodeVolumesTB": round(sum(h["dataCapacityBytes"] for h in data_hosts) / TIB, 6),
            "hdfsUsedPct": round(100 * hdfs_used / hdfs["capacityBytes"], 1) if hdfs.get("capacityBytes") else None,
        },
        "hosts": hosts,
        "otherHosts": others,
        "otherHostsCcuOs": round(sum(o["ccu"]["os"]["total"] for o in others), 2),
        "warnings": warnings,
    }
    report["findings"] = _findings(report)
    report["questions"] = _questions(report)
    return report


def _os_label(all_hosts: list[dict], ids: set) -> str | None:
    pretty = {"redhat": "RHEL", "rhel": "RHEL", "centos": "CentOS", "rocky": "Rocky Linux", "ubuntu": "Ubuntu",
              "sles": "SLES", "oracle": "Oracle Linux"}
    names = {pretty.get(str(n).lower(), n) if n else None
             for n in ((h.get("distribution") or {}).get("name") for h in all_hosts if h.get("hostId") in ids)}
    vers = {(h.get("distribution") or {}).get("version") for h in all_hosts if h.get("hostId") in ids}
    names.discard(None)
    vers.discard(None)
    if len(names) == 1:
        return f"{names.pop()} {vers.pop()}" if len(vers) == 1 else names.pop()
    return None


# ---------------------------------------------------------------------------
# Achados e perguntas
# ---------------------------------------------------------------------------

def br(n: float | None, d: int = 2) -> str:
    """Número no formato pt-BR."""
    if n is None:
        return "—"
    return f"{n:,.{d}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def tb(n: float | None) -> str:
    """TB com 4 casas quando o valor é pequeno (laboratórios), 2 nos demais."""
    return br(n, 4 if n is not None and n < 0.1 else 2)


def _host_status(h: dict) -> tuple[str, str]:
    full = [m for m in h["systemMounts"] if m["usedPct"] >= 80]
    if full:
        m = max(full, key=lambda x: x["usedPct"])
        return "warn", f"{m['mount']} {m['usedPct']:.0f}%"
    if h["dataVolumes"] and max(m["usedPct"] for m in h["dataVolumes"]) >= 75:
        return "warn", "disco"
    if h.get("swapMaxBytes"):
        return "warn", "swap"
    mem = h.get("memoryUsedPct")
    if h.get("cpuPeakPct") is not None and h["cpuPeakPct"] < 5 and mem and mem[1] < 20:
        return "idle", "ocioso"
    return "ok", "ok"


def _findings(r: dict) -> list[dict]:
    out: list[dict] = []
    hosts = r["hosts"]
    cum = r["capacityUnderManagement"]
    hdfs = r["dataUnderManagement"]["hdfs"]
    if cum.get("hdfsUsedPct") is not None and cum["hdfsUsedPct"] >= 70:
        used, cap = hdfs["usedBytes"] / TIB, hdfs["capacityBytes"] / TIB
        out.append({"level": "warn", "title": f"HDFS a {br(cum['hdfsUsedPct'], 0)}%",
                    "text": f"São {br(used, 1)} TB usados de {br(cap, 1)} TB. Vale planejar expansão ou "
                            "limpeza antes de chegar a 85%."})
    for h in hosts:
        for m in h["systemMounts"]:
            if m["usedPct"] >= 80:
                out.append({"level": "warn", "title": f"{m['mount']} do {h['short']} em {br(m['usedPct'], 0)}%",
                            "text": "Se encher, os serviços desse host podem parar de gravar logs e dados locais."})
        if h.get("swapMaxBytes"):
            out.append({"level": "warn", "title": f"Swap em uso no {h['short']}",
                        "text": f"Pico de {br(h['swapMaxBytes'] / GIB, 1)} GB de swap na janela."})
    # mesmo ponto de montagem com uso muito diferente entre os DataNodes
    per_mount: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for h in hosts:
        if "DATANODE" in h["roleTypes"]:
            for m in h["dataVolumes"]:
                per_mount[m["mount"]].append((h["short"], m["usedPct"]))
    for mount, vals in sorted(per_mount.items()):
        if len(vals) > 1:
            hi, lo = max(vals, key=lambda v: v[1]), min(vals, key=lambda v: v[1])
            if hi[1] - lo[1] >= 30:
                out.append({"level": "warn", "title": f"{mount} desbalanceado",
                            "text": f"No {hi[0]} está com {br(hi[1], 0)}%; no {lo[0]}, com {br(lo[1], 0)}%. "
                                    "O volume pode ter entrado em momentos diferentes ou o DataNode não está "
                                    "distribuindo os blocos entre volumes."})
    idle_vol = sum(m["capacity"] for h in hosts if "DATANODE" not in h["roleTypes"]
                   for m in h["dataVolumes"] if m["usedPct"] < 5)
    if idle_vol >= TIB:
        out.append({"level": "note", "title": f"{br(idle_vol / TIB, 1)} TB de discos de dados quase vazios fora dos DataNodes",
                    "text": "Volumes de dados com menos de 5% de uso em hosts sem DataNode. Se o contrato cobrar "
                            "Capacity Under Management sobre todos os volumes, eles pesam sem uso real."})
    idle = [h for h in hosts if _host_status(h)[0] == "idle"]
    if idle:
        ccu = sum(h["ccu"]["os"]["total"] for h in idle)
        out.append({"level": "note", "title": "Hosts ociosos: " + ", ".join(h["short"] for h in idle),
                    "text": f"Pico de CPU abaixo de 5% e memória abaixo de 20%. Juntos somam {br(ccu, 1)} CCU; "
                            "são os primeiros candidatos a redução."})
    peak = r["totals"]["cpuPeakPct"]
    if peak is not None and peak < 20:
        out.append({"level": "info", "title": "Sobra CPU em todo o cluster",
                    "text": f"O pico horário não passou de {br(peak, 1)}%. A CCU é calculada sobre o recurso "
                            "disponível, então paga pela folga."})
    share = r["ccu"]["os"]["memorySharePct"]
    if share and share > 50:
        out.append({"level": "info", "title": "A memória pesa mais que os cores na CCU",
                    "text": f"Cerca de {br(share, 0)}% das CCU vêm da parcela de memória. Reduzir RAM tem mais "
                            "efeito na licença do que reduzir cores."})
    ht = [h["short"] for h in hosts if h["hyperthreading"]]
    if ht:
        out.append({"level": "info", "title": "Hyperthreading ligado",
                    "text": "Os cores lógicos foram usados no cálculo (vale o maior valor) em: " + ", ".join(ht) + "."})
    return out


def _questions(r: dict) -> list[dict]:
    os_, nom = r["ccu"]["os"], r["ccu"]["nominal"]
    t = r["totals"]
    qs = [
        {"topic": "Base de memória", "text": f"RAM vista pelo SO ({br(os_['total'])} CCU) ou RAM nominal "
                                             f"({br(nom['total'])} CCU)."},
        {"topic": "Arredondamento", "text": "a página não define; arredondando cada Node para cima, o total vai a "
                                            + (f"{os_['roundedPerNode']} CCU nas duas bases."
                                               if os_["roundedPerNode"] == nom["roundedPerNode"] else
                                               f"{os_['roundedPerNode']} CCU (base SO) ou {nom['roundedPerNode']} "
                                               "(nominal).")},
    ]
    if any(h["hyperthreading"] for h in r["hosts"]):
        qs.append({"topic": "Hyperthreading", "text": f"os hosts têm {t['logicalCores']} cores lógicos e "
                   f"{t['physicalCores']} físicos; o cálculo usou os lógicos."})
    else:
        extra = t["cores"] / 6
        qs.append({"topic": "Hyperthreading", "text": f"o Cloudera Manager informa {t['cores']} cores, sem diferença "
                   "entre lógicos e físicos. Se o hyperthreading estiver ligado e os cores lógicos forem o dobro, "
                   f"a parcela de cores dobra e o total iria a cerca de {br(os_['total'] + extra, 0)} CCU."})
    if r["otherHosts"]:
        names = ", ".join(o["short"] for o in r["otherHosts"][:12]) + ("…" if len(r["otherHosts"]) > 12 else "")
        qs.append({"topic": "Quais Nodes contam", "text": f"o cálculo inclui os {t['nodes']} hosts do cluster. O mesmo "
                   f"Cloudera Manager gerencia outros {len(r['otherHosts'])} hosts fora dele ({names}), que somariam "
                   f"cerca de {br(r['otherHostsCcuOs'], 0)} CCU se também rodarem produtos Cloudera."})
    else:
        qs.append({"topic": "Quais Nodes contam", "text": f"o cálculo inclui os {t['nodes']} hosts, inclusive "
                                                          "gateways e hosts de serviço."})
    dum = r["dataUnderManagement"]
    extra = f" Não entraram: {', '.join(s.title() for s in dum['notCounted'])}." if dum["notCounted"] else ""
    qs.append({"topic": "Dados brutos ou lógicos", "text": f"Data Under Management foi calculada sem réplicas "
               f"({tb(dum['logicalTB'])} TB); com réplicas fica em {tb(dum['rawTB'])} TB.{extra}"})
    qs.append({"topic": "Métrica cobrada", "text": "a página não diz qual métrica vale para cada produto; isso "
                                                   "consta no contrato."})
    return qs


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

CSS = """
:root{
  --bg:#f5f7f8; --surface:#ffffff; --ink:#15212b; --muted:#5b6b78; --line:#d9e1e6;
  --steel:#2f5d7c; --steel-soft:#e3edf3; --amber:#b86e12; --amber-soft:#f8ecdb;
  --ok:#2f7d56; --warn:#a85d00; --crit:#b23a3a;
  --font-display:"IBM Plex Sans Condensed","Arial Narrow",system-ui,sans-serif;
  --font-body:"IBM Plex Sans",system-ui,-apple-system,"Segoe UI",sans-serif;
  --font-mono:"IBM Plex Mono",ui-monospace,"Cascadia Mono",Consolas,monospace;
}
@media screen and (prefers-color-scheme: dark){:root:not([data-theme="light"]){
  --bg:#0f161c; --surface:#16212a; --ink:#e4ebf0; --muted:#97a8b5; --line:#2a3946;
  --steel:#7fb0d1; --steel-soft:#1d3241; --amber:#e3a24e; --amber-soft:#3a2a14;
  --ok:#6cc596; --warn:#e9a04a; --crit:#ec7d7d; color-scheme:dark}}
:root[data-theme="dark"]{
  --bg:#0f161c; --surface:#16212a; --ink:#e4ebf0; --muted:#97a8b5; --line:#2a3946;
  --steel:#7fb0d1; --steel-soft:#1d3241; --amber:#e3a24e; --amber-soft:#3a2a14;
  --ok:#6cc596; --warn:#e9a04a; --crit:#ec7d7d; color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.55 var(--font-body)}
.wrap{max-width:1080px;margin:0 auto;padding-block:40px 64px;
  padding-left:max(16px,env(safe-area-inset-left,0px));padding-right:max(16px,env(safe-area-inset-right,0px));
  display:flex;flex-direction:column;gap:44px}
h1,h2,h3{font-family:var(--font-display);text-wrap:balance;margin:0;letter-spacing:.005em}
h1{font-size:clamp(30px,5vw,44px);line-height:1.05}
h2{font-size:24px}
h3{font-size:17px}
p{margin:0;max-width:68ch}
.eyebrow{font:500 12px/1 var(--font-mono);text-transform:uppercase;letter-spacing:.12em;color:var(--steel)}
.muted{color:var(--muted)}
header{display:flex;flex-direction:column;gap:14px;border-bottom:1px solid var(--line);padding-bottom:28px}
.meta{display:flex;flex-wrap:wrap;gap:8px}
.chip{font:500 12px/1 var(--font-mono);padding:6px 9px;border:1px solid var(--line);border-radius:4px;background:var(--surface);color:var(--muted)}
section{display:flex;flex-direction:column;gap:18px}
.sec-head{display:flex;flex-direction:column;gap:6px}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
.tile{background:var(--surface);border:1px solid var(--line);border-radius:6px;padding:14px 16px;display:flex;flex-direction:column;gap:4px;min-width:0}
.tile .k{font:500 11px/1.2 var(--font-mono);text-transform:uppercase;letter-spacing:.1em;color:var(--muted)}
.tile .v{font:600 28px/1.1 var(--font-display);font-variant-numeric:tabular-nums}
.tile .s{font-size:12.5px;color:var(--muted)}
.tile.lic{background:var(--amber-soft);border-color:transparent}
.tile.lic .v{color:var(--amber)}
.tbl{overflow-x:auto;border:1px solid var(--line);border-radius:6px;background:var(--surface)}
table{border-collapse:collapse;width:100%;min-width:760px;font-size:13.5px}
th,td{padding:9px 12px;text-align:left;border-bottom:1px solid var(--line);vertical-align:top}
th{font:500 11px/1.3 var(--font-mono);text-transform:uppercase;letter-spacing:.08em;color:var(--muted);background:var(--bg);white-space:nowrap}
td.r,th.r{text-align:right}
td.r{font-variant-numeric:tabular-nums;font-family:var(--font-mono);font-size:13px;white-space:nowrap}
tbody tr:last-child td{border-bottom:0}
tfoot td{font-weight:600;background:var(--steel-soft);border-top:1px solid var(--line);border-bottom:0}
.host{font-family:var(--font-mono);font-weight:500;white-space:nowrap}
.roles{color:var(--muted);font-size:12.5px;min-width:200px}
.pill{display:inline-block;font:500 11px/1 var(--font-mono);padding:4px 7px;border-radius:999px;white-space:nowrap}
.pill.ok{color:var(--ok);background:color-mix(in srgb,var(--ok) 14%,transparent)}
.pill.warn{color:var(--warn);background:color-mix(in srgb,var(--warn) 16%,transparent)}
.pill.idle{color:var(--steel);background:var(--steel-soft)}
.chart{background:var(--surface);border:1px solid var(--line);border-radius:6px;padding:18px 18px 14px;display:flex;flex-direction:column;gap:10px}
.legend{display:flex;flex-wrap:wrap;gap:16px;font-size:12.5px;color:var(--muted)}
.legend i{display:inline-block;width:11px;height:11px;border-radius:2px;margin-right:6px;vertical-align:-1px}
.bars{display:flex;flex-direction:column;gap:10px}
.bar-row{display:grid;grid-template-columns:120px 1fr 64px;align-items:center;gap:10px}
.bar-row .lbl{font:500 12.5px/1 var(--font-mono);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bar-row .val{font:500 12.5px/1 var(--font-mono);text-align:right;font-variant-numeric:tabular-nums}
.track{height:16px;background:var(--bg);border-radius:3px;display:flex;overflow:hidden}
.seg-c{background:var(--steel)}
.seg-m{background:var(--amber)}
.axis{display:grid;grid-template-columns:120px 1fr 64px;gap:10px}
.ticks{position:relative;height:16px;font:400 11px/1 var(--font-mono);color:var(--muted)}
.ticks span{position:absolute;transform:translateX(-50%)}
.ticks span:first-child{transform:none}
.toggle{display:inline-flex;border:1px solid var(--line);border-radius:6px;overflow:hidden;background:var(--surface);align-self:flex-start;flex-wrap:wrap}
.toggle button{font:500 13px/1 var(--font-body);color:var(--muted);background:transparent;border:0;padding:10px 14px;cursor:pointer}
.toggle button[aria-pressed="true"]{background:var(--steel);color:var(--surface)}
.toggle button:focus-visible{outline:2px solid var(--amber);outline-offset:-2px}
[hidden]{display:none!important}
.formula{background:var(--surface);border:1px solid var(--line);border-radius:6px;padding:16px 18px;display:flex;flex-direction:column;gap:6px}
.formula code{font:500 15px/1.4 var(--font-mono);color:var(--amber)}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:14px}
.grid2>*{min-width:0}
.card{background:var(--surface);border:1px solid var(--line);border-radius:6px;padding:16px 18px;display:flex;flex-direction:column;gap:10px}
.card dl{margin:0;display:grid;grid-template-columns:1fr auto;gap:6px 14px;font-size:14px}
.card dt{color:var(--muted)}
.card dd{margin:0;font-family:var(--font-mono);font-variant-numeric:tabular-nums;text-align:right}
.card dd.big{font:600 20px/1.2 var(--font-display);color:var(--amber)}
.findings{display:flex;flex-direction:column;gap:10px;margin:0;padding:0;list-style:none}
.findings li{background:var(--surface);border:1px solid var(--line);border-left:4px solid var(--steel);border-radius:6px;padding:12px 16px;display:flex;flex-direction:column;gap:4px}
.findings li.warn{border-left-color:var(--warn)}
.findings li.info{border-left-color:var(--ok)}
.findings b{font-family:var(--font-display);font-size:16px}
.qs{margin:0;padding-left:20px;display:flex;flex-direction:column;gap:6px}
footer{border-top:1px solid var(--line);padding-top:20px;font-size:13px;color:var(--muted);display:flex;flex-direction:column;gap:6px}
a{color:var(--steel)}
@media (max-width:520px){.bar-row,.axis{grid-template-columns:84px 1fr 54px}}
@page{size:A4 landscape;margin:12mm}
@media print{
  *{-webkit-print-color-adjust:exact;print-color-adjust:exact}
  body{font-size:12px;background:#fff}
  .wrap{padding:0;gap:26px;max-width:none}
  .toggle{display:none}
  .tbl{overflow:visible}
  table{min-width:0;font-size:11px}
  th,td{padding:5px 8px}
  tr,.tile,.card,.findings li,.formula,.sec-head{break-inside:avoid}
  .sec-head{break-after:avoid}
  h2{break-after:avoid}
}
"""

FONTS = ('<link rel="preconnect" href="https://fonts.googleapis.com">\n'
         '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>\n'
         '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500'
         '&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Sans+Condensed:wght@600;700&display=swap">')

JS = """
(function(){
  var KEY='cm-license-base';
  function show(base){
    document.querySelectorAll('[data-base]').forEach(function(el){el.hidden=el.getAttribute('data-base')!==base;});
    document.querySelectorAll('.toggle button').forEach(function(b){b.setAttribute('aria-pressed',b.getAttribute('data-show')===base);});
    try{localStorage.setItem(KEY,base);}catch(e){}
  }
  document.querySelectorAll('.toggle button').forEach(function(b){b.addEventListener('click',function(){show(b.getAttribute('data-show'));});});
  var s=null;try{s=localStorage.getItem(KEY);}catch(e){}
  show(s==='nominal'?'nominal':'os');
})();
"""


def _e(s: Any) -> str:
    return html.escape(str(s))


def _gb(b: float) -> str:
    if b < 10 * GIB:
        return f"{br(b / GIB, 2)} GB"
    return f"{br(b / GIB, 0)} GB" if b < TIB else f"{br(b / TIB, 2)} TB"


def _vol_summary(vols: list[dict]) -> str:
    if not vols:
        return "—"
    sizes = sorted(m["capacity"] for m in vols)
    median = sizes[len(sizes) // 2]
    same = all(abs(s - median) / median < 0.05 for s in sizes)
    head = f"{len(vols)} × {_gb(median)}" if same else f"{len(vols)} volumes · {_gb(sum(sizes))}"
    pcts = sorted(m["usedPct"] for m in vols)
    lo, hi = pcts[0], pcts[-1]
    rng = f"{br(lo, 0)}%" if round(lo) == round(hi) else f"{br(lo, 0)}–{br(hi, 0)}%"
    return f"{head} · {rng}"


def _range(v: list[float] | None) -> str:
    if not v:
        return "—"
    lo, hi = round(v[0]), round(v[1])
    return f"{lo}%" if lo == hi else f"{lo}–{hi}%"


def _tile(k: str, v: str, s: str, lic: bool = False, base: str | None = None) -> str:
    attr = (f' data-base="{base}"' + (" hidden" if base == "nominal" else "")) if base else ""
    return (f'<div class="tile{" lic" if lic else ""}"{attr}><span class="k">{_e(k)}</span>'
            f'<span class="v">{_e(v)}</span><span class="s">{_e(s)}</span></div>')


def _ccu_block(r: dict, base: str) -> str:
    hosts = r["hosts"]
    mx = max((h["ccu"][base]["total"] for h in hosts), default=1)
    step = 1 if mx <= 10 else 5 if mx <= 50 else 10 if mx <= 100 else 25
    top = math.ceil(mx / step) * step
    bars, rows = [], []
    for h in hosts:
        c, mem = h["ccu"][base], h["memoryOsGB"] if base == "os" else h["memoryNominalGB"]
        bars.append(f'<div class="bar-row"><span class="lbl" title="{_e(h["hostname"])}">{_e(h["short"])}</span>'
                    f'<div class="track" role="img" aria-label="{_e(h["short"])}: {br(c["total"])} CCU">'
                    f'<div class="seg-c" style="width:{100 * c["cores"] / top:.3f}%"></div>'
                    f'<div class="seg-m" style="width:{100 * c["memory"] / top:.3f}%"></div></div>'
                    f'<span class="val">{br(c["total"])}</span></div>')
        rows.append(f'<tr><td class="host">{_e(h["short"])}</td><td class="r">{h["cores"]}</td>'
                    f'<td class="r">{br(mem)} GB</td><td class="r">{br(c["cores"], 3)}</td>'
                    f'<td class="r">{br(c["memory"], 3)}</td><td class="r"><b>{br(c["total"], 3)}</b></td>'
                    f'<td class="r">{c["rounded"]}</td></tr>')
    t = r["ccu"][base]
    mem_total = r["totals"]["memoryOsGB"] if base == "os" else r["totals"]["memoryNominalGB"]
    ticks = "".join(f'<span style="left:{100 * i / top:.3f}%">{i}</span>' for i in range(0, top + 1, step))
    return f"""
    <div data-base="{base}" class="chart" aria-label="CCU por host">
      <div class="legend"><span><i style="background:var(--steel)"></i>parcela de cores (÷ 6)</span><span><i style="background:var(--amber)"></i>parcela de memória (÷ 12)</span></div>
      <div class="bars">{''.join(bars)}</div>
      <div class="axis"><span></span><div class="ticks">{ticks}</div><span></span></div>
    </div>
    <div data-base="{base}" class="tbl">
      <table>
        <thead><tr><th>Host</th><th class="r">Cores</th><th class="r">Memória</th><th class="r">Cores ÷ 6</th><th class="r">Memória ÷ 12</th><th class="r">CCU</th><th class="r">Arredondada</th></tr></thead>
        <tbody>{''.join(rows)}</tbody>
        <tfoot><tr><td>Total</td><td class="r">{r['totals']['cores']}</td><td class="r">{br(mem_total)} GB</td><td class="r">{br(t['cores'])}</td><td class="r">{br(t['memory'])}</td><td class="r">{br(t['total'])}</td><td class="r">{t['roundedPerNode']}</td></tr></tfoot>
      </table>
    </div>"""


def render_html(r: dict) -> str:
    t, cum, dum = r["totals"], r["capacityUnderManagement"], r["dataUnderManagement"]
    name = r["displayName"]
    date = datetime.strptime(r["generatedAt"], "%Y-%m-%dT%H:%M:%SZ").strftime("%d/%m/%Y")
    days = r["usageWindowDays"]
    os_mems = sorted({br(h["memoryOsGB"]) for h in r["hosts"]})
    nom_mems = sorted({br(h["memoryNominalGB"], 0) for h in r["hosts"]}, key=lambda s: float(s.replace(".", "")))
    peak_host = max((h for h in r["hosts"] if h["cpuPeakPct"] is not None), key=lambda h: h["cpuPeakPct"], default=None)
    hdfs, kudu = dum["hdfs"], dum["kudu"]

    chips = [f"Cloudera Manager {r['cmVersion']}" if r["cmVersion"] else None,
             f"CDP Runtime {r['runtimeVersion']}" if r["runtimeVersion"] else None,
             r["os"], f"{t['nodes']} hosts · {len(r['services'])} serviços", f"Uso medido: últimos {br(days, 0)} dias"]
    chips_html = "".join(f'<span class="chip">{_e(c)}</span>' for c in chips if c)

    tiles = "".join([
        _tile("Nodes", str(t["nodes"]), "1 cluster"),
        _tile("Cores", str(t["cores"]), "com hyperthreading" if t["logicalCores"] > t["physicalCores"] > 0
              else "sem hyperthreading"),
        _tile("Memória", f"{br(t['memoryOsGB'], 1 if t['memoryOsGB'] < 1000 else 0)} GB", "vista pelo SO"),
        _tile("Pico de CPU", f"{br(t['cpuPeakPct'], 1)}%" if t["cpuPeakPct"] is not None else "—",
              f"média horária, {peak_host['short']}" if peak_host else "sem dados"),
        _tile("CCU", br(r["ccu"]["os"]["total"]), "RAM vista pelo SO", True, "os"),
        _tile("CCU", br(r["ccu"]["nominal"]["total"]), "RAM nominal", True, "nominal"),
        _tile("Data Under Mgmt", f"{tb(dum['logicalTB'])} TB",
              f"lógico; {tb(dum['rawTB'])} TB com réplicas", True),
        _tile("Capacity Under Mgmt", f"{br(cum['hdfsTB'])} TB", "capacidade do HDFS", True),
    ])

    inv = []
    for h in r["hosts"]:
        cls, label = _host_status(h)
        inv.append(f'<tr><td class="host" title="{_e(h["hostname"])}">{_e(h["short"])}</td>'
                   f'<td class="roles">{_e(h["roles"])}</td><td class="r">{h["cores"]}</td>'
                   f'<td class="r">{br(h["memoryOsGB"], 1)} GB</td>'
                   f'<td class="r">{br(h["cpuPeakPct"], 1) + "%" if h["cpuPeakPct"] is not None else "—"}</td>'
                   f'<td class="r">{_range(h["memoryUsedPct"])}</td><td class="r">{_vol_summary(h["dataVolumes"])}</td>'
                   f'<td><span class="pill {cls}">{_e(label)}</span></td></tr>')
    swap_note = ("Swap usado: zero em todos os hosts." if all(not h.get("swapMaxBytes") for h in r["hosts"])
                 else "Há hosts usando swap (veja os achados).")

    cards_dum = [("Dados lógicos" + (" (HDFS + Kudu)" if kudu else " no HDFS"), f"{tb(dum['logicalTB'])} TB", True),
                 (f"Espaço com {hdfs.get('replication', 3)} réplicas", f"{tb(dum['rawTB'])} TB", False)]
    if kudu:
        cards_dum.append(("Kudu, com réplicas", _gb(kudu.get("onDiskBytes") or 0), False))
    if hdfs.get("files"):
        cards_dum.append(("Arquivos no HDFS", br(hdfs["files"], 0), False))
    cards_cum = [(f"Só HDFS ({sum(1 for h in r['hosts'] if 'DATANODE' in h['roleTypes'])} DataNodes)", f"{br(cum['hdfsTB'])} TB", True),
                 ("Todos os discos de dados", f"{br(cum['allDataVolumesTB'])} TB", False),
                 ("Discos dos DataNodes", f"{br(cum['dataNodeVolumesTB'])} TB", False)]
    if cum.get("hdfsUsedPct") is not None:
        cards_cum.append(("Ocupado de fato no HDFS", f"{br(cum['hdfsUsedPct'], 1)}%", False))

    def dl(items):
        big_cls = ' class="big"'
        return "".join(f'<dt>{_e(k)}</dt><dd{big_cls if big else ""}>{_e(v)}</dd>' for k, v, big in items)

    not_counted = [s.title() for s in dum["notCounted"]]
    data_note = "O volume vem do HDFS" + (" e do Kudu" if kudu else "") + "."
    if not_counted:
        names = not_counted[0] if len(not_counted) == 1 else ", ".join(not_counted[:-1]) + " e " + not_counted[-1]
        plural = len(not_counted) > 1
        data_note += (f" {names} {'ficaram' if plural else 'ficou'} fora da Data Under Management, porque o "
                      f"Cloudera Manager não expõe o espaço usado {'deles' if plural else 'dele'} de forma "
                      "comparável. Os discos são os mesmos dos hosts e já entram na Capacity Under Management.")

    others = r["otherHosts"]
    gpu_note = "nenhum host com GPU informado pelo CM"
    findings = "".join(f'<li class="{_e(f["level"])}"><b>{_e(f["title"])}</b><span>{_e(f["text"])}</span></li>'
                       for f in r["findings"]) or '<li class="info"><b>Nada a destacar</b><span>Nenhum limite foi ultrapassado.</span></li>'
    questions = "".join(f'<li><b>{_e(q["topic"])}:</b> {_e(q["text"])}</li>' for q in r["questions"])
    warn_html = ("".join(f"<span>Aviso na coleta: {_e(w)}</span>" for w in r["warnings"]))

    return f"""<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{_e(name)} Capacidade e Licenças</title>
{FONTS}
<style>{CSS}</style>
</head>
<body>
<div class="wrap">
  <header>
    <span class="eyebrow">Levantamento de infraestrutura · {date}</span>
    <h1>{_e(name)}: capacidade dos hosts e licenciamento Cloudera</h1>
    <p class="muted">Inventário de CPU, memória e disco dos {t['nodes']} hosts do cluster, com o cálculo das métricas de licenciamento (CCU, Data Under Management e Capacity Under Management) pela definição oficial da Cloudera.</p>
    <div class="meta">{chips_html}</div>
  </header>

  <section aria-labelledby="s-resumo">
    <div class="sec-head"><span class="eyebrow">Resumo</span><h2 id="s-resumo">Os números do cluster</h2></div>
    <div class="tiles">{tiles}</div>
  </section>

  <section aria-labelledby="s-inv">
    <div class="sec-head">
      <span class="eyebrow">Inventário</span>
      <h2 id="s-inv">Recursos e uso por host</h2>
      <p class="muted">Capacidade informada pelo Cloudera Manager e uso nos últimos {br(days, 0)} dias (médias horárias). {swap_note}</p>
    </div>
    <div class="tbl">
      <table>
        <thead><tr><th>Host</th><th>Função principal</th><th class="r">Cores</th><th class="r">RAM</th><th class="r">CPU pico</th><th class="r">Memória usada</th><th class="r">Discos de dados</th><th>Situação</th></tr></thead>
        <tbody>{''.join(inv)}</tbody>
        <tfoot><tr><td>Total</td><td></td><td class="r">{t['cores']}</td><td class="r">{br(t['memoryOsGB'], 1)} GB</td><td colspan="4" class="muted">Discos de dados: {br(t['dataCapacityTB'])} TB no total</td></tr></tfoot>
      </table>
    </div>
  </section>

  <section aria-labelledby="s-lic">
    <div class="sec-head">
      <span class="eyebrow">Licenciamento</span>
      <h2 id="s-lic">Cloudera Compute Units (CCU)</h2>
      <p class="muted">A CCU é calculada sobre os recursos disponíveis em cada host, não sobre o consumo. Todo servidor ou VM que roda produtos Cloudera conta como Node, então os {t['nodes']} hosts do cluster entram na conta.</p>
    </div>
    <div class="formula">
      <span class="eyebrow">Fórmula oficial, por Node</span>
      <code>CCU = cores ÷ 6 + memória em GB ÷ 12</code>
      <span class="muted" style="font-size:13px">1 GB = 1.024 MB. Para cores, vale o maior número entre virtuais, físicos e com hyperthreading.</span>
    </div>
    <div class="toggle" role="group" aria-label="Base de memória do cálculo">
      <button type="button" data-show="os" aria-pressed="true">RAM vista pelo SO</button>
      <button type="button" data-show="nominal" aria-pressed="false">RAM nominal</button>
    </div>
    <p class="muted" style="font-size:13.5px" data-base="os">Memória que o Linux e o Cloudera Manager enxergam em cada host ({' / '.join(os_mems)} GB). É um pouco menor que o tamanho nominal porque o kernel e o firmware reservam uma parte.</p>
    <p class="muted" style="font-size:13.5px" data-base="nominal" hidden>Tamanho nominal estimado ({' / '.join(nom_mems)} GB), arredondando a RAM do SO para cima em múltiplos de {r['nominalMemoryStepGB']} GB. É a base mais comum em auditoria de licença; confirme com o tamanho contratado das VMs ou servidores.</p>
    {_ccu_block(r, 'os')}
    {_ccu_block(r, 'nominal').replace('data-base="nominal"', 'data-base="nominal" hidden')}
  </section>

  <section aria-labelledby="s-data">
    <div class="sec-head">
      <span class="eyebrow">Licenciamento</span>
      <h2 id="s-data">Dados e capacidade sob gestão</h2>
      <p class="muted">As duas métricas de armazenamento usam TB de 1.024 GB. {_e(data_note)}</p>
    </div>
    <div class="grid2">
      <div class="card"><h3>Data Under Management</h3>
        <p class="muted" style="font-size:13.5px">Total de dados que os produtos Cloudera estão configurados para acessar.</p>
        <dl>{dl(cards_dum)}</dl></div>
      <div class="card"><h3>Capacity Under Management</h3>
        <p class="muted" style="font-size:13.5px">Soma da capacidade utilizável (após formatação) dos volumes usados pelos produtos.</p>
        <dl>{dl(cards_cum)}</dl></div>
    </div>
    <div class="tbl">
      <table style="min-width:520px">
        <thead><tr><th>Outras métricas da página</th><th class="r">Valor</th><th>Observação</th></tr></thead>
        <tbody>
          <tr><td>Node</td><td class="r">{t['nodes']}</td><td class="muted">todos rodam serviço ou cliente Cloudera</td></tr>
          <tr><td>Cluster</td><td class="r">1</td><td class="muted">{_e(name)}{_e(f"; o mesmo CM gerencia outros {len(others)} hosts fora dele") if others else ""}</td></tr>
          <tr><td>Core</td><td class="r">{t['cores']}</td><td class="muted">{t['logicalCores']} lógicos, {t['physicalCores']} físicos</td></tr>
          <tr><td>CGU (GPU)</td><td class="r">0</td><td class="muted">{gpu_note}</td></tr>
          <tr><td>Agent (MiNiFi)</td><td class="r">0</td><td class="muted">não medido pelo Cloudera Manager</td></tr>
        </tbody>
      </table>
    </div>
  </section>

  <section aria-labelledby="s-pts">
    <div class="sec-head"><span class="eyebrow">Achados</span><h2 id="s-pts">O que chama atenção</h2></div>
    <ul class="findings">{findings}</ul>
  </section>

  <section aria-labelledby="s-q">
    <div class="sec-head"><span class="eyebrow">Para confirmar</span><h2 id="s-q">Pontos a validar no Order Form</h2></div>
    <ol class="qs">{questions}</ol>
  </section>

  <footer>
    <span>Fontes: API do Cloudera Manager (hosts, roles, séries temporais de CPU, memória, filesystems, HDFS e Kudu) e a página <a href="{METRICS_URL}" target="_blank" rel="noopener">Cloudera Licensed Metrics</a>.</span>
    <span>Levantamento gerado em {date} pelo cm-mcp-server. Os valores de uso são médias horárias dos últimos {br(days, 0)} dias. Volumes de dados: pontos de montagem que casam com <code>{_e(r['dataMountRegex'])}</code>.</span>
    {warn_html}
  </footer>
</div>
<script>{JS}</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# PDF (Edge/Chrome headless)
# ---------------------------------------------------------------------------

BROWSER_CANDIDATES = [
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
]


def find_browser() -> str | None:
    env = os.environ.get("CM_PDF_BROWSER")
    if env:
        return env if Path(env).exists() or shutil.which(env) else None
    for name in ("msedge", "google-chrome", "chrome", "chromium", "chromium-browser", "microsoft-edge"):
        p = shutil.which(name)
        if p:
            return p
    return next((p for p in BROWSER_CANDIDATES if Path(p).exists()), None)


def html_to_pdf(html_path: Path, pdf_path: Path, timeout: float = 120) -> None:
    browser = find_browser()
    if not browser:
        raise RuntimeError("Nenhum Edge/Chrome/Chromium encontrado para gerar o PDF. Instale um deles ou "
                           "defina CM_PDF_BROWSER com o caminho do executável. O HTML foi gerado em "
                           f"{html_path}.")
    with tempfile.TemporaryDirectory(prefix="cm-pdf-") as profile:
        cmd = [browser, "--headless=new", "--disable-gpu", "--no-first-run", "--no-default-browser-check",
               f"--user-data-dir={profile}", "--no-pdf-header-footer", "--virtual-time-budget=10000",
               f"--print-to-pdf={pdf_path}", html_path.resolve().as_uri()]
        proc = subprocess.run(cmd, capture_output=True, timeout=timeout, check=False)
    if not pdf_path.exists() or pdf_path.stat().st_size == 0:
        raise RuntimeError(f"O navegador não gerou o PDF (código {proc.returncode}): "
                           f"{proc.stderr.decode(errors='replace')[-500:]}")


def _slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "-", s).strip("-").lower() or "cluster"


def write_report(r: dict, fmt: str, output_dir: str | None = None) -> dict[str, str]:
    """Grava o HTML (e o PDF, se pedido) e devolve os caminhos."""
    out = Path(output_dir or os.environ.get("CM_REPORT_DIR") or Path.home() / "cm-reports").expanduser()
    out.mkdir(parents=True, exist_ok=True)
    stem = f"licenciamento-{_slug(r['cluster'])}-{r['generatedAt'][:10].replace('-', '')}"
    html_path = out / f"{stem}.html"
    html_path.write_text(render_html(r), encoding="utf-8")
    paths = {"html": str(html_path)}
    if fmt == "pdf":
        pdf_path = out / f"{stem}.pdf"
        html_to_pdf(html_path, pdf_path)
        paths["pdf"] = str(pdf_path)
    return paths


def _summary(r: dict) -> dict:
    return {
        "cluster": r["cluster"], "nodes": r["totals"]["nodes"], "cores": r["totals"]["cores"],
        "memoryOsGB": r["totals"]["memoryOsGB"], "memoryNominalGB": r["totals"]["memoryNominalGB"],
        "ccu": {"os": r["ccu"]["os"]["total"], "nominal": r["ccu"]["nominal"]["total"],
                "roundedPerNodeOs": r["ccu"]["os"]["roundedPerNode"],
                "roundedPerNodeNominal": r["ccu"]["nominal"]["roundedPerNode"]},
        "dataUnderManagementTB": {"logical": r["dataUnderManagement"]["logicalTB"],
                                  "raw": r["dataUnderManagement"]["rawTB"]},
        "capacityUnderManagementTB": {"hdfs": r["capacityUnderManagement"]["hdfsTB"],
                                      "allDataVolumes": r["capacityUnderManagement"]["allDataVolumesTB"]},
        "findings": [f["title"] for f in r["findings"]],
        "otherHosts": len(r["otherHosts"]), "warnings": r["warnings"],
    }


@mcp.tool()
def license_report(cluster: str, output: str = "json", days: float = 7, output_dir: str | None = None,
                   data_mount_regex: str = DEFAULT_DATA_MOUNTS, nominal_memory_step_gb: int = 8) -> str:
    """
    Levantamento de capacidade e licenciamento Cloudera de um cluster: cores, memória, uso
    (pico de CPU e faixa de memória nos últimos `days` dias), discos de dados por host e as
    métricas da página Cloudera Licensed Metrics:
      - CCU por Node = cores ÷ 6 + memória GB ÷ 12, sobre o recurso disponível, em duas bases:
        RAM vista pelo SO e RAM nominal (arredondada para cima em múltiplos de
        `nominal_memory_step_gb`); traz também o total arredondando cada Node para cima;
      - Data Under Management (TB lógicos = HDFS usado ÷ replicação + Kudu ÷ 3; e brutos);
      - Capacity Under Management (capacidade do HDFS e soma de todos os volumes de dados,
        identificados por `data_mount_regex`).
    Inclui achados (HDFS cheio, /var cheio, volumes desbalanceados, hosts ociosos, discos
    vazios fora dos DataNodes) e pontos a validar no Order Form, além dos hosts do mesmo CM
    que estão fora do cluster. TB = 1.024 GB.
    output: "json" (relatório completo), "html" (grava a página e devolve o caminho e um
    resumo) ou "pdf" (grava HTML e PDF; usa Edge/Chrome headless, ou CM_PDF_BROWSER).
    output_dir: pasta dos arquivos (padrão CM_REPORT_DIR ou ~/cm-reports).
    """
    fmt = output.lower()
    if fmt not in ("json", "html", "pdf"):
        return "output deve ser json, html ou pdf."
    r = build_license_report(cluster, days, data_mount_regex, nominal_memory_step_gb)
    if fmt == "json":
        return _out(r)
    return _out({"files": write_report(r, fmt, output_dir), "summary": _summary(r)})
