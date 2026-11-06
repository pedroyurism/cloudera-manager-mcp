"""Servidores HTTP falsos do Cloudera Manager e do YARN ResourceManager para os testes."""

from __future__ import annotations

import json
import re
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from datetime import datetime, timedelta, timezone

from tests import mock_impala

RM_PORT = 18088
CM_PORT = 17180
V = "/api/v51"
HOST = {"hostId": "h1", "hostname": "node01.local"}

# ---------------------------------------------------------------- aplicações YARN
def _app(i: int, user: str, pool: str, running: bool, mem_gbh: float, state: str = "FINISHED") -> dict:
    return {
        "applicationId": f"application_1700000000000_{i:04d}",
        "name": f"job-{user}-{i}",
        "user": user,
        "pool": pool,
        "state": "RUNNING" if running else state,
        "startTime": "2026-11-04T10:00:00.000Z",
        "endTime": None if running else "2026-11-04T11:30:00.000Z",
        "progress": 50.0 if running else None,
        # como no CM real: apps terminadas vêm com -1 ("não se aplica")
        "allocatedMB": 8192 if running else -1,
        "allocatedVCores": 4 if running else -1,
        "runningContainers": 4 if running else -1,
        "allocatedMemorySeconds": mem_gbh * 1024 * 3600,
        "allocatedVcoreSeconds": mem_gbh * 3600 / 2,
        "attributes": {"application_type": "SPARK"},
    }


APPS = (
    [_app(i, "etl_user", "root.etl", True, 100) for i in range(3)]                      # 3 rodando
    + [_app(100 + i, "etl_user", "root.etl", False, 50) for i in range(120)]           # 120 terminados (força paginação)
    + [_app(300 + i, "ana", "root.analytics", False, 10) for i in range(5)]
    + [_app(400, "ana", "root.analytics", False, 1, state="FAILED")]
    + [_app(500, "bob", "root.default", True, 5)]
)

SEEN: list[str] = []

# ---------------------------------------------------------------- relatório de saúde
_NOW = datetime.now(timezone.utc)


def _hb(seconds_ago: float) -> str:
    return (_NOW - timedelta(seconds=seconds_ago)).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _chk(**checks: str) -> list[dict]:
    return [{"name": k, "summary": v, "suppressed": False} for k, v in checks.items()]


GOOD_HOST_CHECKS = _chk(HOST_CLOCK_OFFSET="GOOD", HOST_MEMORY_SWAPPING="GOOD",
                        HOST_AGENT_LOG_DIRECTORY_FREE_SPACE="GOOD", HOST_AGENT_CERTIFICATE_EXPIRY="DISABLED")
CLUSTER_HOSTS = [
    {**HOST, "healthSummary": "GOOD", "lastHeartbeat": _hb(10), "healthChecks": GOOD_HOST_CHECKS},
    {"hostId": "h2", "hostname": "node02.local", "healthSummary": "CONCERNING", "lastHeartbeat": _hb(900),
     "healthChecks": _chk(HOST_CLOCK_OFFSET="CONCERNING", HOST_MEMORY_SWAPPING="GOOD",
                          HOST_AGENT_LOG_DIRECTORY_FREE_SPACE="GOOD")},
    {"hostId": "h3", "hostname": "node03.local", "healthSummary": "BAD", "lastHeartbeat": _hb(5000),
     "maintenanceMode": True, "healthChecks": _chk(HOST_MEMORY_SWAPPING="BAD")},
    {"hostId": "h4", "hostname": "node04.local", "healthSummary": "GOOD", "healthChecks": GOOD_HOST_CHECKS},
]


def _role(name: str, rtype: str, host: str, state: str = "STARTED", health: str = "GOOD", **extra) -> dict:
    return {"name": name, "type": rtype, "hostRef": {"hostId": host, "hostname": f"{host}.local"},
            "roleState": state, "healthSummary": health, "healthChecks": [],
            "configStalenessStatus": "FRESH", "maintenanceMode": False, **extra}


MGMT_ROLES = {"items": [
    _role("mgmt-SM", "SERVICEMONITOR", "h1", state="STOPPED", health="BAD"),
    _role("mgmt-HM", "HOSTMONITOR", "h1"),
    _role("mgmt-ES", "EVENTSERVER", "h1", health="CONCERNING",
          healthChecks=_chk(EVENT_SERVER_EVENT_STORE_SIZE="CONCERNING")),
]}


def _alert(i: int, sev: str, svc: str) -> dict:
    return {"id": f"ev{i}", "timeOccurred": _hb(60 * i), "severity": sev, "category": "HEALTH_EVENT",
            "alert": True, "content": f"Health test changes {i}",
            "attributes": [{"name": "SERVICE", "values": [svc]},
                           {"name": "ALERT_SUMMARY", "values": [f"The health of service {svc} has become bad."]}]}


# 130 alertas: força a paginação de 100 em 100
ALERTS = ([_alert(i, "CRITICAL", "hdfs") for i in range(1, 121)]
          + [_alert(200 + i, "IMPORTANT", "yarn") for i in range(5)]
          + [_alert(300 + i, "CRITICAL", "zookeeper") for i in range(5)])


def _filter_apps(flt: str) -> list[dict]:
    apps = APPS
    if "executing = true" in flt:
        apps = [a for a in apps if a["endTime"] is None]
    if "executing = false" in flt:
        apps = [a for a in apps if a["endTime"] is not None]
    if m := re.search(r"user = (\S+)", flt):
        apps = [a for a in apps if a["user"] == m.group(1)]
    if m := re.search(r"pool = (\S+)", flt):
        apps = [a for a in apps if a["pool"] == m.group(1)]
    return apps


def _ts(name: str, metric: str, values: list[float]) -> dict:
    return {"metadata": {"metricName": metric, "entityName": f"yarn:{name}",
                         "attributes": {"poolName": name}},
            "data": [{"timestamp": f"t{i}", "value": v} for i, v in enumerate(values)]}


CM_STATIC = {
    "/api/version": "v51",
    f"{V}/cm/version": {"version": "7.13.1"},
    f"{V}/cm/service": {"name": "mgmt", "serviceState": "STARTED", "healthSummary": "GOOD", "healthChecks": []},
    f"{V}/clusters": {"items": [{"name": "Cluster1", "displayName": "Prod", "fullVersion": "7.1.9"}]},
    f"{V}/clusters/Cluster1/services": {"items": [
        {"name": "hdfs", "type": "HDFS", "serviceState": "STARTED", "healthSummary": "GOOD", "healthChecks": [],
         "configStalenessStatus": "STALE", "clientConfigStalenessStatus": "FRESH"},
        {"name": "impala", "type": "IMPALA", "serviceState": "STARTED", "healthSummary": "GOOD", "healthChecks": [],
         "configStalenessStatus": "FRESH", "clientConfigStalenessStatus": "STALE"},
        {"name": "yarn", "type": "YARN", "serviceState": "STARTED", "healthSummary": "BAD",
         "healthChecks": [{"name": "YARN_RESOURCEMANAGERS_HEALTH", "summary": "BAD", "explanation": "RM down"}]},
        {"name": "oozie", "type": "OOZIE", "serviceState": "STARTED", "healthSummary": "BAD", "healthChecks": [],
         "maintenanceMode": True},
    ]},
    f"{V}/clusters/Cluster1/services/yarn": {"name": "yarn", "type": "YARN", "serviceState": "STARTED", "healthSummary": "BAD"},
    f"{V}/clusters/Cluster1/services/yarn/roles": {"items": [
        {"name": "yarn-RM-standby", "type": "RESOURCEMANAGER", "hostRef": {"hostId": "h2"}, "haStatus": "STANDBY",
         "roleState": "STARTED", "healthSummary": "GOOD", "healthChecks": []},
        {"name": "yarn-RM-1", "type": "RESOURCEMANAGER", "hostRef": {"hostId": "h1"}, "haStatus": "ACTIVE",
         "roleState": "STOPPED", "healthSummary": "BAD", "healthChecks": [{"name": "RM_SCM", "summary": "BAD"}]},
        {"name": "yarn-NM-1", "type": "NODEMANAGER", "hostRef": HOST, "roleState": "STARTED", "healthSummary": "GOOD", "healthChecks": []},
    ]},
    f"{V}/clusters/Cluster1/services/yarn/roles/yarn-RM-1/config": {"items": [
        {"name": "resourcemanager_webserver_port", "default": "8088", "value": str(RM_PORT)}]},
    f"{V}/clusters/Cluster1/services/yarn/config": {"items": [{"name": "yarn_service_env_safety_valve", "value": "X=1"}]},
    f"{V}/clusters/Cluster1/services/yarn/roleConfigGroups": {"items": [{"name": "yarn-RESOURCEMANAGER-BASE", "roleType": "RESOURCEMANAGER", "base": True}]},
    f"{V}/clusters/Cluster1/services/yarn/roleConfigGroups/yarn-RESOURCEMANAGER-BASE/config": {"items": [{"name": "resource_manager_java_heapsize", "value": "4294967296"}]},
    f"{V}/clusters/Cluster1/services/yarn/roles/yarn-RM-1/logs/stderr": "\n".join(f"linha {i}" for i in range(500)),
    f"{V}/clusters/Cluster1/services/hdfs/roles": {"items": [
        _role("hdfs-NN-1", "NAMENODE", "h1", haStatus="ACTIVE"),
        _role("hdfs-NN-2", "NAMENODE", "h2", haStatus="STANDBY"),
        _role("hdfs-DN-1", "DATANODE", "h1", configStalenessStatus="STALE"),
        _role("hdfs-DN-2", "DATANODE", "h2", state="STOPPED", health="BAD",
              healthChecks=_chk(DATA_NODE_HOST_HEALTH="BAD")),
        _role("hdfs-GW-1", "GATEWAY", "h1", state="NA", health="GOOD"),
    ]},
    f"{V}/clusters/Cluster1/services/impala/roles": {"items": [
        _role("impala-IMPALAD-1", "IMPALAD", "h1", health="CONCERNING",
              healthChecks=_chk(IMPALAD_QUERY_MONITORING_STATUS="CONCERNING")),
        _role("impala-CATALOG-1", "CATALOGSERVER", "h2", configStalenessStatus="STALE_REFRESHABLE"),
    ]},
    f"{V}/clusters/Cluster1/services/oozie/roles": {"items": [
        _role("oozie-SERVER-1", "OOZIE_SERVER", "h3", state="STOPPED", health="BAD")]},
    f"{V}/clusters/Cluster1/hosts": {"items": CLUSTER_HOSTS},
    f"{V}/cm/service/roles": MGMT_ROLES,
    f"{V}/hosts": {"items": [{**HOST, "ipAddress": "10.0.0.1", "numCores": 16, "totalPhysMemBytes": 68719476736,
                               "healthSummary": "GOOD", "roleRefs": [{}]}]},
    f"{V}/hosts/h1": {"hostId": "h1", "hostname": "127.0.0.1", "ipAddress": "10.0.0.1",
                      "roleRefs": [{"serviceName": "yarn", "roleName": "yarn-RM-1"}]},
    f"{V}/events": {"items": [{"timeOccurred": "t", "severity": "CRITICAL", "category": "HEALTH_EVENT",
                               "content": "RM ruim", "attributes": [{"name": "SERVICE", "values": ["yarn"]}]}]},
    f"{V}/cm/commands": {"items": []},
    f"{V}/clusters/Cluster1/commands": {"items": [{"id": 7, "name": "Restart", "active": True}]},
    f"{V}/commands/7": {"id": 7, "name": "Restart", "active": False, "success": False,
                        "children": {"items": [{"id": 8, "name": "Start", "success": False,
                                                "roleRef": {"roleName": "yarn-RM-1"}}]}},
    f"{V}/clusters/Cluster1/parcels": {"items": [{"product": "CDH", "version": "7.1.9", "stage": "ACTIVATED"},
                                                   {"product": "X", "version": "1", "stage": "AVAILABLE_REMOTELY"}]},
    f"{V}/audits": {"items": [{"timestamp": "t", "username": "admin", "command": "RESTART", "allowed": True}]},
    f"{V}/clusters/Cluster1/services/yarn/yarnApplications/attributes": {"items": [
        {"name": "user", "type": "STRING", "displayName": "User", "description": "Usuário"},
        {"name": "application_duration", "type": "MILLISECONDS", "displayName": "Duration"}]},
    f"{V}/clusters/Cluster1/services/yarn/yarnUtilization": {
        "avgCpuUtilization": 120.5, "maxCpuUtilization": 300.0, "avgMemoryUtilization": 400000.0,
        "tenantUtilizations": {"items": [
            {"tenantName": "root.analytics", "avgYarnMemoryAllocation": 1000.0, "avgYarnContainerWaitRatio": 0.4},
            {"tenantName": "root.etl", "avgYarnMemoryAllocation": 90000.123, "avgYarnContainerWaitRatio": 0.05}]}},
}

RM_STATIC = {
    "/ws/v1/cluster/metrics": {"clusterMetrics": {
        "appsRunning": 4, "appsPending": 2, "allocatedMB": 32768, "availableMB": 32768, "totalMB": 65536,
        "reservedMB": 0, "allocatedVirtualCores": 16, "availableVirtualCores": 48, "totalVirtualCores": 64,
        "containersAllocated": 16, "containersPending": 5, "activeNodes": 3, "unhealthyNodes": 1, "lostNodes": 0}},
    "/ws/v1/cluster/scheduler": {"scheduler": {"schedulerInfo": {
        "type": "capacityScheduler", "queueName": "root", "capacity": 100.0, "usedCapacity": 50.0,
        "queues": {"queue": [
            {"queuePath": "root.etl", "queueName": "etl", "state": "RUNNING", "capacity": 60.0,
             "absoluteCapacity": 60.0, "absoluteMaxCapacity": 100.0, "usedCapacity": 80.0,
             "absoluteUsedCapacity": 48.0, "numApplications": 3, "numActiveApplications": 3,
             "numPendingApplications": 0, "resourcesUsed": {"memory": 24576, "vCores": 12},
             "users": {"user": [
                 {"username": "etl_user", "resourcesUsed": {"memory": 24576, "vCores": 12},
                  "numActiveApplications": 3, "numPendingApplications": 0}]}},
            {"queuePath": "root.default", "queueName": "default", "state": "RUNNING", "capacity": 40.0,
             "absoluteCapacity": 40.0, "usedCapacity": 30.0, "absoluteUsedCapacity": 12.0,
             "numApplications": 1, "resourcesUsed": {"memory": 8192, "vCores": 4}, "users": None},
        ]}}}},
    "/ws/v1/cluster/apps/application_1700000000000_0400": {"app": {
        "id": "application_1700000000000_0400", "user": "ana", "queue": "root.analytics", "state": "FINISHED",
        "finalStatus": "FAILED", "diagnostics": "Container killed: OOM", "elapsedTime": 1000,
        "allocatedMB": -1, "allocatedVCores": -1, "runningContainers": -1}},
}


class _Base(BaseHTTPRequestHandler):
    def log_message(self, *a):  # silencioso
        pass

    def _send(self, code: int, body) -> None:
        out = body if isinstance(body, str) else json.dumps(body)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(out.encode())


class CMHandler(_Base):
    def do_GET(self):  # noqa: N802
        u = urlparse(self.path)
        q = parse_qs(u.query)
        SEEN.append(self.path)
        if not self.headers.get("Authorization", "").startswith("Basic "):
            return self._send(401, {"message": "auth"})
        if u.path == f"{V}/clusters/Cluster1/services/yarn/yarnApplications":
            apps = _filter_apps(q.get("filter", [""])[0])
            off, lim = int(q.get("offset", ["0"])[0]), int(q.get("limit", ["100"])[0])
            return self._send(200, {"applications": apps[off:off + lim], "warnings": []})
        if u.path == f"{V}/timeseries":
            query = q.get("query", [""])[0]
            if "fair_share" in query:   # simula Capacity Scheduler: métricas de fair share inexistentes
                return self._send(400, {"message": "Unknown metric fair_share_mb"})
            if "YARN_POOL" in query:
                return self._send(200, {"items": [{"timeSeries": [
                    _ts("root.etl", "allocated_memory_mb", [10240, 40960, 24576]),
                    _ts("root.etl", "pending_containers", [0, 12, 3]),
                    _ts("root.etl", "apps_running", [3]),
                    _ts("root.default", "allocated_memory_mb", [8192]),
                    _ts("root.default", "apps_pending", [2]),
                ]}]})
            return self._send(200, {"items": [{"timeSeries": [
                {"metadata": {"metricName": "cpu_percent", "entityName": "node01"},
                 "data": [{"timestamp": "t1", "value": 10}, {"timestamp": "t2", "value": 42}]}]}]})
        if u.path == f"{V}/events" and "alert==true" in q.get("query", [""])[0]:
            off, lim = int(q.get("resultOffset", ["0"])[0]), int(q.get("maxResults", ["100"])[0])
            return self._send(200, {"totalResults": len(ALERTS), "items": ALERTS[off:off + lim]})
        imp = f"{V}/clusters/Cluster1/services/impala/impalaQueries"
        if u.path == imp:
            qs = mock_impala.filter_queries(q.get("filter", [""])[0])
            off, lim = int(q.get("offset", ["0"])[0]), int(q.get("limit", ["100"])[0])
            return self._send(200, {"queries": qs[off:off + lim],
                                    "warnings": ["Impala query scan limit reached."] if off == 0 else []})
        if u.path.startswith(imp + "/"):
            qid = unquote(u.path[len(imp) + 1:])
            if qid not in mock_impala.PROFILES:
                return self._send(404, {"message": "query not found"})
            return self._send(200, {"details": mock_impala.PROFILES[qid]})
        body = CM_STATIC.get(u.path)
        if body is None:
            return self._send(404, {"message": "nope"})
        # como no CM real: logs só saem em text/plain; Accept só JSON dá 406
        if "/logs/" in u.path and self.headers.get("Accept") == "application/json":
            return self._send(406, "")
        return self._send(200, body)


class RMHandler(_Base):
    def do_GET(self):  # noqa: N802
        body = RM_STATIC.get(urlparse(self.path).path)
        return self._send(200, body) if body else self._send(404, {"RemoteException": {"message": "not found"}})


def start() -> None:
    for port, handler in ((CM_PORT, CMHandler), (RM_PORT, RMHandler)):
        srv = HTTPServer(("127.0.0.1", port), handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
