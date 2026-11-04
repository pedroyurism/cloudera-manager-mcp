"""Núcleo compartilhado: configuração, instância FastMCP, cliente do CM e helpers."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import httpx
from fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

CM_HOST = os.environ.get("CM_HOST", "")                  # ex.: cm01.cliente.local
CM_PORT = os.environ.get("CM_PORT", "")                  # padrão: 7183 (TLS) / 7180
CM_USE_TLS = os.environ.get("CM_USE_TLS", "true").lower() == "true"
CM_USERNAME = os.environ.get("CM_USERNAME", "")
CM_PASSWORD = os.environ.get("CM_PASSWORD", "")
CM_API_VERSION = os.environ.get("CM_API_VERSION", "")    # vazio = autodetectar
CM_VERIFY_SSL = os.environ.get("CM_VERIFY_SSL", "true")  # true | false | caminho p/ CA bundle
CM_TIMEOUT = float(os.environ.get("CM_TIMEOUT", "30"))
MAX_CHARS = int(os.environ.get("CM_MCP_MAX_CHARS", "60000"))

mcp = FastMCP(
    "cloudera-manager",
    instructions=(
        "Ferramentas somente leitura para o Cloudera Manager. "
        "Comece por cm_info e list_clusters para descobrir nomes de clusters e serviços; "
        "os demais tools usam o nome interno (ex.: 'hdfs', 'yarn', 'impala'), não o display name."
    ),
)


class CMClient:
    def __init__(self) -> None:
        if not CM_HOST:
            raise RuntimeError("CM_HOST não configurado.")
        if not (CM_USERNAME and CM_PASSWORD):
            raise RuntimeError("CM_USERNAME/CM_PASSWORD não configurados.")

        scheme = "https" if CM_USE_TLS else "http"
        port = CM_PORT or ("7183" if CM_USE_TLS else "7180")
        self.base = f"{scheme}://{CM_HOST}:{port}/api"

        verify: bool | str
        if CM_VERIFY_SSL.lower() in ("true", "false"):
            verify = CM_VERIFY_SSL.lower() == "true"
        else:
            verify = CM_VERIFY_SSL  # caminho para CA bundle

        self.http = httpx.Client(
            auth=(CM_USERNAME, CM_PASSWORD),
            verify=verify,
            timeout=CM_TIMEOUT,
            headers={"Accept": "application/json"},
        )
        self._version = CM_API_VERSION or None

    @property
    def version(self) -> str:
        if not self._version:
            r = self.http.get(f"{self.base}/version")
            r.raise_for_status()
            self._version = r.text.strip()
        return self._version

    def get(self, path: str, params: dict[str, Any] | None = None, raw: bool = False) -> Any:
        url = f"{self.base}/{self.version}/{path.lstrip('/')}"
        params = {k: v for k, v in (params or {}).items() if v is not None}
        r = self.http.get(url, params=params)
        if r.status_code >= 400:
            raise RuntimeError(f"CM API {r.status_code} em GET {path}: {r.text[:500]}")
        return r.text if raw else r.json()


_client: CMClient | None = None


def cm() -> CMClient:
    global _client
    if _client is None:
        _client = CMClient()
    return _client


def _out(data: Any) -> str:
    """Serializa e trunca para não estourar o contexto do modelo."""
    text = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False, indent=1)
    if len(text) > MAX_CHARS:
        return text[:MAX_CHARS] + f"\n... [truncado: {len(text)} caracteres no total]"
    return text


def _items(resp: Any) -> list[dict]:
    return resp.get("items", []) if isinstance(resp, dict) else resp


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _health(obj: dict) -> dict:
    return {
        "healthSummary": obj.get("healthSummary"),
        "unhealthyChecks": [
            {"name": h.get("name"), "summary": h.get("summary"), "explanation": h.get("explanation")}
            for h in obj.get("healthChecks", []) or []
            if h.get("summary") not in ("GOOD", "DISABLED", "NOT_AVAILABLE")
        ],
    }




# ---------------------------------------------------------------------------
# Cliente do YARN ResourceManager (REST /ws/v1) – opcional
# ---------------------------------------------------------------------------

YARN_RM_URL = os.environ.get("YARN_RM_URL", "")            # ex.: https://rm01:8090 ou URL do Knox
YARN_RM_AUTH = os.environ.get("YARN_RM_AUTH", "none").lower()  # none | basic | kerberos
YARN_RM_USERNAME = os.environ.get("YARN_RM_USERNAME", "")
YARN_RM_PASSWORD = os.environ.get("YARN_RM_PASSWORD", "")
YARN_RM_TLS = os.environ.get("YARN_RM_TLS", "true" if CM_USE_TLS else "false").lower() == "true"
YARN_RM_PORT = os.environ.get("YARN_RM_PORT", "")
YARN_RM_VERIFY_SSL = os.environ.get("YARN_RM_VERIFY_SSL", CM_VERIFY_SSL)


def _verify(value: str) -> bool | str:
    return value.lower() == "true" if value.lower() in ("true", "false") else value


def yarn_service_name(cluster: str, service: str | None = None) -> str:
    """Retorna o nome do serviço YARN do cluster (autodetecta pelo tipo se não informado)."""
    if service:
        return service
    for s in _items(cm().get(f"clusters/{cluster}/services")):
        if s.get("type") == "YARN":
            return s["name"]
    raise RuntimeError(f"Nenhum serviço YARN encontrado no cluster {cluster}.")


class RMClient:
    def __init__(self, base: str) -> None:
        auth: Any = None
        if YARN_RM_AUTH == "basic":
            auth = (YARN_RM_USERNAME, YARN_RM_PASSWORD)
        elif YARN_RM_AUTH == "kerberos":
            try:
                from httpx_gssapi import OPTIONAL, HTTPSPNEGOAuth
            except ImportError as e:  # pragma: no cover
                raise RuntimeError(
                    "YARN_RM_AUTH=kerberos requer o extra: pip install -e '.[kerberos]' "
                    "e um ticket válido (kinit)."
                ) from e
            auth = HTTPSPNEGOAuth(mutual_authentication=OPTIONAL)
        self.base = base.rstrip("/")
        self.http = httpx.Client(
            auth=auth,
            verify=_verify(YARN_RM_VERIFY_SSL),
            timeout=CM_TIMEOUT,
            follow_redirects=True,  # RM standby redireciona para o ativo
            headers={"Accept": "application/json"},
        )

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.base}/ws/v1/{path.lstrip('/')}"
        params = {k: v for k, v in (params or {}).items() if v is not None}
        r = self.http.get(url, params=params)
        if r.status_code == 401:
            raise RuntimeError(
                "ResourceManager retornou 401. Se o cluster usa Kerberos/SPNEGO, configure "
                "YARN_RM_AUTH=kerberos (com kinit) ou aponte YARN_RM_URL para o Knox com YARN_RM_AUTH=basic."
            )
        if r.status_code >= 400:
            raise RuntimeError(f"RM API {r.status_code} em GET {path}: {r.text[:500]}")
        return r.json()


_rm_clients: dict[str, RMClient] = {}


def _discover_rm_url(cluster: str, service: str) -> str:
    """Descobre o ResourceManager ativo pelo CM (host + porta web)."""
    c = cm()
    roles = [
        r for r in _items(c.get(f"clusters/{cluster}/services/{service}/roles", {"view": "full"}))
        if r.get("type") == "RESOURCEMANAGER"
    ]
    if not roles:
        raise RuntimeError(f"Nenhuma role RESOURCEMANAGER em {cluster}/{service}.")
    roles.sort(key=lambda r: 0 if r.get("haStatus") == "ACTIVE" else 1)
    role = roles[0]
    href = role.get("hostRef") or {}
    hostname = href.get("hostname")
    if not hostname and href.get("hostId"):
        hostname = c.get(f"hosts/{href['hostId']}").get("hostname")
    port = YARN_RM_PORT
    if not port:
        key = "resourcemanager_webserver_https_port" if YARN_RM_TLS else "resourcemanager_webserver_port"
        try:
            cfg = _items(c.get(
                f"clusters/{cluster}/services/{service}/roles/{role['name']}/config", {"view": "full"}))
            item = next((i for i in cfg if i.get("name") == key), None)
            if item:
                port = item.get("value") or item.get("default")
        except Exception:  # noqa: BLE001
            port = ""
    port = port or ("8090" if YARN_RM_TLS else "8088")
    return f"{'https' if YARN_RM_TLS else 'http'}://{hostname}:{port}"


def rm(cluster: str, service: str | None = None) -> RMClient:
    """Cliente do RM: usa YARN_RM_URL se configurado, senão descobre o RM ativo pelo CM."""
    if YARN_RM_URL:
        key = YARN_RM_URL
    else:
        key = _discover_rm_url(cluster, yarn_service_name(cluster, service))
    if key not in _rm_clients:
        _rm_clients[key] = RMClient(key)
    return _rm_clients[key]
