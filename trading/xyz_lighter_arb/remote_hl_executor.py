import json
import time
import http.client
from urllib.parse import urlparse
from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class RemoteExecResult:
    ok: bool
    status_code: int
    latency_ms: float
    detail: str = ""


class RemoteHLExecutorClient:
    """Call remote HL execution gateway (Tokyo) from Shanghai node."""

    def __init__(self, base_url: str, timeout_sec: float = 2.0):
        self._base = base_url.rstrip("/")
        self._timeout = timeout_sec
        parsed = urlparse(self._base)
        self._scheme = (parsed.scheme or "http").lower()
        self._host = parsed.hostname or ""
        self._port = parsed.port or (443 if self._scheme == "https" else 80)
        self._base_path = parsed.path.rstrip("/")
        self._conn = None

    def _ensure_conn(self):
        if self._conn is not None:
            return
        if self._scheme == "https":
            self._conn = http.client.HTTPSConnection(
                self._host,
                self._port,
                timeout=self._timeout,
            )
        else:
            self._conn = http.client.HTTPConnection(
                self._host,
                self._port,
                timeout=self._timeout,
            )

    def _close_conn(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _request(self, method: str, path: str, body: bytes = b"", headers: Dict[str, str] = None) -> RemoteExecResult:
        if headers is None:
            headers = {}
        full_path = f"{self._base_path}{path}" if self._base_path else path
        t0 = time.monotonic()
        try:
            self._ensure_conn()
            req_headers = {"Connection": "keep-alive", **headers}
            self._conn.request(method, full_path, body=body, headers=req_headers)
            resp = self._conn.getresponse()
            payload = resp.read().decode("utf-8", errors="ignore")
            latency = (time.monotonic() - t0) * 1000
            status = int(getattr(resp, "status", 0) or 0)
            return RemoteExecResult(
                ok=(200 <= status < 300),
                status_code=status,
                latency_ms=latency,
                detail=payload[:300],
            )
        except Exception as e:
            latency = (time.monotonic() - t0) * 1000
            self._close_conn()
            return RemoteExecResult(ok=False, status_code=0, latency_ms=latency, detail=str(e))

    def send_dry_run(self, payload: Dict[str, Any]) -> RemoteExecResult:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        return self._request(
            "POST",
            "/exec",
            body=body,
            headers={"Content-Type": "application/json"},
        )

    def health_check(self) -> RemoteExecResult:
        return self._request("GET", "/health")

    def close(self):
        self._close_conn()
