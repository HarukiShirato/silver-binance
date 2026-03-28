import json
import time
import urllib.error
import urllib.request
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

    def send_dry_run(self, payload: Dict[str, Any]) -> RemoteExecResult:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            url=f"{self._base}/exec",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        t0 = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                latency = (time.monotonic() - t0) * 1000
                status = int(getattr(resp, "status", 200))
                text = resp.read().decode("utf-8", errors="ignore")
                detail = text[:300]
                return RemoteExecResult(ok=(200 <= status < 300), status_code=status, latency_ms=latency, detail=detail)
        except urllib.error.HTTPError as e:
            latency = (time.monotonic() - t0) * 1000
            return RemoteExecResult(ok=False, status_code=int(e.code), latency_ms=latency, detail=str(e))
        except Exception as e:
            latency = (time.monotonic() - t0) * 1000
            return RemoteExecResult(ok=False, status_code=0, latency_ms=latency, detail=str(e))

    def health_check(self) -> RemoteExecResult:
        req = urllib.request.Request(
            url=f"{self._base}/health",
            method="GET",
        )
        t0 = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                latency = (time.monotonic() - t0) * 1000
                status = int(getattr(resp, "status", 200))
                text = resp.read().decode("utf-8", errors="ignore")
                detail = text[:300]
                return RemoteExecResult(ok=(200 <= status < 300), status_code=status, latency_ms=latency, detail=detail)
        except urllib.error.HTTPError as e:
            latency = (time.monotonic() - t0) * 1000
            return RemoteExecResult(ok=False, status_code=int(e.code), latency_ms=latency, detail=str(e))
        except Exception as e:
            latency = (time.monotonic() - t0) * 1000
            return RemoteExecResult(ok=False, status_code=0, latency_ms=latency, detail=str(e))
