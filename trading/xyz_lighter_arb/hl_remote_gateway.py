#!/usr/bin/env python3
"""
Minimal remote execution gateway for Tokyo node.
Current behavior: accept /exec and return ACK (dry-run relay test).
"""

import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG_DIR = "logs"
EVENT_LOG = os.path.join(LOG_DIR, "hl_gateway_events.jsonl")
os.makedirs(LOG_DIR, exist_ok=True)


def _append_event(event: dict):
    event = dict(event)
    event["ts"] = time.time()
    with open(EVENT_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


class Handler(BaseHTTPRequestHandler):
    server_version = "HLRemoteGateway/0.1"

    def do_GET(self):
        if self.path == "/health":
            msg = f"[HL-GW] health check from {self.client_address[0]}:{self.client_address[1]}"
            print(msg)
            _append_event(
                {
                    "event": "health",
                    "client_ip": self.client_address[0],
                    "client_port": self.client_address[1],
                    "path": self.path,
                }
            )
            self._send(200, {"ok": True, "ts": time.time()})
            return
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        if self.path != "/exec":
            self._send(404, {"ok": False, "error": "not found"})
            return

        n = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(n) if n > 0 else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            self._send(400, {"ok": False, "error": "invalid json"})
            return

        print(
            f"[HL-GW] exec from {self.client_address[0]}:{self.client_address[1]} "
            f"signal_id={payload.get('signal_id')} signal={payload.get('signal')} lots={payload.get('lots')}"
        )
        _append_event(
            {
                "event": "exec",
                "client_ip": self.client_address[0],
                "client_port": self.client_address[1],
                "path": self.path,
                "signal_id": payload.get("signal_id"),
                "signal": payload.get("signal"),
                "lots": payload.get("lots"),
                "symbol": payload.get("symbol"),
                "spread_pct": payload.get("spread_pct"),
                "zscore": payload.get("zscore"),
            }
        )

        # Dry-run only ACK.
        resp = {
            "ok": True,
            "mode": "dry_run_ack",
            "received_at": time.time(),
            "signal_id": payload.get("signal_id"),
            "signal": payload.get("signal"),
            "lots": payload.get("lots"),
            "symbol": payload.get("symbol"),
        }
        self._send(200, resp)

    def log_message(self, fmt, *args):
        # Keep gateway logs clean; use default stdout only for criticals.
        return

    def _send(self, code: int, obj: dict):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main():
    host = "0.0.0.0"
    port = 18080
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"HL remote gateway listening on {host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
