#!/usr/bin/env python3
"""Remote execution + quote gateway for Tokyo node."""

import json
import os
import time
import asyncio
import threading
import urllib.parse
import urllib.request
from typing import Optional, Dict, Any
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import websockets

LOG_DIR = "logs"
EVENT_LOG = os.path.join(LOG_DIR, "hl_gateway_events.jsonl")
os.makedirs(LOG_DIR, exist_ok=True)

HL_API_URL = os.environ.get("HL_API_URL", "https://api.hyperliquid.xyz").strip()
HL_DEX = os.environ.get("HL_DEX", "xyz").strip()
DEFAULT_SYMBOL = os.environ.get("HL_QUOTE_SYMBOL", "SILVER").strip() or "SILVER"
QUOTE_POLL_SEC = max(0.2, float(os.environ.get("HL_QUOTE_POLL_SEC", "0.5")))
QUOTE_STALE_SEC = max(1.0, float(os.environ.get("HL_QUOTE_STALE_SEC", "5")))
QUOTE_WS_HOST = os.environ.get("HL_QUOTE_WS_HOST", "0.0.0.0").strip() or "0.0.0.0"
QUOTE_WS_PORT = int(os.environ.get("HL_QUOTE_WS_PORT", "18081"))
QUOTE_WS_PUSH_SEC = max(0.05, float(os.environ.get("HL_QUOTE_WS_PUSH_SEC", "0.2")))
SELF_HEAL_RESTART_SEC = max(30.0, float(os.environ.get("HL_SELF_HEAL_RESTART_SEC", "180")))

_quote_lock = threading.Lock()
_quote_state: Dict[str, Any] = {
    "ok": False,
    "symbol": f"{HL_DEX}:{DEFAULT_SYMBOL}" if HL_DEX and ":" not in DEFAULT_SYMBOL else DEFAULT_SYMBOL,
    "price": 0.0,
    "ts": 0.0,
    "source": "hyperliquid_allMids",
    "error": "",
}
_last_quote_req_log_ts = 0.0


def _append_event(event: dict):
    event = dict(event)
    event["ts"] = time.time()
    with open(EVENT_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip() or DEFAULT_SYMBOL
    if HL_DEX and ":" not in s:
        s = f"{HL_DEX}:{s}"
    return s


def _fetch_all_mids() -> Dict[str, Any]:
    payload = {"type": "allMids"}
    if HL_DEX:
        payload["dex"] = HL_DEX

    req = urllib.request.Request(
        url=f"{HL_API_URL.rstrip('/')}/info",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=3) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _quote_updater():
    symbol = _normalize_symbol(DEFAULT_SYMBOL)
    fail_since: Optional[float] = None
    while True:
        try:
            mids = _fetch_all_mids()
            price = float(mids.get(symbol, 0) or 0)
            if price <= 0:
                raise RuntimeError(f"symbol not found in allMids: {symbol}")

            now = time.time()
            with _quote_lock:
                _quote_state.update(
                    {
                        "ok": True,
                        "symbol": symbol,
                        "price": price,
                        "ts": now,
                        "source": "hyperliquid_allMids",
                        "error": "",
                    }
                )
            fail_since = None
        except Exception as e:
            now = time.time()
            if fail_since is None:
                fail_since = now
            bad_for = now - fail_since
            with _quote_lock:
                _quote_state["ok"] = False
                _quote_state["error"] = str(e)
            _append_event({"event": "quote_update_error", "error": str(e), "bad_for_sec": round(bad_for, 3)})
            if bad_for >= SELF_HEAL_RESTART_SEC:
                msg = (
                    f"[HL-GW] self-heal restart: quote updater unhealthy for {bad_for:.1f}s "
                    f"(threshold={SELF_HEAL_RESTART_SEC:.1f}s), last_error={e}"
                )
                print(msg)
                _append_event(
                    {
                        "event": "self_heal_restart",
                        "reason": "quote_updater_unhealthy",
                        "bad_for_sec": round(bad_for, 3),
                        "threshold_sec": SELF_HEAL_RESTART_SEC,
                        "last_error": str(e),
                    }
                )
                os._exit(17)
        time.sleep(QUOTE_POLL_SEC)


def _get_quote(symbol: str) -> Dict[str, Any]:
    requested_symbol = _normalize_symbol(symbol)
    with _quote_lock:
        state = dict(_quote_state)

    # Current updater tracks one symbol only.
    if requested_symbol != state.get("symbol"):
        return {
            "ok": False,
            "error": f"unsupported symbol: {requested_symbol}, only {state.get('symbol')} is available",
            "symbol": requested_symbol,
            "ts": time.time(),
        }

    now = time.time()
    ts = float(state.get("ts", 0) or 0)
    age = now - ts if ts > 0 else 1e9
    stale = age > QUOTE_STALE_SEC

    return {
        "ok": bool(state.get("ok", False)),
        "symbol": state.get("symbol"),
        "price": float(state.get("price", 0) or 0),
        "ts": ts,
        "age_sec": round(age, 3),
        "stale": stale,
        "source": state.get("source", "unknown"),
        "error": state.get("error", ""),
    }


async def _quote_ws_handler(ws):
    """Push latest quote to websocket clients."""
    try:
        path = getattr(ws, "path", "") or ""
        parsed = urllib.parse.urlparse(path)
        query = urllib.parse.parse_qs(parsed.query or "")
        symbol = (query.get("symbol") or [DEFAULT_SYMBOL])[0]
        client = ws.remote_address
        print(f"[HL-GW] quote ws connected from {client} symbol={symbol}")
        while True:
            quote = _get_quote(symbol)
            await ws.send(json.dumps(quote, ensure_ascii=False))
            await asyncio.sleep(QUOTE_WS_PUSH_SEC)
    except Exception:
        pass


async def _run_ws_server():
    async with websockets.serve(_quote_ws_handler, QUOTE_WS_HOST, QUOTE_WS_PORT, ping_interval=15, ping_timeout=10):
        print(f"[HL-GW] quote websocket listening on {QUOTE_WS_HOST}:{QUOTE_WS_PORT}")
        await asyncio.Future()


def _quote_ws_thread():
    asyncio.run(_run_ws_server())


class Handler(BaseHTTPRequestHandler):
    server_version = "HLRemoteGateway/0.1"

    def do_GET(self):
        global _last_quote_req_log_ts
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
            ws_scheme = "ws"
            self._send(
                200,
                {
                    "ok": True,
                    "ts": time.time(),
                    "quote_ws_url": f"{ws_scheme}://{self.headers.get('Host', '').split(':')[0] or '127.0.0.1'}:{QUOTE_WS_PORT}/quote",
                },
            )
            return

        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/quote":
            query = urllib.parse.parse_qs(parsed.query or "")
            symbol = (query.get("symbol") or [DEFAULT_SYMBOL])[0]
            quote = _get_quote(symbol)

            now = time.time()
            if now - _last_quote_req_log_ts >= 30:
                _last_quote_req_log_ts = now
                print(
                    f"[HL-GW] quote from {self.client_address[0]} "
                    f"symbol={quote.get('symbol')} ok={quote.get('ok')} stale={quote.get('stale')}"
                )

            _append_event(
                {
                    "event": "quote",
                    "client_ip": self.client_address[0],
                    "client_port": self.client_address[1],
                    "path": self.path,
                    "symbol": quote.get("symbol"),
                    "ok": quote.get("ok"),
                    "stale": quote.get("stale"),
                    "age_sec": quote.get("age_sec"),
                }
            )
            code = 200 if quote.get("ok") else 503
            self._send(code, quote)
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
    updater = threading.Thread(target=_quote_updater, daemon=True)
    updater.start()
    ws_thread = threading.Thread(target=_quote_ws_thread, daemon=True)
    ws_thread.start()
    server = ThreadingHTTPServer((host, port), Handler)
    print(
        f"HL remote gateway listening on {host}:{port} "
        f"(quote_symbol={_normalize_symbol(DEFAULT_SYMBOL)}, poll={QUOTE_POLL_SEC}s, ws_port={QUOTE_WS_PORT})"
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
