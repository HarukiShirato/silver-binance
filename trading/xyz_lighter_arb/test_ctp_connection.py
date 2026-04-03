#!/usr/bin/env python3
"""
CTP connection check script.

Features:
- load env file (.env/.env1)
- configurable front retries
- total hard timeout (avoid hanging forever)
- optional market data sampling + account/position query
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("CTPConnCheck")

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import API, get_ctp_front_candidates, load_api_keys  # noqa: E402
from exchanges.ctp_gateway import CTPGateway, TickData  # noqa: E402


def _load_env_file(path: str) -> None:
    p = Path(path)
    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    if not p.exists():
        raise FileNotFoundError(f"env file not found: {p}")
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception as e:
        raise RuntimeError("python-dotenv is required. pip install python-dotenv") from e
    load_dotenv(dotenv_path=str(p), override=True)
    logger.info(f"Loaded env file: {p}")


async def _on_tick(tick: TickData):
    logger.info(
        f"[{tick.instrument_id}] last={tick.last_price:.1f} "
        f"bid1={tick.bid_price1:.1f}x{tick.bid_volume1} "
        f"ask1={tick.ask_price1:.1f}x{tick.ask_volume1} "
        f"vol={tick.volume} oi={tick.open_interest:.0f} t={tick.update_time}"
    )


async def _run(args: argparse.Namespace) -> dict:
    load_api_keys(dry_run=True, hl_exec_mode="remote")

    if not API.ctp_user_id or not API.ctp_password or not API.ctp_auth_code:
        raise RuntimeError("Missing CTP env: CTP_USER_ID / CTP_PASSWORD / CTP_AUTH_CODE")

    fronts = get_ctp_front_candidates()
    if args.max_fronts > 0:
        fronts = fronts[: args.max_fronts]
    if not fronts:
        raise RuntimeError("No CTP fronts configured")

    gateway = CTPGateway(
        broker_id=API.ctp_broker_id,
        user_id=API.ctp_user_id,
        password=API.ctp_password,
        md_front=API.ctp_md_front,
        td_front=API.ctp_td_front,
        app_id=API.ctp_app_id,
        auth_code=API.ctp_auth_code,
    )

    connected_front = None
    last_error = None
    account_obj = None
    positions = []
    try:
        for idx, (md_front, td_front) in enumerate(fronts, start=1):
            logger.info(f"Try front [{idx}/{len(fronts)}] md={md_front} td={td_front}")
            gateway.md_front = md_front
            gateway.td_front = td_front
            try:
                await asyncio.wait_for(gateway.connect(timeout=args.connect_timeout), timeout=args.connect_timeout + 5)
                connected_front = {"md": md_front, "td": td_front}
                logger.info(f"Connected front md={md_front} td={td_front}, trading_day={gateway.trading_day}")
                break
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Front failed md={md_front} td={td_front}, err={e}")
                try:
                    await gateway.close()
                except Exception:
                    pass

        if connected_front is None:
            raise RuntimeError(f"All fronts failed, last_error={last_error}")

        if args.quote_wait_sec > 0:
            for inst in args.instruments:
                gateway.subscribe(inst)
                gateway.on_tick(inst, _on_tick)
            logger.info(f"Subscribed {args.instruments}, wait {args.quote_wait_sec}s for ticks")
            await asyncio.sleep(args.quote_wait_sec)

        logger.info("Query account...")
        account_obj = await gateway.query_account()

        if args.query_positions:
            logger.info("Query positions...")
            positions = await gateway.query_positions()

        account = None
        if account_obj:
            account = {
                "balance": account_obj.balance,
                "available": account_obj.available,
                "frozen": account_obj.frozen,
                "margin": account_obj.margin,
                "profit": account_obj.profit,
                "commission": account_obj.commission,
            }

        pos_items = []
        for p in positions or []:
            if p.volume <= 0:
                continue
            pos_items.append(
                {
                    "instrument_id": p.instrument_id,
                    "direction": p.direction,
                    "volume": p.volume,
                    "available": p.available,
                    "avg_price": p.avg_price,
                    "profit": p.profit,
                    "margin": p.margin,
                }
            )

        return {
            "ok": True,
            "front": connected_front,
            "trading_day": gateway.trading_day,
            "account": account,
            "positions": pos_items,
        }
    finally:
        # NOTE:
        # In some environments, CTP native close/join may block for a long time.
        # For one-shot diagnostic scripts, we skip explicit close here and rely on
        # process termination to avoid hanging the terminal.
        pass


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CTP connection check")
    parser.add_argument("--env-file", default=".env", help="env file path, e.g. .env1")
    parser.add_argument("--hard-timeout-sec", type=float, default=60.0, help="total timeout for full script")
    parser.add_argument("--connect-timeout", type=float, default=20.0, help="connect timeout per front")
    parser.add_argument("--max-fronts", type=int, default=1, help="front count to try (default primary only)")
    parser.add_argument("--quote-wait-sec", type=float, default=0.0, help="wait seconds after subscribe")
    parser.add_argument(
        "--instruments",
        nargs="*",
        default=["ag2506"],
        help="instruments to subscribe when quote-wait-sec > 0",
    )
    parser.add_argument("--query-positions", action="store_true", help="query positions too")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        _load_env_file(args.env_file)
        result = asyncio.run(asyncio.wait_for(_run(args), timeout=args.hard_timeout_sec))
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except asyncio.TimeoutError:
        print(json.dumps({"ok": False, "error": f"timeout>{args.hard_timeout_sec}s"}, ensure_ascii=False))
        return 124
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    code = main()
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    finally:
        os._exit(code)
