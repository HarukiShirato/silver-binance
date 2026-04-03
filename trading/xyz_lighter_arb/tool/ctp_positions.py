#!/usr/bin/env python3
import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import API, get_ctp_front_candidates, load_api_keys  # noqa: E402
from exchanges.ctp_gateway import CTPGateway  # noqa: E402


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("ctp_positions_tool")


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


async def _query_once(timeout_sec: float, max_fronts: int, non_zero_only: bool) -> dict:
    load_api_keys(dry_run=True, hl_exec_mode="remote")
    if not API.ctp_user_id or not API.ctp_password or not API.ctp_auth_code:
        raise RuntimeError("Missing CTP env: CTP_USER_ID / CTP_PASSWORD / CTP_AUTH_CODE")

    fronts = get_ctp_front_candidates()
    if max_fronts > 0:
        fronts = fronts[:max_fronts]
    if not fronts:
        raise RuntimeError("No CTP fronts configured.")

    last_error = None
    for md_front, td_front in fronts:
        gateway = CTPGateway(
            broker_id=API.ctp_broker_id,
            user_id=API.ctp_user_id,
            password=API.ctp_password,
            md_front=md_front,
            td_front=td_front,
            app_id=API.ctp_app_id,
            auth_code=API.ctp_auth_code,
        )
        try:
            logger.info(f"Trying CTP front md={md_front}, td={td_front}")
            await asyncio.wait_for(gateway.connect(), timeout=timeout_sec)
            logger.info("CTP connected, querying positions...")
            positions = await gateway.query_positions()
            items = []
            for p in positions or []:
                if non_zero_only and p.volume <= 0:
                    continue
                items.append(
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
            # IMPORTANT: skip gateway.close() here because CTP native close can block.
            return {
                "ok": True,
                "front": {"md": md_front, "td": td_front},
                "trading_day": gateway.trading_day,
                "positions": items,
                "count": len(items),
            }
        except Exception as e:
            last_error = e
            logger.warning(f"Front failed md={md_front}, td={td_front}, err={e}")
    raise RuntimeError(f"All CTP fronts failed. last_error={last_error}")


async def _main() -> int:
    parser = argparse.ArgumentParser(description="Query CTP positions.")
    parser.add_argument("--env-file", default=".env", help="Path to env file, e.g. .env1")
    parser.add_argument("--timeout-sec", type=float, default=20.0, help="Connect timeout per front")
    parser.add_argument("--hard-timeout-sec", type=float, default=60.0, help="Total timeout for whole query")
    parser.add_argument("--max-fronts", type=int, default=1, help="How many CTP fronts to try")
    parser.add_argument("--all", action="store_true", help="Include zero-volume entries")
    args = parser.parse_args()

    try:
        _load_env_file(args.env_file)
        result = await asyncio.wait_for(
            _query_once(args.timeout_sec, args.max_fronts, non_zero_only=not args.all),
            timeout=args.hard_timeout_sec,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except asyncio.TimeoutError:
        print(json.dumps({"ok": False, "error": f"timeout>{args.hard_timeout_sec}s"}, ensure_ascii=False))
        return 124
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    code = asyncio.run(_main())
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    finally:
        os._exit(code)
