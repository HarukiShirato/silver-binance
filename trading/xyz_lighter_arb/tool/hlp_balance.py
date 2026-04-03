#!/usr/bin/env python3
import argparse
import asyncio
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import API, load_api_keys  # noqa: E402
from exchanges.hyperliquid import HyperliquidClient  # noqa: E402


def _load_env_file(path: str) -> None:
    if not path:
        return
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


async def _query_once(include_raw: bool) -> dict:
    # dry_run=True avoids forcing key checks; wallet address is enough for user state.
    load_api_keys(dry_run=True, hl_exec_mode="remote")
    if not API.hl_wallet_address:
        raise RuntimeError("Missing HL_WALLET_ADDRESS in env.")

    client = HyperliquidClient(
        api_url=API.hl_api_url,
        ws_url=API.hl_ws_url,
        dex=API.hl_dex,
        private_key=API.hl_api_wallet_private_key,
        wallet_address=API.hl_wallet_address,
    )
    try:
        state = await client.get_user_state()
    finally:
        await client.close()

    margin_summary = state.get("marginSummary", {}) if isinstance(state, dict) else {}
    account_value = float(margin_summary.get("accountValue", 0) or 0)
    total_margin_used = float(margin_summary.get("totalMarginUsed", 0) or 0)
    withdrawable = float(margin_summary.get("withdrawable", 0) or 0)
    total_raw_usd = float(margin_summary.get("totalRawUsd", account_value) or account_value)
    unrealized_pnl = account_value - total_raw_usd

    result = {
        "ok": True,
        "wallet": API.hl_wallet_address,
        "dex": API.hl_dex,
        "account": {
            "account_value": account_value,
            "available_withdrawable": withdrawable,
            "used_margin": total_margin_used,
            "unrealized_pnl_est": unrealized_pnl,
        },
    }
    if include_raw:
        result["raw"] = state
    return result


async def _main() -> int:
    parser = argparse.ArgumentParser(description="Query Hyperliquid account balance.")
    parser.add_argument("--env-file", default=".env", help="Path to env file, e.g. .env2")
    parser.add_argument("--raw", action="store_true", help="Include raw clearinghouseState payload")
    args = parser.parse_args()

    try:
        _load_env_file(args.env_file)
        result = await _query_once(args.raw)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
