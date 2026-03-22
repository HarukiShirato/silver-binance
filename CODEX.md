# CODEX.md

## Project Overview
- Project: `silver-binance-main`
- Active trading module: `trading/xyz_lighter_arb`
- Purpose: spread mean-reversion between SHFE silver (`AG`) and Hyperliquid `SILVER`.
- Core flow: market data -> signal generation (`z-score`) -> risk checks -> dual-leg execution.

## Key Directories
- `backtest/`: historical analysis and parameter research scripts.
- `data/`: generated analysis artifacts and runtime state files.
- `trading/xyz_lighter_arb/`: main live/sim trading codebase (current working module).
- `trading/xyz_lighter_arbv2/`: reference version from collaborator (already merged into `xyz_lighter_arb` for key files).

## Runtime Entry Points
- Main strategy runtime:
  - `trading/xyz_lighter_arb/main.py`
- CTP connectivity/sim check:
  - `trading/xyz_lighter_arb/test_ctp_connection.py`

## Environment Variables
Required in `trading/xyz_lighter_arb`:
- `HL_PRIVATE_KEY`
- `HL_WALLET_ADDRESS`
- `CTP_USER_ID`
- `CTP_PASSWORD`
- `CTP_AUTH_CODE`
- `FEISHU_WEBHOOK_URL` (optional but recommended)

Use `.env.example` as template; keep real secrets in `.env` (do not commit).

## Typical Local Workflow
From `trading/xyz_lighter_arb`:

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Connectivity test:

```bash
set -a
source .env
set +a
python test_ctp_connection.py
```

Run strategy:

```bash
set -a
source .env
set +a
python main.py
```

## Production/Server Notes
- ECS/Ubuntu deployments commonly use `pm2` or `systemd` to keep processes alive.
- Logs should be persisted under `trading/xyz_lighter_arb/logs/`.
- Runtime risk state is persisted to `trading/xyz_lighter_arb/data/risk_state.json`.

## Important Implementation Notes
- Strategy parameters are in `trading/xyz_lighter_arb/config.py`.
- `signal_engine.py` in current version uses time-based sampling (`sample_interval`) to align live stats with backtest-style cadence.
- CTP gateway implementation is in `trading/xyz_lighter_arb/exchanges/ctp_gateway.py`.
- Real order placement path is in `trading/xyz_lighter_arb/execution_engine.py`.

## Troubleshooting Quick Checks
- CTP timeout:
  - check network reachability to CTP fronts (`61219` / `61209`).
  - verify `CTP_USER_ID`, `CTP_PASSWORD`, `CTP_AUTH_CODE`.
  - verify trading session window.
- Hyperliquid errors:
  - verify wallet key/address pair and API reachability.
- If process is alive but no trades:
  - check session gating and signal readiness (`window`, `sample_interval`, data freshness).

## Collaboration Conventions
- Treat `trading/xyz_lighter_arb` as the primary working directory.
- Keep `xyz_lighter_arbv2` as snapshot/reference unless explicitly consolidating more changes.
- Never hardcode credentials into tracked source files.
