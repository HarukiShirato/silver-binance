# CODEX.md

## Project Overview
- Project: `silver-binance-main`
- Active module: `trading/xyz_lighter_arb`
- Strategy: SHFE silver (`AG`) vs Hyperliquid `xyz:SILVER` spread mean-reversion
- Execution mode in current deployment: Shanghai strategy process + Tokyo HL remote gateway

## Current Deployment Topology
- Shanghai (ECS):
  - Runs `main.py`
  - Connects CTP (MD/TD), forex feed
  - Uses remote HL quote/exec endpoints from Tokyo
- Tokyo (EC2):
  - Runs `hl_remote_gateway.py`
  - Provides:
    - REST health: `:18080/health`
    - REST quote: `:18080/quote`
    - WS quote stream: `:18081/quote`
    - Remote dry-run/live exec ack on `:18080`

## Key Directories
- `trading/xyz_lighter_arb/main.py`: strategy runtime entry
- `trading/xyz_lighter_arb/config.py`: all strategy/runtime/risk config
- `trading/xyz_lighter_arb/data_engine.py`: CTP/HL/FX ingestion and normalization
- `trading/xyz_lighter_arb/exchanges/ctp_gateway.py`: CTP adapter
- `trading/xyz_lighter_arb/exchanges/hyperliquid.py`: HL API client
- `trading/xyz_lighter_arb/hl_remote_gateway.py`: Tokyo remote gateway
- `trading/xyz_lighter_arb/notifier.py`: Feishu notifications
- `trading/xyz_lighter_arb/data/`: runtime state and trade/event artifacts
- `trading/xyz_lighter_arb/logs/`: runtime logs

## Important Runtime Config (Current)
- Sampling and signals:
  - `entry_zscore = 2.3`
  - `exit_zscore = 0.8`
  - `sample_interval = 45`
  - `spread_window = 60`
- Cooldown:
  - `cooldown_seconds = 120`
  - only blocks new entries (`LONG/SHORT`), does not block exits
- Session behavior:
  - `disconnect_ctp_when_closed = true`
  - non-session auto disconnect is enabled
- Position reconcile:
  - `position_reconcile_consistency_count = 3`
  - `position_reconcile_correction_cooldown_sec = 120`
  - `DRY_RUN`: mismatch alert only, no external-force local correction
- HL remote:
  - `HL_EXEC_MODE=remote`
  - `HL_DATA_MODE=remote`
  - `HL_REMOTE_URL=http://<tokyo-ip>:18080`
  - `HL_REMOTE_QUOTE_WS_URL=ws://<tokyo-ip>:18081/quote`

## Secrets and Security
- Do not store HL private key in `.env`.
- Live local-HL signer key is expected from AWS Secrets Manager (`HL_API_WALLET_SECRET_ID`, `AWS_REGION`) when needed.
- `.env.example` is the template; real `.env` must stay untracked.

## Startup / Operations
- Main process (Shanghai):
  - `pm2 restart silver-hedge --update-env`
- Tokyo gateway:
  - keep `hl-remote` running under pm2
- Recommended cron safety net (Shanghai):
  - restart before session starts (08:58, 13:29, 20:58 + Sunday 20:58) to avoid missed reopen

## Observability
- Core logs:
  - `trading/xyz_lighter_arb/logs/hedge.log`
  - PM2 logs: `/root/.pm2/logs/silver-hedge-*.log` (server-side)
- Runtime states:
  - `trading/xyz_lighter_arb/data/risk_state.json`
  - signal window persistence (warm start) is enabled
- Event/trade artifacts:
  - `trading/xyz_lighter_arb/data/trade_events.csv`
  - optional custom summaries in `data/`

## Common Quick Checks
- CTP fronts reachable:
  - `nc -vz <md_front_host> <md_port>`
  - `nc -vz <td_front_host> <td_port>`
- Tokyo HL endpoints:
  - `curl http://<tokyo-ip>:18080/health`
  - `curl "http://<tokyo-ip>:18080/quote?symbol=xyz:SILVER"`
  - `nc -vz <tokyo-ip> 18081`
- If no trades:
  - check `session` not `closed`
  - check `window` progress reaches `60/60`
  - check stale flags (`ag/hl/fx`) and `blocked_*` reasons in logs

## Known Operational Pitfalls
- If CTP connect fails at session boundary, process may stay alive but not trading.
- If Tokyo quote ws is unreachable, system falls back to HTTP polling; latency and staleness risk increase.
- “Position mismatch” in DRY_RUN can be local-state drift; external legs may both be flat.

## Collaboration Notes
- Primary working path: `trading/xyz_lighter_arb`
- Do not reintroduce deprecated `HL_PRIVATE_KEY` env usage.
- Keep diffs surgical; avoid file-wide encoding/EOL rewrites.
