# Polymarket BTC ETH 5min bot


This bot is a very short-term scalping system that trades only in the final seconds of a 5-minute Polymarket BTC/ETH epoch. It looks for strong divergence between Chainlink strike pricing and Polymarket spot, but only acts when both BTC and ETH agree on direction to confirm the signal. If conditions like tight spreads, sufficient liquidity, and repeated confirmation over a couple of ticks are met, it enters a quick YES or NO position. After entry, it exits almost immediately using tight risk controls, mainly a short time-stop and crash protection, and then waits for the next epoch.


Polymarket **CLOB V2** bot for BTC and ETH **5-minute** Up/Down epoch markets. See [V2_MIGRATION.md](V2_MIGRATION.md).

- **Monitor → redeem** epoch flow (no split/merge)
- **btc** + **eth** only — merged monitor wave with strike/spot and order-book ticks
- **Chainlink Data Streams** for strike and live spot
- **buy1** (BTC) and **buy2** (ETH) spike entries when books and spot/strike align
- **risk1** / **risk2** / **risk3** protective exits after a confirmed buy fill

Main runtime files:

- `src/polybot5m/engine.py` — cycle orchestration, schedule, redeem
- `src/polybot5m/execution/executor.py` — per-market monitor loop, merged wave logs
- `src/polybot5m/execution/exit_strategy.py` — `EntryStrategyCoordinator` (buy1/buy2 + risk exits)

## Epoch lifecycle (`liquidity_maker`)

```
MONITOR until epoch end (entries + risk exits)
    → [optional] REDEEM after delay
    → next cycle
```

- **monitor** — REST-polls CLOB `/book` for YES/NO, optional CLOB user WS (fills), Chainlink strike/spot, strategy logic.
- **redeem_enabled** — redeem winning tokens after epoch end + `redeem_delay_seconds` (off by default).

Trading and logging run on **btc** and **eth** only.

## Price feeds (`price_feed`)

| Setting | Purpose |
|---------|---------|
| `provider: chainlink` | Strike at epoch start (Polymarket resolution family) |
| `spot_provider: chainlink` | Live spot for `spot_minus_strike` and `[STRIKE_SPOT]` logs |
| `chainlink_spot_poll_interval_s` | How often spot is refreshed (default `0.15`) |

Chainlink credentials belong in env vars (`POLYBOT5MBES_PRICE_FEED__CHAINLINK__STREAMS_USER_ID`, `__STREAMS_SECRET`), not committed YAML.

## Trading schedule (`schedule`)

When `schedule.enabled: true`, the bot sleeps outside configured UTC windows (default Mon–Sat, two windows per day). Default config has `schedule.enabled: false` for 24/7 operation.

Schedule gates the **entire bot** (no monitor cycles while sleeping). It does not gate buy1/buy2 independently — if the bot is running, strategies evaluate on every monitor tick inside the epoch window.

## Strategy overview

| Strategy | Symbol | Window (t_minus) | Action |
|----------|--------|------------------|--------|
| buy1 | btc | 60s → 8s | GTC BUY btc YES or NO |
| buy2 | eth | 60s → 8s | GTC BUY eth YES or NO |
| risk1 | btc or eth | after fill | GTC SELL (stop loss vs entry) |
| risk2 | btc or eth | after fill | GTC SELL (time stop) |
| risk3 | btc or eth | after fill | GTC SELL (low bid) |

**buy1** and **buy2** are independent — both can fire in the same epoch. Each strategy fires **at most once per epoch** (first side that satisfies `monitoring_cycles` consecutive ticks). There is no priority between buy1 and buy2.

After a successful **risk1**, **risk2**, or **risk3** exit (position flat), **no new buy1 (btc) or buy2 (eth)** on that symbol for the rest of the epoch.

### Shared entry gates (buy1 and buy2)

All must pass on every monitoring tick:

1. **Time** — `trigger_time_end_sec ≤ t_minus ≤ trigger_time_start_sec` (uses min remaining time across btc/eth).
2. **Spot vs strike** — `|spot_minus_strike_btc|`, `|averge_spot_minus_btc|` (epoch average), and `|spot_minus_strike_eth|` each above configured mins.
3. **Sign alignment** — if `spot_minus_strike_btc > 0`, both `averge_spot_minus_btc` and `spot_minus_strike_eth` must be `> 0`; if `< 0`, both must be `< 0`.
4. **Leg book** — target symbol’s chosen side: spread `< max_spread` and `best_ask > min_best_ask`.
5. **Other leg** — same outcome on the paired symbol: `best_bid > other_symbol_min_best_bid`.
6. **Consecutive ticks** — conditions hold for `monitoring_cycles` ticks (default 2); first side (YES or NO) to qualify wins and locks that outcome.

Order: **GTC** limit buy at `buy_limit_price` (default 0.99) for `shares` (default 5). On order accept, that strategy is marked done for the epoch. FAK no-match retry logic still applies if `order_type: FAK` is configured; other rejections abort the strategy for the epoch.

### Fill tracking

After **BUY1_ORDER** / **BUY2_ORDER**, fill is confirmed when:

- CLOB user WS reports a BUY trade (`MATCHED` / `MINED` / `CONFIRMED`) on the bought token, and/or
- on-chain `rem_YES` / `rem_NO` increases above the pre-order baseline (paper mode).

Logs **BUY1_FILL** / **BUY2_FILL** with average fill price and size. Risk monitoring starts only after fill confirmation.

### risk1 (stop loss vs entry)

After a confirmed buy fill on btc or eth:

- If bought outcome `best_bid < fill_price - loss_offset` for `monitoring_cycles` ticks → sell all position shares at `best_bid − sell_offset` (GTC).
- Up to `max_sell_attempts` sell tries (retried on subsequent monitor ticks while `exit_busy`).

Default: `enabled: false`.

### risk2 (time stop)

After a confirmed buy fill:

- If still holding shares `hold_timeout_sec` after the first fill timestamp → sell at `best_bid − sell_offset` (GTC).
- Up to `max_sell_attempts` tries.

Default: `enabled: true`, `hold_timeout_sec: 10`.

### risk3 (absolute low bid)

After a confirmed buy fill:

- If bought outcome `best_bid < bid_below` for `monitoring_cycles` ticks → sell at `best_bid − sell_offset` (GTC).
- Up to `max_sell_attempts` tries.

Default: `enabled: true`, `bid_below: 0.03`.

**Priority each tick:** risk1 → risk2 → risk3 (first match wins).

### Position / re-entry rules

- No new buy on a symbol while an order is pending or an open filled position exists.
- After successful risk exit (`RISK1_DONE` / `RISK2_DONE` / `RISK3_DONE` with flat balance), that symbol is blocked for buy1/buy2 until the next epoch.
- Epoch stats (btc sms average, buy state, buy blocks) reset at each new epoch via `reset_btc_epoch_stats()`.

## Default parameters (`config/default.yaml`)

### Bot

- `bot.paper_trading`: `true` (paper) / `false` (live) — primary mode switch for Windows exe and YAML-only runs
- `bot.dry_run`: `false` (when `paper_trading: true`, dry_run is ignored)

- `liquidity_maker.epoch`: `5m`
- `liquidity_maker.symbols`: `[btc, eth]`
- `liquidity_maker.cycles`: `0` (run forever)
- `liquidity_maker.stagger_delay_seconds`: `0`

### Redeem

- `liquidity_maker.redeem_enabled`: `false`
- `liquidity_maker.redeem_async_enabled`: `true`
- `liquidity_maker.redeem_delay_seconds`: `150`
- `liquidity_maker.redeem_per_symbol_gap_seconds`: `10`
- `liquidity_maker.redeem_max_retries`: `5`

### Monitor / logging

- `liquidity_maker.monitor_poll_interval_s`: `0.15`
- `liquidity_maker.monitor_rest_book_timeout_s`: `3.0`
- `liquidity_maker.monitor_balance_refresh_timeout_s`: `3.0`
- `liquidity_maker.monitor_balance_force_refresh_min_s`: `1.0`
- `liquidity_maker.monitor_wave_collect_timeout_s`: `3.0`
- `liquidity_maker.monitor_user_ws_enabled`: `true` (recommended for live fill detection)
- `liquidity_maker.monitor_balance_poll_interval_s`: `2.0`
- `liquidity_maker.monitor_log_interval_s`: `0.15`
- `liquidity_maker.monitor_verbose_seconds_before_end`: `165`
- `liquidity_maker.log_strike_spot_interval_s`: `0.15`
- `liquidity_maker.spot_minus_strike_difference_rate_lookback_s`: `0` (difference_rate not logged)
- `liquidity_maker.trading_process_jsonl`: `exports/trading_process.jsonl`
- `liquidity_maker.trading_process_log_mode`: `trades` (`full` optional)

Merged wave console output per tick:

```
⏰t_minus=…
  [btc/5m] [STRIKE_SPOT] …
  [eth/5m] [STRIKE_SPOT] …
  [btc/5m] [MARKET_TICK] …
  [eth/5m] [MARKET_TICK] …
```

### buy1

- `enabled`: `true`
- `trigger_time_start_sec` / `trigger_time_end_sec`: `45` / `3`
- `max_spread`: `0.1`, `min_best_ask`: `0.90`
- `spot_minus_strike_btc_abs_min`: `40`, `averge_spot_minus_btc_abs_min`: `18`, `spot_minus_strike_eth_abs_min`: `1.0`
- `other_symbol_min_best_bid`: `0.90`
- `monitoring_cycles`: `2`, `shares`: `5`, `buy_limit_price`: `0.99`, `order_type`: `GTC`

### buy2

- `enabled`: `true`
- `trigger_time_start_sec` / `trigger_time_end_sec`: `45` / `3`
- `spot_minus_strike_btc_abs_min`: `55` (stricter than buy1), `spot_minus_strike_eth_abs_min`: `1.5`
- Other fields mirror buy1 unless noted in YAML.

### risk1 / risk2 / risk3

- `risk1`: `enabled: false`, `loss_offset: 0.20`, `monitoring_cycles: 3`, `sell_offset: 0.05`, `max_sell_attempts: 5`
- `risk2`: `enabled: true`, `hold_timeout_sec: 10`, `sell_offset: 0.10`, `max_sell_attempts: 5`
- `risk3`: `enabled: true`, `bid_below: 0.03`, `monitoring_cycles: 3`, `sell_offset: 0.02`, `max_sell_attempts: 5`

### Schedule (UTC)

- `schedule.enabled`: `false` (set `true` to restrict hours)
- `schedule.weekdays`: Mon–Sat
- Windows: `04:00–11:01`, `18:00–22:01`

## Setup

### Windows or Mac (no Python install)

1. Clone this repository from Github.
2. Edit `config/default.yaml` — set `bot.paper_trading` (`true` = paper, `false` = live).
3. Copy `.env.example` to `.env` and fill wallet / API / Chainlink secrets.
4. Double-click `POLY-BTC-ETH-BOT.exe` or run from Command Prompt in that folder for Windows.
5. Double-click `POLY-BTC-ETH-BOT-Mac.exe` in that folder for Mac.

Expected layout:

```
Polymarket-BTC-ETH-Bot/
  POLY-BTC-ETH-BOT.exe
  config/default.yaml
  .env
  logs/
  exports/
```

The exe reads `config/default.yaml` next to it and uses `bot.paper_trading` for paper vs live (no `--paper` flag needed). CLI flags on Linux still override YAML when passed.

### Linux / Ubuntu (Python 3.11+)

```bash
cd Polymarket-BTC-ETH-Bot
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[relayer]"
cp .env.example .env   # wallet, CLOB API, builder keys, Chainlink creds
```

Run:

```bash
polybot5m run --paper
```

Or set `bot.paper_trading: true` in `config/default.yaml` and run:

```bash
polybot5m run
```

Without install:

```bash
PYTHONPATH=src python3 -m polybot5m.cli run --paper
```

### Building the Windows exe

On a Windows machine with Python 3.11+:

```powershell
cd Polymarket-BTC-ETH-Bot
.\build\build_windows.ps1
```

This writes `POLY-BTC-ETH-BOT.exe` in the repo root. Rebuild after code changes; ship the exe with `config/default.yaml` and `.env.example` (users add their own `.env`).

## Usage

```bash
polybot5m run                         # live mode (or paper if bot.paper_trading: true in YAML)
polybot5m run --dry-run               # no on-chain redeem tx
polybot5m run --paper                 # paper execution mode (overrides YAML)
polybot5m run -c config/default.yaml --paper
polybot5m run --cycles 5
polybot5m-backtest --help
```

**Paper vs live in `config/default.yaml`:**

```yaml
bot:
  paper_trading: true   # paper — simulated fills, safe for testing
  # paper_trading: false  # live — real CLOB orders (requires .env wallet + API keys)
```

`--paper` on the CLI forces paper mode even when YAML says `false`. Env override: `POLYBOT5MBES_BOT__PAPER_TRADING=true`.

Config overrides use `POLYBOT5MBES_` with `__` nesting, for example:

- `POLYBOT5MBES_EXECUTION__RPC_URL`
- `POLYBOT5MBES_BUY1__ENABLED=false`
- `POLYBOT5MBES_RISK2__ENABLED=false`
- `POLYBOT5MBES_SCHEDULE__ENABLED=false`
- `POLYBOT5MBES_PRICE_FEED__CHAINLINK__STREAMS_USER_ID`
- `POLYBOT5MBES_PRICE_FEED__CHAINLINK__STREAMS_SECRET`

## Logging outputs

Console tags: `[MARKET_TICK]`, `[STRIKE_SPOT]`, strategy events.

Strategy JSONL events (phases **BUY1**, **BUY2**, **RISK1**, **RISK2**, **RISK3**):

| Event | Meaning |
|-------|---------|
| `BUY1_TRIGGER` / `BUY2_TRIGGER` | Entry conditions met, sending order |
| `BUY1_ORDER` / `BUY2_ORDER` | Order accepted, awaiting fill |
| `BUY1_FILL` / `BUY2_FILL` | Fill confirmed |
| `BUY1_FAK_NO_MATCH` / `BUY2_FAK_NO_MATCH` | FAK had no match; may retry |
| `BUY1_ABORT` / `BUY2_ABORT` | Order rejected (non-FAK-no-match) |
| `RISK1_TRIGGER` / `RISK2_TRIGGER` / `RISK3_TRIGGER` | Risk exit started |
| `RISK*_ORDER` / `RISK*_RETRY` | Sell order placed |
| `RISK*_DONE` | Position flat; symbol blocked for rest of epoch |
| `RISK*_FAIL` / `RISK*_RETRY_FAIL` | Sell attempt rejected |

Export path: `liquidity_maker.trading_process_jsonl`. Optional file logging: `bot.log_file` + `bot.log_timestamp_name`.

## Quick config check

```bash
python3 -c "
from polybot5m.config import load_config
c = load_config('config/default.yaml')
print('markets:', [(m.symbol, m.epoch) for m in c.liquidity_maker.markets])
print('spot:', c.price_feed.spot_provider, 'strike:', c.price_feed.provider)
print('paper_trading:', c.bot.paper_trading)
print('buy1:', c.buy1.enabled, 'buy<img width="794" height="563" alt="6160965350590191187" src="https://github.com/user-attachments/assets/665edf6c-624c-43ef-84f2-acded869a0a7" />
2:', c.buy2.enabled)
print('risk1:', c.risk1.enabled, 'risk2:', c.risk2.enabled, 'risk3:', c.risk3.enabled)
print('redeem:', c.liquidity_maker.redeem_enabled)
"
```

## Notes

- Strike/spot metrics are trading signals and log fields, not the settlement oracle.
- Keep secrets (builder keys, Chainlink creds, private key) in env vars rather than committed YAML.
- Test with `bot.paper_trading: true` (or `--paper` on Linux) before live execution. Paper mode disables user WS; fills are inferred from `rem_*` balance changes.
- `averge_spot_minus_btc` spelling matches config/code (epoch average of btc `spot_minus_strike`).

## Result Screenshot

<img width="794" height="563" alt="6160965350590191187" src="https://github.com/user-attachments/assets/ef93eb04-38eb-435b-9f29-b4c492007ceb" />


<img width="794" height="334" alt="6160965350590191188" src="https://github.com/user-attachments/assets/e7f0013c-9d8c-4bbc-9006-38900cf88a8b" />


<img width="805" height="784" alt="6160965350590191189" src="https://github.com/user-attachments/assets/102bee3c-5d38-47fd-814d-0a989c1846c8" />



## 👤 Operator / Contact

If you encounter any issues or are interested in other profitable Polymarket trading bots, feel free to reach out to me.

- Telegram: [@nahuelvivas88 ](https://t.me/nahuelvivas88) 
