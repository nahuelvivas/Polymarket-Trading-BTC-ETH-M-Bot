# Polymarket split → monitor → redeem — Product Requirements

## Overview

Bot for Polymarket crypto prediction markets (e.g. BTC at 5m/15m). It splits pUSD into YES/NO tokens, polls the order book during the epoch, then redeems winning tokens.

## Core Strategy

### Phase 1: Split
- Call CTF `splitPosition` to convert USDC.e into equal YES + NO shares
- Configurable: `portfolio_allocation_usdc` per market/epoch

### Phase 2: Monitor
- Poll YES/NO best bids and optional strike/spot logging until epoch end

### Phase 3: Redeem
- Wait `redeem_delay_seconds` after resolution
- Redeem winning shares → USDC.e via builder-relayer (gasless)

## State Machine

```
IDLE → SPLIT → MONITOR → REDEEM → IDLE
```

Each state is logged and exported for monitoring.

## Target Markets

Configured via `config/markets.toml`:

| Symbol | Epochs |
|--------|--------|
| BTC    | 5m, 15m |
| ETH    | 5m, 15m |
| SOL    | 5m, 15m |
| XRP    | 5m, 15m |

All market+epoch pairs run in parallel within each cycle.

## Technical Requirements

### Dependencies
- **builder-relayer-client** — Gasless split and redeem via Polymarket relayer
- **py-clob-client** — CLOB order signing and submission
- **CTF contracts** — splitPosition (create tokens) and redeemPositions (recover USDC.e)
- **Gamma API** — Market/event discovery by epoch slug

### Execution Flow
1. Compute epoch slug for each market+interval
2. Fetch event from Gamma API → condition_id, asset_ids (YES/NO tokens)
3. Split USDC.e via relayer: approve + splitPosition in one batched tx
4. Monitor order books until epoch resolution
5. Sleep until redeem delay elapses
6. Redeem winning tokens via relayer

### Error Handling
- **Market not ready**: Skip epoch, retry next cycle
- **Insufficient USDC.e**: Log error, skip split
- **Builder API rate limits**: Retry with next credential from pool (supports 1–19 numbered creds)
- **Partial fills on limit sells**: Winning side redeemed; unfilled shares left in wallet

### Configuration Schema

```yaml
liquidity_maker:
  portfolio_allocation_usdc: 1000
  limit_sell_price: 0.99
  split_approve_first: true
  redeem_delay_seconds: 120
  markets_file: "config/markets.toml"
  export_dir: "exports"
  cycles: 0
```

## Success Metrics (Logging)

All activity is exported to `exports/liquidity_maker_activity.jsonl`:

- **Split success rate** — % of splits that return a tx hash
- **Limit sell fill rate** — Orders filled at $0.99
- **Redemption yield** — USDC.e recovered per market
- **Per-market/epoch P&L** — Revenue from filled sells + redeemed winnings vs. allocated capital
