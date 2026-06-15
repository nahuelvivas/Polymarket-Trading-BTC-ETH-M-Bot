# Polymarket CLOB V2 — bot migration notes

Polymarket upgraded to **CLOB V2** on **28 April 2026** ([help article](https://help.polymarket.com/en/articles/14762452-polymarket-exchange-upgrade-april-28-2026), [migration guide](https://docs.polymarket.com/v2-migration)).

This bot targets **production V2** at `https://clob.polymarket.com`.

## What this repo already uses (V2)

| Area | Status |
|------|--------|
| CLOB SDK | `py-clob-client-v2` (required) |
| Collateral | **pUSD** (`PUSD_ADDRESS`), not USDC.e |
| CTF / adapters | V2 `CtfCollateralAdapter` addresses in `constants.py` |
| Wallet mode | Deposit wallet (POLY_1271, `signature_type: 3`) |
| Relayer split/redeem | `py-builder-relayer-client` + `BUILDER_API_*` env (unchanged) |

## What you must configure

### 1. Install V2 SDK

```bash
pip install -e ".[relayer,telegram]"
# requires py-clob-client-v2 from pyproject.toml
```

Do **not** use legacy `py-clob-client` — V1-signed orders fail on production.

### 2. Builder code (CLOB orders)

V2 attaches builder attribution via a **builder code** on each order (not `POLY_BUILDER_*` HMAC headers).

1. Open [Polymarket Builder settings](https://polymarket.com/settings?tab=builder)
2. Copy your **builder code** (bytes32)
3. Set in `.env`:

```bash
POLYBOT5MBES_EXECUTION__BUILDER_CODE=0x...
```

**Relayer keys** (`POLYBOT5MBES_EXECUTION__BUILDER_API_KEY/SECRET/PASSPHRASE`) are still required for gasless split/redeem.

### Collateral adapters (May 2026 redeploy)

Polymarket redeployed `CtfCollateralAdapter` and `NegRiskCtfCollateralAdapter` with **new events** for split, merge, and redeem. The relayer rejects the old adapters after **2026-05-01 15:00 UTC**.

| Contract | Current (use this) | Legacy (do not use) |
|----------|-------------------|---------------------|
| CtfCollateralAdapter | `0xAdA100Db00Ca00073811820692005400218FcE1f` | `0xADa100874d00e3331D00F2007a9c336a65009718` |
| NegRiskCtfCollateralAdapter | `0xadA2005600Dec949baf300f4C6120000bDB6eAab` | `0xAdA200001000ef00D07553cEE7006808F895c6F1` |

Source: [Polymarket contracts](https://docs.polymarket.com/resources/contracts), [dev announcement](https://x.com/PolymarketDevs/status/2049980813014622690).

Optional overrides (normally leave empty):

```bash
# POLYBOT5MBES_EXECUTION__CTF_COLLATERAL_ADAPTER=0xAdA100Db00Ca00073811820692005400218FcE1f
# POLYBOT5MBES_EXECUTION__NEG_RISK_CTF_COLLATERAL_ADAPTER=0xadA2005600Dec949baf300f4C6120000bDB6eAab
```

The bot rejects legacy adapter addresses at runtime.

### 3. Fund with pUSD

- UI users: Polymarket wraps USDC → pUSD automatically (one-time approval).
- API-only: fund the deposit wallet with **pUSD** (bridge deposit or Collateral Onramp `wrap()`).
- Split/redeem operate on pUSD via V2 adapters.

### 4. Re-place open orders

Pre-cutover resting orders were cleared. Restart the bot after upgrade.

## V2 order signing changes (handled by this bot)

- No `feeRateBps`, `nonce`, or `taker` on signed orders — fees set at match time.
- New fields: `timestamp`, `metadata`, `builder` (`builder_code`).
- EIP-712 Exchange domain version `"2"` (SDK handles this).

## Checklist

- [ ] `py-clob-client-v2` installed (not `py-clob-client`)
- [ ] `POLYBOT5MBES_EXECUTION__BUILDER_CODE` set (optional but recommended for builders)
- [ ] `POLYBOT5MBES_EXECUTION__BUILDER_API_*` set for relayer split/redeem
- [ ] Deposit wallet funded with **pUSD**
- [ ] Paper-test one epoch before live trading

## References

- [CLOB V2 migration guide](https://docs.polymarket.com/v2-migration)
- [Exchange upgrade FAQ](https://help.polymarket.com/en/articles/14762452-polymarket-exchange-upgrade-april-28-2026)
- [Contract addresses](https://docs.polymarket.com/resources/contracts)
