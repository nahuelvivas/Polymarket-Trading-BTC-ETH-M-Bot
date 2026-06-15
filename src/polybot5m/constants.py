"""Constants for Polymarket Liquidity Maker Bot."""

from __future__ import annotations

import os

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com"
WS_MSG_BOOK = "book"
WS_MSG_PRICE_CHANGE = "price_change"
WS_MSG_ORDER = "order"
WS_MSG_TRADE = "trade"
WS_USER_PING_INTERVAL_S = 10.0

INTERVAL_SECONDS = {"5m": 300, "15m": 900}

STRUCTURED_SLUG_INTERVALS = {"5m", "15m"}

# CLOB execution
DEFAULT_TICK_SIZE = "0.01"
CHAIN_ID = 137  # Polygon mainnet
CHAIN_ID_AMOY = 80002  # Polygon Amoy testnet
ORDER_TYPE_FOK = "FOK"
ORDER_TYPE_GTC = "GTC"

# ── V2 collateral / CTF (Polygon mainnet) ─────────────────────────────────────
# Canonical list: https://docs.polymarket.com/resources/contracts
# Adapter redeploy (new split/merge/redeem events): May 2026
# https://x.com/PolymarketDevs/status/2049980813014622690
# Relayer stops accepting legacy adapters after 2026-05-01 15:00 UTC.

CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# Current production adapters (route split/merge/redeem through pUSD)
CTF_COLLATERAL_ADAPTER_ADDRESS = "0xAdA100Db00Ca00073811820692005400218FcE1f"
NEG_RISK_CTF_COLLATERAL_ADAPTER_ADDRESS = "0xadA2005600Dec949baf300f4C6120000bDB6eAab"

# Pre–May 2026 adapters — DO NOT use for relayer split/merge/redeem
LEGACY_CTF_COLLATERAL_ADAPTER_ADDRESS = "0xADa100874d00e3331D00F2007a9c336a65009718"
LEGACY_NEG_RISK_CTF_COLLATERAL_ADAPTER_ADDRESS = "0xAdA200001000ef00D07553cEE7006808F895c6F1"

PUSD_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
PUSD_IMPL_ADDRESS = "0x6bBCef9f7ef3B6C592c99e0f206a0DE94Ad0925f"
COLLATERAL_ONRAMP_ADDRESS = "0x93070a847efEf7F70739046A929D47a521F5B8ee"
COLLATERAL_OFFRAMP_ADDRESS = "0x2957922Eb93258b93368531d39fAcCA3B4dC5854"
CTF_EXCHANGE_V2_ADDRESS = "0xE111180000d2663C0091e4f400237545B87B996B"
NEG_RISK_CTF_EXCHANGE_V2_ADDRESS = "0xe2222d279d744050d28e00520010520000310F59"

RELAYER_URL = "https://relayer-v2.polymarket.com/"
SIGNATURE_TYPE_POLY_1271 = 3
DEPOSIT_WALLET_FACTORIES: dict[int, str] = {
    CHAIN_ID: "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07",
    CHAIN_ID_AMOY: "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07",
}
DEPOSIT_WALLET_IMPLEMENTATIONS: dict[int, str] = {
    CHAIN_ID: "0x58CA52ebe0DadfdF531Cde7062e76746de4Db1eB",
    CHAIN_ID_AMOY: "0x50a88fE9a441cB4c9c2aD6A2207CE2795C7D7Fbd",
}
# Legacy alias retained for backward compatibility with older imports.
USDCe_ADDRESS = PUSD_ADDRESS

_LEGACY_COLLATERAL_ADAPTERS = frozenset(
    {
        LEGACY_CTF_COLLATERAL_ADAPTER_ADDRESS.lower(),
        LEGACY_NEG_RISK_CTF_COLLATERAL_ADAPTER_ADDRESS.lower(),
    }
)


def is_legacy_collateral_adapter(address: str) -> bool:
    """True if address is a pre–May 2026 adapter (relayer rejects after cutover)."""
    return (address or "").strip().lower() in _LEGACY_COLLATERAL_ADAPTERS


def _adapter_override(*, neg_risk: bool, override: str = "") -> str:
    """Config/env override for adapter address (empty = use defaults)."""
    raw = (override or "").strip()
    if not raw:
        env_key = (
            "POLYBOT5MBES_EXECUTION__NEG_RISK_CTF_COLLATERAL_ADAPTER"
            if neg_risk
            else "POLYBOT5MBES_EXECUTION__CTF_COLLATERAL_ADAPTER"
        )
        raw = (os.getenv(env_key) or "").strip()
    return raw


def collateral_adapter_address(*, neg_risk: bool = False, override: str = "") -> str:
    """On-chain target for splitPosition / mergePositions / redeemPositions (V2 adapters)."""
    custom = _adapter_override(neg_risk=neg_risk, override=override)
    if custom:
        if is_legacy_collateral_adapter(custom):
            current = (
                NEG_RISK_CTF_COLLATERAL_ADAPTER_ADDRESS
                if neg_risk
                else CTF_COLLATERAL_ADAPTER_ADDRESS
            )
            raise ValueError(
                f"Legacy collateral adapter {custom} is no longer accepted by the relayer. "
                f"Use {current} (see V2_MIGRATION.md)."
            )
        return custom
    if neg_risk:
        return NEG_RISK_CTF_COLLATERAL_ADAPTER_ADDRESS
    return CTF_COLLATERAL_ADAPTER_ADDRESS


def split_target_for_deposit_wallet(*, neg_risk: bool = False, override: str = "") -> str:
    """splitPosition / mergePositions target for deposit-wallet relayer batches."""
    return collateral_adapter_address(neg_risk=neg_risk, override=override)


def merge_target_for_deposit_wallet(*, neg_risk: bool = False, override: str = "") -> str:
    return split_target_for_deposit_wallet(neg_risk=neg_risk, override=override)


def redeem_target_for_deposit_wallet(*, neg_risk: bool = False, override: str = "") -> str:
    return collateral_adapter_address(neg_risk=neg_risk, override=override)


# Chainlink Data Streams (strike at epoch; spot poll)
CHAINLINK_REST_URL = "https://api.dataengine.chain.link"
CHAINLINK_WS_URL = "wss://ws.dataengine.chain.link"
CHAINLINK_PRICE_DECIMALS = 1e18
