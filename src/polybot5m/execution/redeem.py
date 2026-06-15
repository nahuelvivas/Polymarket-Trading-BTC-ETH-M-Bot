"""Redeem winning conditional tokens via CtfCollateralAdapter after market resolution.

Supports two paths:
- Relayer (gasless): use Polymarket relayer + builder API key — same as TypeScript claimWinnings.
- Direct: sign and send tx with Web3 (you pay gas).

When rate limited, retries with next builder cred from pool (POLYBOT5MBES_EXECUTION__BUILDER_API_KEY_1..5).
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from eth_abi import encode
from eth_utils import keccak, to_checksum_address

from polybot5m.constants import (
    CTF_COLLATERAL_ADAPTER_ADDRESS,
    PUSD_ADDRESS,
    RELAYER_URL,
    redeem_target_for_deposit_wallet,
)
from polybot5m.execution.deposit_wallet import (
    DepositWalletCall,
    DepositWalletRelayer,
    resolve_deposit_wallet_address,
)

# Same as TypeScript: ctfInterface.encodeFunctionData("redeemPositions", [
#   pUSD, ethers.constants.HashZero, marketConditionId, [1, 2]
# ])
REDEEM_ABI = [
    {
        "name": "redeemPositions",
        "type": "function",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "outputs": [],
    }
]

# ethers.constants.HashZero = 32 zero bytes
HASH_ZERO = b"\x00" * 32

# Env prefix for builder API creds
BUILDER_ENV_PREFIX = "POLYBOT5MBES_EXECUTION__BUILDER_API_"


def _is_rate_limit_error(err: str) -> bool:
    """True if error looks like a rate limit / quota response."""
    s = (err or "").lower()
    return any(
        x in s
        for x in ("rate limit", "rate limit exceeded", "429", "too many requests", "quota exceeded", "throttl")
    )


def _is_wallet_busy_error(err: str | None) -> bool:
    """Relayer rejects concurrent actions on the same deposit wallet."""
    s = (err or "").lower()
    return any(
        x in s
        for x in (
            "in-flight",
            "in flight",
            "wallet busy",
            "active action",
        )
    )


def _load_builder_creds_pool() -> list[tuple[str, str, str]]:
    """
    Load builder API credentials from env. Supports:
    - POLYBOT5MBES_EXECUTION__BUILDER_API_KEY_1, _SECRET_1, _PASSPHRASE_1 (through _5 or more)
    - Legacy: BUILDER_API_KEY, BUILDER_API_SECRET, BUILDER_API_PASSPHRASE (no suffix)

    Returns list of (key, secret, passphrase) tuples. Numbered creds first, then legacy.
    """
    pool: list[tuple[str, str, str]] = []
    for i in range(1, 20):  # support 1..19
        key = os.getenv(f"{BUILDER_ENV_PREFIX}KEY_{i}", "")
        secret = os.getenv(f"{BUILDER_ENV_PREFIX}SECRET_{i}", "")
        passphrase = os.getenv(f"{BUILDER_ENV_PREFIX}PASSPHRASE_{i}", "")
        if key and secret and passphrase:
            pool.append((key, secret, passphrase))
    # Legacy single cred (no number)
    key = os.getenv("POLYBOT5MBES_EXECUTION__BUILDER_API_KEY", "")
    secret = os.getenv("POLYBOT5MBES_EXECUTION__BUILDER_API_SECRET", "")
    passphrase = os.getenv("POLYBOT5MBES_EXECUTION__BUILDER_API_PASSPHRASE", "")
    if key and secret and passphrase:
        legacy = (key, secret, passphrase)
        if legacy not in pool:
            pool.append(legacy)
    return pool


def ordered_builder_cred_pool(
    cred_pool: list[tuple[str, str, str]],
    cred_index: int,
    *,
    rotation_seconds: float = 0.0,
    stagger_markets: bool = False,
) -> list[tuple[str, str, str]]:
    """Order credentials for split/redeem: time-rotation + optional per-market stagger, then failover order.

    When rotation_seconds <= 0, behavior matches legacy: start at cred_index % n.
    When rotation_seconds > 0 and stagger_markets is False, all markets share the same
    time slot as first try. When stagger_markets is True, add cred_index into the offset.
    """
    n = len(cred_pool)
    if n == 0:
        return []
    rs = float(rotation_seconds or 0.0)
    if rs > 0 and n > 1:
        slot = int(time.time() // rs) % n
    else:
        slot = 0
    if rs > 0 and n > 1 and not stagger_markets:
        extra = 0
    else:
        extra = int(cred_index) % n
    base = (slot + extra) % n
    return [cred_pool[(base + i) % n] for i in range(n)]


def _condition_id_to_bytes32(condition_id: str) -> bytes:
    """Convert hex condition_id (0x... or raw hex) to 32-byte bytes."""
    raw = condition_id.strip()
    if raw.startswith("0x"):
        raw = raw[2:]
    b = bytes.fromhex(raw)
    if len(b) > 32:
        return b[-32:]
    if len(b) < 32:
        return b"\x00" * (32 - len(b)) + b
    return b

def _function_selector(signature: str) -> bytes:
    """First 4 bytes of Keccak-256 of the function signature."""
    return keccak(text=signature)[:4]


def _encode_redeem_calldata(
    collateral_token: str,
    condition_id_b32: bytes,
    index_sets: list[int],
) -> str:
    """Return hex-encoded calldata for redeemPositions(collateralToken, parentCollectionId, conditionId, indexSets).

    Same pattern as: selector = keccak(sig)[:4]; data = selector + encode(types, args).
    Matches TypeScript: ctfInterface.encodeFunctionData("redeemPositions", [USDC_E, HashZero, marketConditionId, [1,2]])
    """
    selector = _function_selector("redeemPositions(address,bytes32,bytes32,uint256[])")
    encoded_args = encode(
        ["address", "bytes32", "bytes32", "uint256[]"],
        [to_checksum_address(collateral_token), HASH_ZERO, condition_id_b32, index_sets],
    )
    return "0x" + (selector + encoded_args).hex()


def _redeem_via_relayer(
    condition_id: str,
    private_key: str,
    chain_id: int,
    builder_api_key: str,
    builder_api_secret: str,
    builder_api_passphrase: str,
    rpc_url: str = "https://rpc.ankr.com/polygon/b7025907c7c47329edc930ec748839dd7d71e4d5ce738db39aa03eec87bdd3f2",
    relayer_url: str = RELAYER_URL,
    ctf_address: str = CTF_COLLATERAL_ADAPTER_ADDRESS,
    collateral_token: str = PUSD_ADDRESS,
    index_sets: list[int] | None = None,
) -> dict[str, Any]:
    """
    Execute redeem via Polymarket relayer (gasless). Matches TypeScript claimWinnings flow.
    Requires builder API credentials (POLY_BUILDER_API_KEY, etc.).
    """
    return _redeem_batch_via_relayer(
        condition_ids=[condition_id],
        private_key=private_key,
        chain_id=chain_id,
        builder_api_key=builder_api_key,
        builder_api_secret=builder_api_secret,
        builder_api_passphrase=builder_api_passphrase,
        rpc_url=rpc_url,
        relayer_url=relayer_url,
        ctf_address=ctf_address,
        collateral_token=collateral_token,
        index_sets=index_sets,
    )


def _redeem_batch_via_relayer(
    condition_ids: list[str],
    private_key: str,
    chain_id: int,
    builder_api_key: str,
    builder_api_secret: str,
    builder_api_passphrase: str,
    rpc_url: str = "https://rpc.ankr.com/polygon/b7025907c7c47329edc930ec748839dd7d71e4d5ce738db39aa03eec87bdd3f2",
    relayer_url: str = RELAYER_URL,
    ctf_address: str = CTF_COLLATERAL_ADAPTER_ADDRESS,
    collateral_token: str = PUSD_ADDRESS,
    index_sets: list[int] | None = None,
    deposit_wallet_address: str = "",
) -> dict[str, Any]:
    """
    Batch redeem multiple markets in one deposit-wallet relayer WALLET batch.
    Returns {tx_hash, error, condition_ids} where condition_ids are the ones included in the batch.
    """
    if not condition_ids:
        return {"tx_hash": None, "error": "No condition_ids to redeem", "condition_ids": []}

    index_sets = index_sets if index_sets is not None else [1, 2]
    try:
        wallet_address = resolve_deposit_wallet_address(
            private_key=private_key,
            chain_id=chain_id,
            configured_funder=deposit_wallet_address,
        )
        redeem_target = ctf_address or redeem_target_for_deposit_wallet(neg_risk=False)
        print(
            f"  [redeem] deposit wallet adapter={redeem_target[:10]}…",
            flush=True,
        )
        calls: list[DepositWalletCall] = []
        for condition_id in condition_ids:
            condition_id_b32 = _condition_id_to_bytes32(condition_id)
            data_hex = _encode_redeem_calldata(collateral_token, condition_id_b32, index_sets)
            calls.append(
                DepositWalletCall(
                    target=redeem_target,
                    value="0",
                    data=data_hex,
                )
            )

        if len(calls) > 1:
            print(f"  Batch redeeming {len(calls)} condition_ids in one deposit-wallet call")
        else:
            print(f"data_hex: {calls[0].data}")

        relayer = DepositWalletRelayer(
            relayer_url=relayer_url,
            chain_id=chain_id,
            private_key=private_key,
            builder_api_key=builder_api_key,
            builder_api_secret=builder_api_secret,
            builder_api_passphrase=builder_api_passphrase,
        )
        if not relayer.is_deployed(wallet_address):
            return {
                "tx_hash": None,
                "error": f"deposit wallet {wallet_address} is not deployed",
                "condition_ids": condition_ids,
            }

        nonce = relayer.get_wallet_nonce()
        deadline = str(int(time.time()) + 600)
        response = relayer.execute_deposit_wallet_batch(
            wallet_address=wallet_address,
            calls=calls,
            nonce=nonce,
            deadline=deadline,
        )
        result = response.wait()
        tx_hash = None
        if result:
            tx_hash = result.get("transactionHash") or result.get("transaction_hash")
        if not tx_hash and getattr(response, "transaction_hash", None):
            tx_hash = response.transaction_hash
        if tx_hash:
            return {"tx_hash": tx_hash, "error": None, "condition_ids": condition_ids}
        return {"tx_hash": None, "error": "Relayer did not return transaction hash", "condition_ids": condition_ids}
    except Exception as e:
        return {"tx_hash": None, "error": str(e), "condition_ids": condition_ids}


async def redeem_positions(
    condition_id: str,
    private_key: str,
    rpc_url: str,
    chain_id: int = 137,
    ctf_address: str = CTF_COLLATERAL_ADAPTER_ADDRESS,
    collateral_token: str = PUSD_ADDRESS,
    index_sets: list[int] | None = None,
    *,
    use_relayer: bool = True,
    api_key: str | None = None,
    api_secret: str | None = None,
    api_passphrase: str | None = None,
    relayer_url: str = RELAYER_URL,
    builder_cred_rotation_seconds: float = 0.0,
    builder_cred_rotation_stagger_markets: bool = False,
) -> dict[str, Any]:
    """
    Redeem winning CTF positions for a single resolved market.

    If use_relayer is True and api_key, api_secret, api_passphrase are all set,
    uses Polymarket relayer (gasless, same as TypeScript claimWinnings).
    """
    return await redeem_positions_batch(
        condition_ids=[condition_id],
        private_key=private_key,
        rpc_url=rpc_url,
        chain_id=chain_id,
        ctf_address=ctf_address,
        collateral_token=collateral_token,
        index_sets=index_sets,
        use_relayer=use_relayer,
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
        relayer_url=relayer_url,
        builder_cred_rotation_seconds=builder_cred_rotation_seconds,
        builder_cred_rotation_stagger_markets=builder_cred_rotation_stagger_markets,
    )


async def redeem_positions_batch(
    condition_ids: list[str],
    private_key: str,
    rpc_url: str,
    chain_id: int = 137,
    ctf_address: str = CTF_COLLATERAL_ADAPTER_ADDRESS,
    collateral_token: str = PUSD_ADDRESS,
    index_sets: list[int] | None = None,
    *,
    use_relayer: bool = True,
    cred_index: int = 0,
    api_key: str | None = None,
    api_secret: str | None = None,
    api_passphrase: str | None = None,
    relayer_url: str = RELAYER_URL,
    builder_cred_rotation_seconds: float = 0.0,
    builder_cred_rotation_stagger_markets: bool = False,
    deposit_wallet_address: str = "",
) -> dict[str, Any]:
    """
    Batch redeem winning CTF positions for multiple resolved markets in one relayer call.

    cred_index rotates which builder credential to start with, so parallel
    redeems each use a different credential (e.g. market 0 → cred 0, market 1 → cred 1).
    When builder_cred_rotation_seconds > 0, the starting index also advances on a time window
    (e.g. hourly) to spread relayer quota across keys. On rate-limit, retries with the next
    credential in the pool.
    """
    if not condition_ids:
        return {"tx_hash": None, "error": "No condition_ids to redeem", "condition_ids": []}

    cred_pool = _load_builder_creds_pool()
    if not cred_pool and use_relayer and api_key and api_secret and api_passphrase:
        cred_pool = [(api_key, api_secret, api_passphrase)]
    elif not cred_pool:
        return {
            "tx_hash": None,
            "error": "Batch redeem requires relayer. Set POLYBOT5MBES_EXECUTION__BUILDER_API_KEY_1..N or api_key/secret/passphrase.",
            "condition_ids": condition_ids,
        }
    elif api_key and api_secret and api_passphrase:
        single = (api_key, api_secret, api_passphrase)
        if single not in cred_pool:
            cred_pool = [single] + cred_pool

    if (private_key or "").strip() and deposit_wallet_address:
        from polybot5m.execution.relayer_creds import validated_builder_cred_pool

        cred_pool = validated_builder_cred_pool(
            cred_pool,
            private_key=private_key,
            chain_id=chain_id,
            relayer_url=relayer_url,
            deposit_wallet_address=deposit_wallet_address,
        )
        if not cred_pool:
            return {
                "tx_hash": None,
                "error": "No authorized builder relayer credentials for redeem",
                "condition_ids": condition_ids,
            }

    n = len(cred_pool)
    wallet_busy_retry_s = 10.0
    max_wallet_busy_retries = 4
    ordered_pool = ordered_builder_cred_pool(
        cred_pool,
        cred_index,
        rotation_seconds=builder_cred_rotation_seconds,
        stagger_markets=builder_cred_rotation_stagger_markets,
    )

    last_error: str | None = None
    for idx, (key, secret, passphrase) in enumerate(ordered_pool):
        for busy_try in range(max_wallet_busy_retries):
            result = await asyncio.to_thread(
                _redeem_batch_via_relayer,
                condition_ids=condition_ids,
                private_key=private_key,
                chain_id=chain_id,
                builder_api_key=key,
                builder_api_secret=secret,
                builder_api_passphrase=passphrase,
                rpc_url=rpc_url,
                relayer_url=relayer_url,
                ctf_address=ctf_address,
                collateral_token=collateral_token,
                index_sets=index_sets,
                deposit_wallet_address=deposit_wallet_address,
            )
            err = result.get("error")
            if not err:
                return result
            last_error = err
            if _is_wallet_busy_error(err) and busy_try < max_wallet_busy_retries - 1:
                print(
                    f"  Relayer wallet busy — retry in {wallet_busy_retry_s:g}s "
                    f"({busy_try + 2}/{max_wallet_busy_retries})",
                )
                await asyncio.sleep(wallet_busy_retry_s)
                continue
            break
        if not last_error:
            return result
        if _is_rate_limit_error(last_error) and idx < n - 1:
            print(f"  Rate limit — retrying with next builder cred ({idx + 2}/{n})")
        else:
            break

    return {
        "tx_hash": None,
        "error": last_error or "Redeem failed",
        "condition_ids": condition_ids,
    }
