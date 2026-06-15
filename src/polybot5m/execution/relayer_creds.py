"""Builder relayer credential validation (shared by redeem)."""

from __future__ import annotations

import time
from typing import Any

from polybot5m.constants import CTF_COLLATERAL_ADAPTER_ADDRESS, PUSD_ADDRESS, RELAYER_URL
from polybot5m.execution.deposit_wallet import resolve_deposit_wallet_address

_builder_pool_cache: dict[tuple[str, int, str], list[tuple[str, str, str]]] = {}


def is_builder_auth_error(err: str | None) -> bool:
    """Relayer rejected builder API credentials (not a batch revert)."""
    s = (err or "").lower()
    return "status_code=401" in s or "invalid authorization" in s


def _build_relay_client(
    private_key: str,
    chain_id: int,
    builder_api_key: str,
    builder_api_secret: str,
    builder_api_passphrase: str,
    relayer_url: str,
):
    from py_builder_relayer_client.client import RelayClient
    from py_builder_signing_sdk.config import BuilderApiKeyCreds, BuilderConfig

    builder_config = BuilderConfig(
        local_builder_creds=BuilderApiKeyCreds(
            key=builder_api_key,
            secret=builder_api_secret,
            passphrase=builder_api_passphrase,
        )
    )
    return RelayClient(
        relayer_url=relayer_url,
        chain_id=chain_id,
        private_key=private_key,
        builder_config=builder_config,
    )


def _wallet_nonce(client) -> str:
    from py_builder_relayer_client.models import TransactionType

    owner = client.signer.address()
    payload = client.get_nonce(owner, TransactionType.WALLET.value)
    if not payload or payload.get("nonce") is None:
        raise RuntimeError("invalid WALLET nonce from relayer")
    return str(payload["nonce"])


def _submit_deposit_wallet_batch(
    client,
    *,
    wallet_address: str,
    calls: list,
    deadline: str | None = None,
) -> dict[str, Any]:
    from py_builder_relayer_client.models import DepositWalletCall

    if not isinstance(calls[0], DepositWalletCall):
        raise TypeError("calls must be DepositWalletCall instances")
    dl = deadline or str(int(time.time()) + 600)
    try:
        nonce = _wallet_nonce(client)
        response = client.execute_deposit_wallet_batch(
            calls=calls,
            wallet_address=wallet_address,
            nonce=nonce,
            deadline=dl,
        )
        result = response.wait()
        tx_hash = None
        if result:
            tx_hash = result.get("transactionHash") or result.get("transaction_hash")
        if not tx_hash and getattr(response, "transaction_hash", None):
            tx_hash = response.transaction_hash
        if tx_hash:
            return {"tx_hash": tx_hash, "error": None}
        return {"tx_hash": None, "error": "Relayer did not return transaction hash"}
    except Exception as e:
        return {"tx_hash": None, "error": str(e)}


def probe_builder_cred_authorized_for_submit(
    private_key: str,
    chain_id: int,
    builder_api_key: str,
    builder_api_secret: str,
    builder_api_passphrase: str,
    *,
    relayer_url: str,
    wallet_address: str,
) -> bool:
    """True when relayer accepts builder auth for SUBMIT (nonce-only checks are insufficient)."""
    from py_builder_relayer_client.models import DepositWalletCall

    from polybot5m.execution.redeem import _condition_id_to_bytes32, _encode_redeem_calldata

    client = _build_relay_client(
        private_key,
        chain_id,
        builder_api_key,
        builder_api_secret,
        builder_api_passphrase,
        relayer_url,
    )
    probe_condition = "0x" + "11" * 32
    redeem_data = _encode_redeem_calldata(
        PUSD_ADDRESS,
        _condition_id_to_bytes32(probe_condition),
        [1, 2],
    )
    call = DepositWalletCall(
        target=CTF_COLLATERAL_ADAPTER_ADDRESS,
        value="0",
        data=redeem_data,
    )
    result = _submit_deposit_wallet_batch(
        client,
        wallet_address=wallet_address,
        calls=[call],
    )
    return not is_builder_auth_error(result.get("error"))


def validated_builder_cred_pool(
    cred_pool: list[tuple[str, str, str]],
    *,
    private_key: str,
    chain_id: int,
    relayer_url: str = RELAYER_URL,
    deposit_wallet_address: str = "",
) -> list[tuple[str, str, str]]:
    """Drop builder keys that fail relayer SUBMIT auth (rotation may otherwise pick a 401 key)."""
    if not cred_pool:
        return cred_pool
    wallet = resolve_deposit_wallet_address(
        private_key=private_key,
        chain_id=chain_id,
        configured_funder=deposit_wallet_address,
    )
    cache_key = (wallet.lower(), int(chain_id), relayer_url.rstrip("/"))
    cached = _builder_pool_cache.get(cache_key)
    if cached is not None:
        return cached

    valid: list[tuple[str, str, str]] = []
    dropped: list[str] = []
    for key, secret, passphrase in cred_pool:
        if probe_builder_cred_authorized_for_submit(
            private_key,
            chain_id,
            key,
            secret,
            passphrase,
            relayer_url=relayer_url,
            wallet_address=wallet,
        ):
            valid.append((key, secret, passphrase))
        else:
            dropped.append(key[:12] + "...")

    if dropped:
        print(
            "  WARNING: ignoring builder API key(s) with invalid relayer authorization: "
            + ", ".join(dropped)
            + ". Remove them from .env or regenerate at Polymarket Builder.",
            flush=True,
        )
    out = valid if valid else cred_pool
    _builder_pool_cache[cache_key] = out
    return out
