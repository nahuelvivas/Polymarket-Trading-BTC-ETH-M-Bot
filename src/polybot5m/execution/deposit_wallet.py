"""Deposit wallet helpers for Polymarket relayer WALLET-CREATE / WALLET batches."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from eth_abi import encode
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak, to_bytes, to_checksum_address

from polybot5m.constants import (
    DEPOSIT_WALLET_FACTORIES,
    DEPOSIT_WALLET_IMPLEMENTATIONS,
)

try:
    from py_builder_relayer_client.http_helpers.helpers import POST, get, post
    from py_builder_signing_sdk.config import BuilderApiKeyCreds, BuilderConfig
except ImportError:  # pragma: no cover - optional relayer extra
    POST = "POST"
    get = None
    post = None
    BuilderApiKeyCreds = None
    BuilderConfig = None

_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
_WALLET_TX_TYPE = "WALLET"
_WALLET_CREATE_TX_TYPE = "WALLET-CREATE"
_DEPOSIT_WALLET_DOMAIN_NAME = "DepositWallet"
_DEPOSIT_WALLET_DOMAIN_VERSION = "1"
_ERC1967_CONST1 = "0xcc3735a920a3ca505d382bbc545af43d6000803e6038573d6000fd5b3d6000f3"
_ERC1967_CONST2 = "0x5155f3363d3d373d3d363d7f360894a13ba1a3210667c828492db98dca3e2076"
_ERC1967_PREFIX = 0x61003D3D8160233D3973
_TERMINAL_RELAYER_STATES = {"STATE_MINED", "STATE_CONFIRMED"}
_FAILED_RELAYER_STATE = "STATE_FAILED"
_DEPOSIT_WALLET_TYPES = {
    "EIP712Domain": [
        {"name": "name", "type": "string"},
        {"name": "version", "type": "string"},
        {"name": "chainId", "type": "uint256"},
        {"name": "verifyingContract", "type": "address"},
    ],
    "Call": [
        {"name": "target", "type": "address"},
        {"name": "value", "type": "uint256"},
        {"name": "data", "type": "bytes"},
    ],
    "Batch": [
        {"name": "wallet", "type": "address"},
        {"name": "nonce", "type": "uint256"},
        {"name": "deadline", "type": "uint256"},
        {"name": "calls", "type": "Call[]"},
    ],
}


@dataclass
class DepositWalletCall:
    target: str
    value: str
    data: str

    def to_dict(self) -> dict[str, str]:
        return {
            "target": self.target,
            "value": self.value,
            "data": self.data,
        }


class _RelayerSubmitResponse:
    def __init__(self, client: _DepositWalletRelayerClient, transaction_id: str, transaction_hash: str | None):
        self.client = client
        self.transaction_id = transaction_id
        self.transaction_hash = transaction_hash

    def wait(self, *, max_polls: int = 30, poll_frequency_ms: int = 2000) -> dict[str, Any] | None:
        if not self.transaction_id:
            return None
        for _ in range(max_polls):
            transactions = self.client.get_transaction(self.transaction_id)
            if transactions:
                txn = transactions[0]
                state = txn.get("state")
                if state in _TERMINAL_RELAYER_STATES:
                    return txn
                if state == _FAILED_RELAYER_STATE:
                    return None
            time.sleep(poll_frequency_ms / 1000)
        return None


class _DepositWalletRelayerClient:
    def __init__(
        self,
        *,
        relayer_url: str,
        chain_id: int,
        private_key: str,
        builder_api_key: str,
        builder_api_secret: str,
        builder_api_passphrase: str,
    ) -> None:
        _require_relayer_deps()
        self.relayer_url = relayer_url[:-1] if relayer_url.endswith("/") else relayer_url
        self.chain_id = chain_id
        self.private_key = (private_key or "").strip()
        self.owner = _owner_address(self.private_key)
        self.factory = DEPOSIT_WALLET_FACTORIES.get(chain_id, "")
        if not self.factory:
            raise ValueError(f"deposit wallet factory is not configured for chain_id={chain_id}")
        self.builder_config = BuilderConfig(
            local_builder_creds=BuilderApiKeyCreds(
                key=builder_api_key,
                secret=builder_api_secret,
                passphrase=builder_api_passphrase,
            )
        )

    def get_transaction(self, transaction_id: str) -> list[dict[str, Any]]:
        payload = get(f"{self.relayer_url}/transaction?id={transaction_id}")
        if isinstance(payload, list):
            return payload
        return []

    def _post_submit(self, body: dict[str, Any]) -> _RelayerSubmitResponse:
        headers = self.builder_config.generate_builder_headers(POST, "/submit", str(body))
        if headers is None:
            raise RuntimeError("could not generate builder relayer headers")
        resp = post(
            f"{self.relayer_url}/submit",
            headers=headers.to_dict(),
            data=body,
        )
        return _RelayerSubmitResponse(
            self,
            transaction_id=str(resp.get("transactionID") or ""),
            transaction_hash=resp.get("transactionHash"),
        )

    def is_deployed(self, wallet_address: str) -> bool:
        payload = get(
            f"{self.relayer_url}/deployed?address={to_checksum_address(wallet_address)}&type={_WALLET_TX_TYPE}"
        )
        return bool(payload and payload.get("deployed"))

    def get_wallet_nonce(self) -> str:
        payload = get(f"{self.relayer_url}/nonce?address={self.owner}&type={_WALLET_TX_TYPE}")
        if not payload or payload.get("nonce") is None:
            raise RuntimeError("invalid WALLET nonce payload received from relayer")
        return str(payload["nonce"])

    def deploy_deposit_wallet(self) -> _RelayerSubmitResponse:
        return self._post_submit(
            {
                "type": _WALLET_CREATE_TX_TYPE,
                "from": self.owner,
                "to": to_checksum_address(self.factory),
            }
        )

    def execute_deposit_wallet_batch(
        self,
        *,
        wallet_address: str,
        calls: list[DepositWalletCall],
        nonce: str,
        deadline: str,
    ) -> _RelayerSubmitResponse:
        wallet = to_checksum_address(wallet_address)
        signature = _sign_deposit_wallet_batch(
            private_key=self.private_key,
            chain_id=self.chain_id,
            wallet_address=wallet,
            nonce=nonce,
            deadline=deadline,
            calls=calls,
        )
        return self._post_submit(
            {
                "type": _WALLET_TX_TYPE,
                "from": self.owner,
                "to": to_checksum_address(self.factory),
                "nonce": str(nonce),
                "signature": signature,
                "depositWalletParams": {
                    "depositWallet": wallet,
                    "deadline": str(deadline),
                    "calls": [call.to_dict() for call in calls],
                },
            }
        )


def _require_relayer_deps() -> None:
    if get is None or post is None or BuilderConfig is None:
        raise ImportError(
            "Deposit wallet relayer support requires py-builder-relayer-client and "
            "py-builder-signing-sdk. Install with: pip install -e '.[relayer]'"
        )


def _owner_address(private_key: str) -> str:
    key = (private_key or "").strip()
    if not key:
        raise ValueError("private_key is required")
    return to_checksum_address(Account.from_key(key).address)


def _is_zero_address(address: str) -> bool:
    return to_checksum_address(address) == to_checksum_address(_ZERO_ADDRESS)


def _get_create2_address(bytecode_hash: str, from_address: str, salt: bytes) -> str:
    bytecode_hash_bytes = to_bytes(hexstr=bytecode_hash.removeprefix("0x"))
    from_address_bytes = to_bytes(hexstr=from_address.removeprefix("0x"))
    address_hash = keccak(b"\xff" + from_address_bytes + salt + bytecode_hash_bytes)
    return to_checksum_address(address_hash[-20:].hex())


def _init_code_hash_erc1967(implementation: str, args: bytes) -> str:
    implementation = to_checksum_address(implementation)
    combined = _ERC1967_PREFIX + (len(args) << 56)
    init_code = (
        combined.to_bytes(10, "big")
        + to_bytes(hexstr=implementation)
        + to_bytes(hexstr="0x6009")
        + to_bytes(hexstr=_ERC1967_CONST2)
        + to_bytes(hexstr=_ERC1967_CONST1)
        + args
    )
    return "0x" + keccak(init_code).hex()


def _sign_deposit_wallet_batch(
    *,
    private_key: str,
    chain_id: int,
    wallet_address: str,
    nonce: str,
    deadline: str,
    calls: list[DepositWalletCall],
) -> str:
    full_message = {
        "primaryType": "Batch",
        "types": _DEPOSIT_WALLET_TYPES,
        "domain": {
            "name": _DEPOSIT_WALLET_DOMAIN_NAME,
            "version": _DEPOSIT_WALLET_DOMAIN_VERSION,
            "chainId": chain_id,
            "verifyingContract": wallet_address,
        },
        "message": {
            "wallet": wallet_address,
            "nonce": int(nonce),
            "deadline": int(deadline),
            "calls": [
                {
                    "target": call.target,
                    "value": int(call.value),
                    "data": call.data,
                }
                for call in calls
            ],
        },
    }
    signed = Account.sign_message(
        encode_typed_data(full_message=full_message),
        private_key=private_key,
    )
    return "0x" + signed.signature.hex()


def derive_deposit_wallet_address(*, private_key: str, chain_id: int) -> str:
    """Return the deterministic deposit wallet for the owner EOA."""
    owner = _owner_address(private_key)
    factory = DEPOSIT_WALLET_FACTORIES.get(chain_id, "")
    implementation = DEPOSIT_WALLET_IMPLEMENTATIONS.get(chain_id, "")
    if not factory or not implementation:
        raise ValueError(f"deposit wallet contracts are not configured for chain_id={chain_id}")

    wallet_id = to_bytes(hexstr=owner).rjust(32, b"\x00")
    args = encode(["address", "bytes32"], [to_checksum_address(factory), wallet_id])
    salt = keccak(args)
    bytecode_hash = _init_code_hash_erc1967(implementation, args)
    return _get_create2_address(bytecode_hash, factory, salt)


def resolve_deposit_wallet_address(
    *,
    private_key: str,
    chain_id: int,
    configured_funder: str = "",
) -> str:
    """
    Resolve the deposit wallet used as CLOB funder and on-chain inventory holder.

    Uses POLYBOT5MBES_EXECUTION__FUNDER when set to a non-zero address; otherwise derives
    the wallet from PRIVATE_KEY.
    """
    configured = (configured_funder or "").strip()
    if configured and not _is_zero_address(configured):
        return to_checksum_address(configured)
    return derive_deposit_wallet_address(private_key=private_key, chain_id=chain_id)


class DepositWalletRelayer:
    """Relayer client for deposit-wallet WALLET-CREATE / WALLET batches."""

    def __init__(
        self,
        *,
        relayer_url: str,
        chain_id: int,
        private_key: str,
        builder_api_key: str,
        builder_api_secret: str,
        builder_api_passphrase: str,
    ) -> None:
        self._client = _DepositWalletRelayerClient(
            relayer_url=relayer_url,
            chain_id=chain_id,
            private_key=private_key,
            builder_api_key=builder_api_key,
            builder_api_secret=builder_api_secret,
            builder_api_passphrase=builder_api_passphrase,
        )

    def is_deployed(self, wallet_address: str) -> bool:
        return self._client.is_deployed(wallet_address)

    def get_wallet_nonce(self) -> str:
        return self._client.get_wallet_nonce()

    def execute_deposit_wallet_batch(
        self,
        *,
        wallet_address: str,
        calls: list[DepositWalletCall],
        nonce: str,
        deadline: str,
    ) -> _RelayerSubmitResponse:
        return self._client.execute_deposit_wallet_batch(
            wallet_address=wallet_address,
            calls=calls,
            nonce=nonce,
            deadline=deadline,
        )


def ensure_deposit_wallet_deployed(
    *,
    relayer_url: str,
    chain_id: int,
    private_key: str,
    builder_api_key: str,
    builder_api_secret: str,
    builder_api_passphrase: str,
    wallet_address: str,
) -> str | None:
    """
    Deploy the deposit wallet when missing.

    Returns the relayer transaction hash when a deploy was submitted; otherwise None.
    """
    relayer = DepositWalletRelayer(
        relayer_url=relayer_url,
        chain_id=chain_id,
        private_key=private_key,
        builder_api_key=builder_api_key,
        builder_api_secret=builder_api_secret,
        builder_api_passphrase=builder_api_passphrase,
    )
    wallet = to_checksum_address(wallet_address)
    if relayer.is_deployed(wallet):
        return None

    response = relayer._client.deploy_deposit_wallet()
    result = response.wait()
    if result:
        return result.get("transactionHash") or result.get("transaction_hash")
    return response.transaction_hash


__all__ = [
    "DepositWalletCall",
    "DepositWalletRelayer",
    "derive_deposit_wallet_address",
    "ensure_deposit_wallet_deployed",
    "resolve_deposit_wallet_address",
]
