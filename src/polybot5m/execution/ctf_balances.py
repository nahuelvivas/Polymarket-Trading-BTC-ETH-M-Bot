"""Read conditional token balances from the CTF (ERC-1155) on Polygon."""

from __future__ import annotations

import logging

from polybot5m.constants import CTF_ADDRESS

_log = logging.getLogger(__name__)


def rpc_execution_http_connected(rpc_url: str) -> bool:
    """True when Web3 HTTPProvider can connect (False if RPC URL is bad, expired quota, etc.)."""
    rpc = (rpc_url or "").strip()
    if not rpc:
        return False
    try:
        from web3 import Web3
    except ImportError:
        return False
    try:
        w3 = Web3(Web3.HTTPProvider(rpc))
        return bool(w3.is_connected())
    except Exception:
        return False


def fetch_ctf_outcome_balances_shares(
    rpc_url: str,
    wallet: str,
    yes_token_id: str,
    no_token_id: str,
) -> tuple[float, float] | None:
    """
    Return (yes_shares, no_shares) in human units (raw balance / 1e6), or None on error.
    """
    rpc = (rpc_url or "").strip()
    w = (wallet or "").strip()
    if not rpc or not w:
        return None
    try:
        from web3 import Web3
    except ImportError:
        return None

    abi = [
        {
            "inputs": [
                {"name": "account", "type": "address"},
                {"name": "id", "type": "uint256"},
            ],
            "name": "balanceOf",
            "outputs": [{"name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function",
        }
    ]
    try:
        w3 = Web3(Web3.HTTPProvider(rpc))
        if not w3.is_connected():
            return None
        c = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=abi)
        owner = Web3.to_checksum_address(w)
        y = int(c.functions.balanceOf(owner, int(yes_token_id)).call())
        n = int(c.functions.balanceOf(owner, int(no_token_id)).call())
        return (y / 1_000_000.0, n / 1_000_000.0)
    except Exception as e:
        _log.warning("CTF balanceOf failed wallet=%s yes=%s no=%s: %r", w, yes_token_id, no_token_id, e)
        return None
