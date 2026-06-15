"""Read pUSD (collateral) balance for the Polymarket deposit wallet on Polygon."""

from __future__ import annotations

from polybot5m.constants import PUSD_ADDRESS

_ERC20_BALANCE_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]


def fetch_usdce_balance_usdc(rpc_url: str, wallet_address: str) -> float | None:
    """Return spendable pUSD balance in human units (6 decimals). None if RPC/query fails."""
    from web3 import Web3

    url = (rpc_url or "").strip()
    w = (wallet_address or "").strip()
    if not url or not w:
        return None
    try:
        w3 = Web3(Web3.HTTPProvider(url))
        if not w3.is_connected():
            return None
        owner = Web3.to_checksum_address(w)
        token = Web3.to_checksum_address(PUSD_ADDRESS)
        c = w3.eth.contract(address=token, abi=_ERC20_BALANCE_ABI)
        raw = int(c.functions.balanceOf(owner).call())
        return raw / 1_000_000.0
    except Exception:
        return None


def clamp_split_allocation_usdc(
    amount_usdc: float,
    *,
    min_usdc: float,
    max_usdc: float | None,
) -> float:
    """Apply min/max bounds and 6-decimal rounding for split notional."""
    a = round(float(amount_usdc), 6)
    a = max(float(min_usdc), a)
    if max_usdc is not None and float(max_usdc) > 0:
        a = min(a, float(max_usdc))
    return round(a, 6)
