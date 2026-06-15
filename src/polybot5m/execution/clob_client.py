"""Async wrapper around py-clob-client-v2 (Polymarket CLOB V2 — production since Apr 2026)."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from py_clob_client_v2.client import ClobClient as PyClobClient
from py_clob_client_v2.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    BuilderConfig,
    MarketOrderArgs,
    OrderArgs,
    OrderPayload,
    OrderType,
    PartialCreateOrderOptions,
)
from py_clob_client_v2.order_builder.constants import BUY, SELL

_ZERO_BUILDER_CODE = "0x" + "0" * 64

from polybot5m.constants import CHAIN_ID, CLOB_API_URL, CTF_ADDRESS, SIGNATURE_TYPE_POLY_1271
from polybot5m.execution.clob_balances import official_sync_and_get_conditional_pair

_CTF_ERC1155_BALANCE_ABI = [
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

# Outcome (conditional) token balances from CLOB use 6 decimals (same as on-chain CTF balanceOf).
_CONDITIONAL_BALANCE_DECIMALS = 1_000_000
_MIN_CONDITIONAL_BALANCE_POLL_S = 1.0
_CONDITIONAL_BALANCE_CACHE_MAX_AGE_S = 30.0
_CONDITIONAL_BALANCE_BACKOFF_BASE_S = 5.0
_CONDITIONAL_BALANCE_BACKOFF_MAX_S = 60.0
# Min seconds between CLOB update_balance_allowance(CONDITIONAL) calls (Cloudflare 1015).
_CLOB_CONDITIONAL_UPDATE_MIN_S = 45.0


def _resolve_py_clob_signature_type(signature_type: int) -> Any:
    """Polymarket py-clob-client-v2 examples/keys/signature_types.py."""
    # py-clob-client-v2 required; int fallback if SignatureTypeV2 enum unavailable.
    try:
        from py_clob_client_v2.order_utils.model.signature_type_v2 import SignatureTypeV2

        mapping = {
            0: SignatureTypeV2.EOA,
            1: SignatureTypeV2.POLY_PROXY,
            2: SignatureTypeV2.POLY_GNOSIS_SAFE,
            3: SignatureTypeV2.POLY_1271,
        }
        return mapping.get(int(signature_type), SignatureTypeV2.POLY_1271)
    except ImportError:
        return int(signature_type)


def _order_type_from_str(s: str) -> OrderType:
    u = (s or "GTC").upper()
    if u == "FOK":
        return OrderType.FOK
    if u == "FAK":
        return OrderType.FAK
    if u == "GTD":
        return OrderType.GTD
    return OrderType.GTC


def conditional_balance_raw_from_response(resp: Any) -> int:
    """Parse CONDITIONAL balance from CLOB /balance-allowance (6-decimal raw units)."""
    if not isinstance(resp, dict):
        return 0
    try:
        return int(str(resp.get("balance") or 0))
    except (TypeError, ValueError):
        return 0


def _balance_allowance_dict_to_shares(resp: Any) -> float | None:
    """Parse /balance-allowance JSON; return human shares or None."""
    if not isinstance(resp, dict):
        return None
    raw = resp.get("balance")
    if raw is None:
        return None
    try:
        return int(str(raw)) / float(_CONDITIONAL_BALANCE_DECIMALS)
    except (TypeError, ValueError):
        return None


def parse_collateral_balance_allowance_raw(resp: Any) -> tuple[int, int]:
    """Parse CLOB COLLATERAL balance-allowance (6-decimal raw units)."""
    if not isinstance(resp, dict):
        return 0, 0
    try:
        balance = int(str(resp.get("balance") or 0))
    except (TypeError, ValueError):
        balance = 0
    max_allowance = 0
    allowances = resp.get("allowances")
    if isinstance(allowances, dict):
        for raw in allowances.values():
            try:
                max_allowance = max(max_allowance, int(str(raw)))
            except (TypeError, ValueError):
                continue
    return balance, max_allowance


def read_ctf_position_balance_raw(rpc_url: str, wallet_address: str, token_id: str) -> int:
    """ERC1155 balance on core CTF (fallback when CLOB conditional cache is stale)."""
    from web3 import Web3

    w3 = Web3(Web3.HTTPProvider((rpc_url or "").strip()))
    ctf = w3.eth.contract(
        address=Web3.to_checksum_address(CTF_ADDRESS),
        abi=_CTF_ERC1155_BALANCE_ABI,
    )
    owner = Web3.to_checksum_address(wallet_address)
    position_id = int(str(token_id))
    return int(ctf.functions.balanceOf(owner, position_id).call())


def clob_allowance_for_contract(resp: Any, contract_address: str) -> int:
    """Allowance for one spender from CLOB allowances map (0 if missing)."""
    if not isinstance(resp, dict):
        return 0
    allowances = resp.get("allowances")
    if not isinstance(allowances, dict):
        return 0
    key = (contract_address or "").strip()
    for candidate in (key, key.lower(), key.upper()):
        if candidate in allowances:
            try:
                return int(str(allowances[candidate]))
            except (TypeError, ValueError):
                return 0
    return 0


def _fetch_conditional_outcome_pair_shares_sync(
    client: Any,
    yes_token_id: str,
    no_token_id: str,
    signature_type: int,
) -> tuple[float, float] | None:
    def one(tid: str) -> float | None:
        try:
            p = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=str(tid),
                signature_type=signature_type,
            )
            return _balance_allowance_dict_to_shares(client.get_balance_allowance(p))
        except Exception:
            return None

    y = one(yes_token_id)
    n = one(no_token_id)
    if y is None or n is None:
        return None
    return (float(y), float(n))


class ClobClient:
    """Async-friendly wrapper around py-clob-client-v2."""

    def __init__(
        self,
        private_key: str,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        host: str = CLOB_API_URL,
        chain_id: int = CHAIN_ID,
        signature_type: int = SIGNATURE_TYPE_POLY_1271,
        funder: str = "",
        derive_api_creds: bool = True,
        rpc_url: str = "",
        builder_code: str = "",
    ) -> None:
        private_key = (private_key or "").strip()
        api_key = (api_key or "").strip()
        api_secret = (api_secret or "").strip()
        api_passphrase = (api_passphrase or "").strip()
        funder = (funder or "").strip()
        _host = host.rstrip("/") if host else CLOB_API_URL
        self._signature_type = int(signature_type)
        _sig = _resolve_py_clob_signature_type(signature_type)
        _builder_code = (builder_code or "").strip() or _ZERO_BUILDER_CODE
        self._builder_code = _builder_code
        _builder_config = (
            BuilderConfig(builder_code=_builder_code)
            if _builder_code != _ZERO_BUILDER_CODE
            else None
        )
        self._client = PyClobClient(
            host=_host,
            chain_id=chain_id,
            key=private_key,
            creds=None,
            signature_type=_sig,
            funder=funder or None,
            builder_config=_builder_config,
        )
        has_static_api_creds = bool(api_key and api_secret and api_passphrase)
        derived: Any = None
        derive_error: Exception | None = None

        if derive_api_creds and (private_key or "").strip():
            try:
                derive_fn = getattr(self._client, "create_or_derive_api_key", None)
                if not callable(derive_fn):
                    raise AttributeError("py-clob-client-v2 missing create_or_derive_api_key")
                derived = derive_fn()
            except Exception as e:
                derive_error = e
                derived = None

        if derived is not None:
            self._client.set_api_creds(derived)
        elif has_static_api_creds:
            self._client.set_api_creds(
                ApiCreds(
                    api_key=api_key,
                    api_secret=api_secret,
                    api_passphrase=api_passphrase,
                )
            )
        elif derive_error is not None:
            raise RuntimeError(
                "CLOB API credential derivation failed (check PRIVATE_KEY and network). "
                f"Original error: {derive_error}"
            ) from derive_error
        else:
            raise ValueError(
                "No CLOB API creds available and derivation is disabled. "
                "Set POLYBOT5MBES_EXECUTION__API_KEY/API_SECRET/API_PASSPHRASE "
                "or enable derive_clob_api_creds."
            )

        if has_static_api_creds and derived is not None:
            static_key = api_key.strip()
            derived_key = (getattr(derived, "api_key", "") or "").strip()
            if static_key and derived_key and static_key != derived_key:
                print(
                    "  WARNING: POLYBOT5MBES_EXECUTION__API_KEY in .env does not match wallet-derived "
                    f"CLOB key (env={static_key[:12]}... derived={derived_key[:12]}...). "
                    "Using derived creds for trading and user WebSocket; remove stale API_* from .env.",
                    flush=True,
                )
        creds = getattr(self._client, "creds", None)
        self.api_key = (getattr(creds, "api_key", "") or api_key or "").strip()
        self.api_secret = (getattr(creds, "api_secret", "") or api_secret or "").strip()
        self.api_passphrase = (
            getattr(creds, "api_passphrase", "") or api_passphrase or ""
        ).strip()
        self._cond_balance_lock = asyncio.Lock()
        self._cond_balance_cache: dict[tuple[str, str], tuple[float, float, float]] = {}
        self._cond_balance_last_any_mono = 0.0
        self._cond_balance_backoff_until = 0.0
        self._cond_balance_failures = 0
        self._cond_balance_rate_limited_alerted = False
        self._balance_debug_logged: set[tuple[str, str]] = set()
        self._data_positions_logged: set[tuple[str, str]] = set()
        self._chain_balance_logged: set[tuple[str, str]] = set()
        self._last_clob_conditional_update_mono = 0.0
        self._funder_address = funder
        self._rpc_url = (rpc_url or "").strip()

    def _clob_conditional_update_allowed(self, *, force: bool = False) -> bool:
        if force:
            return True
        return (
            time.monotonic() - self._last_clob_conditional_update_mono
            >= _CLOB_CONDITIONAL_UPDATE_MIN_S
        )

    def _mark_clob_conditional_update(self) -> None:
        self._last_clob_conditional_update_mono = time.monotonic()

    def wallet_address(self) -> str:
        w = (self._funder_address or "").strip()
        if w:
            return w
        builder = getattr(self._client, "builder", None)
        return str(getattr(builder, "funder", None) or "").strip()

    async def _fetch_via_data_api_positions(
        self,
        condition_id: str,
        yes_token_id: str,
        no_token_id: str,
    ) -> tuple[float, float] | None:
        """Portfolio-sized holdings via Polymarket Data API (matches polymarket.com UI)."""
        import aiohttp

        from polybot5m.execution.data_positions import fetch_outcome_positions_shares

        wallet = self.wallet_address()
        market = (condition_id or "").strip()
        if not wallet or not market:
            return None
        try:
            async with aiohttp.ClientSession() as session:
                return await fetch_outcome_positions_shares(
                    session,
                    user_address=wallet,
                    condition_id=market,
                    yes_token_id=yes_token_id,
                    no_token_id=no_token_id,
                    size_threshold=0.0,
                )
        except Exception:
            return None

    async def _fetch_conditional_pair_via_chain(
        self,
        yes_token_id: str,
        no_token_id: str,
        *,
        log: bool = True,
        tag: str = "",
        rpc_url: str | None = None,
        wallet_address: str | None = None,
    ) -> tuple[float, float] | None:
        """ERC-1155 balanceOf on CTF — source of truth after relayer split (deposit wallet)."""
        ru = (rpc_url if rpc_url is not None else self._rpc_url).strip()
        wallet = (wallet_address or self.wallet_address()).strip()
        if not ru or not wallet:
            return None
        from polybot5m.execution.ctf_balances import fetch_ctf_outcome_balances_shares

        return await asyncio.to_thread(
            fetch_ctf_outcome_balances_shares,
            ru,
            wallet,
            yes_token_id,
            no_token_id,
        )

    async def _apply_chain_balance_fallback(
        self,
        yes_token_id: str,
        no_token_id: str,
        yes_v: float,
        no_v: float,
        *,
        log: bool = True,
        tag: str = "",
        rpc_url: str | None = None,
        wallet_address: str | None = None,
    ) -> tuple[float, float]:
        if max(float(yes_v), float(no_v)) > 0:
            return float(yes_v), float(no_v)
        chain = await self._fetch_conditional_pair_via_chain(
            yes_token_id,
            no_token_id,
            log=log,
            tag=tag,
            rpc_url=rpc_url,
            wallet_address=wallet_address,
        )
        if chain is None:
            return float(yes_v), float(no_v)
        cy, cn = float(chain[0]), float(chain[1])
        if max(cy, cn) <= 0:
            return float(yes_v), float(no_v)
        key = (str(yes_token_id), str(no_token_id))
        if key not in self._chain_balance_logged:
            self._chain_balance_logged.add(key)
            p = f"  {tag} " if tag else "  "
            w = (wallet_address or self.wallet_address()).strip()
            print(
                f"{p}[BALANCE] CLOB/Data API shares=0; on-chain CTF "
                f"wallet={w} YES={cy:.4f} NO={cn:.4f}",
                flush=True,
            )
        return cy, cn

    async def _fetch_conditional_pair_via_chain_primary(
        self,
        yes_token_id: str,
        no_token_id: str,
        *,
        rpc_url: str | None = None,
        wallet_address: str | None = None,
    ) -> tuple[float, float] | None:
        """On-chain CTF balanceOf — no CLOB calls (safe for high-frequency monitor polls)."""
        chain = await self._fetch_conditional_pair_via_chain(
            yes_token_id,
            no_token_id,
            log=False,
            rpc_url=rpc_url,
            wallet_address=wallet_address,
        )
        if chain is None:
            return None
        cy, cn = float(chain[0]), float(chain[1])
        if max(cy, cn) <= 0:
            return None
        return cy, cn

    async def _fetch_conditional_pair_via_clob_sync(
        self,
        yes_token_id: str,
        no_token_id: str,
        *,
        condition_id: str = "",
        log: bool = True,
        tag: str = "",
        rpc_url: str | None = None,
        wallet_address: str | None = None,
        sync_update: bool = True,
        force_clob_update: bool = False,
    ) -> tuple[float, float]:
        """
        CLOB balance-allowance (optional update), Data API /positions, then on-chain CTF.
        """
        do_update = sync_update and self._clob_conditional_update_allowed(force=force_clob_update)
        yes_v, no_v, _raw = await asyncio.to_thread(
            official_sync_and_get_conditional_pair,
            self._client,
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            log=log,
            tag=tag,
            sync_update=do_update,
        )
        if do_update:
            self._mark_clob_conditional_update()
        if max(yes_v, no_v) > 0:
            return yes_v, no_v

        data_bal = await self._fetch_via_data_api_positions(
            condition_id,
            yes_token_id,
            no_token_id,
        )
        if data_bal is not None and max(float(data_bal[0]), float(data_bal[1])) > 0:
            key = (str(yes_token_id), str(no_token_id))
            if key not in self._data_positions_logged:
                self._data_positions_logged.add(key)
                p = f"  {tag} " if tag else "  "
                print(
                    f"{p}[BALANCE] CLOB shares=0; Data API /positions "
                    f"user={self.wallet_address()} "
                    f"YES={data_bal[0]:.4f} NO={data_bal[1]:.4f}",
                    flush=True,
                )
            return float(data_bal[0]), float(data_bal[1])

        if log:
            key = (str(yes_token_id), str(no_token_id))
            if key not in self._balance_debug_logged:
                self._balance_debug_logged.add(key)
                p = f"  {tag} " if tag else "  "
                print(
                    f"{p}[BALANCE] CLOB and Data API both 0 "
                    f"funder={self.wallet_address()} sig_type={self._signature_type}",
                    flush=True,
                )
        return await self._apply_chain_balance_fallback(
            yes_token_id,
            no_token_id,
            yes_v,
            no_v,
            log=log,
            tag=tag,
            rpc_url=rpc_url,
            wallet_address=wallet_address,
        )

    async def sync_conditional_balances_after_split(
        self,
        yes_token_id: str,
        no_token_id: str,
        *,
        condition_id: str = "",
        delay_s: float = 3.0,
    ) -> tuple[float, float] | None:
        """Post-split: wait for relayer confirm, then refresh via CLOB SDK + Data API fallback."""
        if delay_s > 0:
            await asyncio.sleep(float(delay_s))
        return await self.fetch_conditional_outcome_balances_shares(
            yes_token_id,
            no_token_id,
            sync=True,
            condition_id=condition_id,
        )

    async def sync_trading_balances(self, *, log: bool = False, tag: str = "") -> None:
        """Official SDK: update_balance_allowance(COLLATERAL)."""
        if log:
            p = f"  {tag} " if tag else "  "
            print(f"{p}[BALANCE] CLOB update_balance_allowance COLLATERAL", flush=True)

        def _run() -> None:
            self._client.update_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )

        await asyncio.to_thread(_run)

    async def get_collateral_balance_allowance_raw(
        self,
        *,
        sync: bool = False,
    ) -> tuple[int, int]:
        """pUSD balance + max trading allowance via Polymarket CLOB API (no Polygon RPC)."""
        if sync:
            await self.sync_trading_balances()

        def _get() -> Any:
            return self._client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )

        resp = await asyncio.to_thread(_get)
        return parse_collateral_balance_allowance_raw(resp)

    async def get_conditional_balance_raw(self, token_id: str, *, sync: bool = False) -> int:
        """Outcome token balance via official get_balance_allowance(CONDITIONAL)."""
        def _run() -> Any:
            if sync:
                self._client.update_balance_allowance(
                    BalanceAllowanceParams(
                        asset_type=AssetType.CONDITIONAL,
                        token_id=str(token_id),
                    )
                )
            return self._client.get_balance_allowance(
                BalanceAllowanceParams(
                    asset_type=AssetType.CONDITIONAL,
                    token_id=str(token_id),
                )
            )

        resp = await asyncio.to_thread(_run)
        return conditional_balance_raw_from_response(resp)

    async def get_mergeable_amount_raw(
        self,
        yes_token_id: str,
        no_token_id: str,
        *,
        sync: bool = True,
        wallet_address: str | None = None,
        rpc_url: str | None = None,
    ) -> tuple[int, int, int]:
        """Return (yes_raw, no_raw, merge_amount_raw) where merge = min(yes, no)."""
        if sync:
            await self.sync_trading_balances()
        # Per-token update_balance_allowance required for CONDITIONAL (collateral sync alone is not enough).
        yes_raw = await self.get_conditional_balance_raw(yes_token_id, sync=sync)
        no_raw = await self.get_conditional_balance_raw(no_token_id, sync=sync)
        merge_raw = min(yes_raw, no_raw)
        if merge_raw > 0:
            return yes_raw, no_raw, merge_raw

        wallet = (wallet_address or "").strip()
        if not wallet:
            builder = getattr(self._client, "builder", None)
            wallet = (getattr(builder, "funder", None) or "") if builder else ""
            wallet = str(wallet).strip()
        ru = (rpc_url or "").strip()
        if not wallet or not ru:
            return yes_raw, no_raw, merge_raw

        try:
            yes_chain, no_chain = await asyncio.gather(
                asyncio.to_thread(
                    read_ctf_position_balance_raw, ru, wallet, yes_token_id
                ),
                asyncio.to_thread(read_ctf_position_balance_raw, ru, wallet, no_token_id),
            )
        except Exception as e:
            print(f"  [merge] on-chain balance fallback failed: {e}", flush=True)
            return yes_raw, no_raw, merge_raw

        merge_chain = min(yes_chain, no_chain)
        if merge_chain > 0:
            print(
                f"  [merge] CLOB reported 0; using on-chain CTF balances "
                f"YES={yes_chain / _CONDITIONAL_BALANCE_DECIMALS:.4f} "
                f"NO={no_chain / _CONDITIONAL_BALANCE_DECIMALS:.4f}",
                flush=True,
            )
            return yes_chain, no_chain, merge_chain
        return yes_raw, no_raw, merge_raw

    async def get_collateral_balance_allowance_response(
        self,
        *,
        sync: bool = False,
    ) -> Any:
        """Raw CLOB /balance-allowance JSON for COLLATERAL."""
        if sync:
            await self.sync_trading_balances()
        return await asyncio.to_thread(
            self._client.get_balance_allowance,
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL),
        )

    async def close(self) -> None:
        pass

    async def get_neg_risk(self, token_id: str) -> bool:
        return await asyncio.to_thread(self._client.get_neg_risk, token_id)

    async def get_fee_rate_bps(self, token_id: str) -> int:
        return await asyncio.to_thread(self._client.get_fee_rate_bps, token_id)

    async def get_tick_size(self, token_id: str) -> str:
        return await asyncio.to_thread(self._client.get_tick_size, token_id)

    async def get_order_book(self, token_id: str) -> Any:
        return await asyncio.to_thread(self._client.get_order_book, token_id)

    def _cached_conditional_pair(
        self,
        yes_token_id: str,
        no_token_id: str,
        *,
        max_age_s: float,
    ) -> tuple[float, float] | None:
        key = (str(yes_token_id), str(no_token_id))
        cached = self._cond_balance_cache.get(key)
        if cached is None:
            return None
        yes_v, no_v, ts_mono = cached
        if time.monotonic() - float(ts_mono) > float(max_age_s):
            return None
        return float(yes_v), float(no_v)

    def _store_conditional_pair(
        self,
        yes_token_id: str,
        no_token_id: str,
        yes_v: float,
        no_v: float,
    ) -> None:
        key = (str(yes_token_id), str(no_token_id))
        self._cond_balance_cache[key] = (float(yes_v), float(no_v), time.monotonic())

    def _register_conditional_balance_failure(self) -> None:
        self._cond_balance_failures += 1
        backoff = min(
            _CONDITIONAL_BALANCE_BACKOFF_MAX_S,
            _CONDITIONAL_BALANCE_BACKOFF_BASE_S * (2 ** max(0, self._cond_balance_failures - 1)),
        )
        self._cond_balance_backoff_until = time.monotonic() + float(backoff)
        if not self._cond_balance_rate_limited_alerted:
            self._cond_balance_rate_limited_alerted = True
            print(
                "  [BALANCE] CLOB conditional balance read failed or rate-limited; "
                f"backing off {backoff:.0f}s (check poll intervals / parallel markets)",
            )

    async def fetch_conditional_outcome_balances_shares(
        self,
        yes_token_id: str,
        no_token_id: str,
        *,
        min_poll_s: float | None = None,
        sync: bool = False,
        condition_id: str = "",
        log_tag: str = "",
        rpc_url: str | None = None,
        wallet_address: str | None = None,
    ) -> tuple[float, float] | None:
        """
        Outcome shares: on-chain CTF (monitor), or CLOB sync (post-split), Data API, chain fallback.
        """
        ru = (rpc_url if rpc_url is not None else self._rpc_url).strip()
        wa = (wallet_address or self.wallet_address()).strip()

        if not sync and ru and wa:
            chain_bal = await self._fetch_conditional_pair_via_chain_primary(
                yes_token_id,
                no_token_id,
                rpc_url=ru,
                wallet_address=wa,
            )
            if chain_bal is not None:
                self._cond_balance_last_any_mono = time.monotonic()
                self._store_conditional_pair(yes_token_id, no_token_id, chain_bal[0], chain_bal[1])
                return chain_bal

        if sync:
            try:
                yes_v, no_v = await self._fetch_conditional_pair_via_clob_sync(
                    yes_token_id,
                    no_token_id,
                    condition_id=condition_id,
                    log=bool(log_tag),
                    tag=log_tag,
                    rpc_url=rpc_url,
                    wallet_address=wallet_address,
                    sync_update=True,
                    force_clob_update=True,
                )
            except Exception:
                self._register_conditional_balance_failure()
                cached = self._cached_conditional_pair(
                    yes_token_id,
                    no_token_id,
                    max_age_s=_CONDITIONAL_BALANCE_CACHE_MAX_AGE_S,
                )
                if cached is not None and max(cached[0], cached[1]) > 0:
                    return cached
                try:
                    yes_v, no_v = await self._apply_chain_balance_fallback(
                        yes_token_id,
                        no_token_id,
                        0.0,
                        0.0,
                        log=bool(log_tag),
                        tag=log_tag,
                        rpc_url=rpc_url,
                        wallet_address=wallet_address,
                    )
                except Exception:
                    return cached
                if max(yes_v, no_v) > 0:
                    self._cond_balance_failures = 0
                    self._cond_balance_rate_limited_alerted = False
                    self._cond_balance_last_any_mono = time.monotonic()
                    self._store_conditional_pair(yes_token_id, no_token_id, yes_v, no_v)
                    return yes_v, no_v
                return cached
            self._cond_balance_failures = 0
            self._cond_balance_rate_limited_alerted = False
            self._cond_balance_last_any_mono = time.monotonic()
            self._store_conditional_pair(yes_token_id, no_token_id, yes_v, no_v)
            return yes_v, no_v

        poll_iv = max(0.05, float(min_poll_s if min_poll_s is not None else _MIN_CONDITIONAL_BALANCE_POLL_S))
        now = time.monotonic()
        if now < self._cond_balance_backoff_until:
            return self._cached_conditional_pair(
                yes_token_id,
                no_token_id,
                max_age_s=_CONDITIONAL_BALANCE_CACHE_MAX_AGE_S,
            )
        cached = self._cached_conditional_pair(
            yes_token_id,
            no_token_id,
            max_age_s=poll_iv,
        )
        if cached is not None and (now - self._cond_balance_last_any_mono) < poll_iv:
            return cached

        async with self._cond_balance_lock:
            now = time.monotonic()
            if now < self._cond_balance_backoff_until:
                return self._cached_conditional_pair(
                    yes_token_id,
                    no_token_id,
                    max_age_s=_CONDITIONAL_BALANCE_CACHE_MAX_AGE_S,
                )
            cached = self._cached_conditional_pair(
                yes_token_id,
                no_token_id,
                max_age_s=poll_iv,
            )
            if cached is not None and (now - self._cond_balance_last_any_mono) < poll_iv:
                return cached

            if ru and wa:
                chain_bal = await self._fetch_conditional_pair_via_chain_primary(
                    yes_token_id,
                    no_token_id,
                    rpc_url=ru,
                    wallet_address=wa,
                )
                if chain_bal is not None:
                    self._cond_balance_failures = 0
                    self._cond_balance_last_any_mono = time.monotonic()
                    self._store_conditional_pair(yes_token_id, no_token_id, chain_bal[0], chain_bal[1])
                    return chain_bal

            if now < self._cond_balance_backoff_until:
                return self._cached_conditional_pair(
                    yes_token_id,
                    no_token_id,
                    max_age_s=_CONDITIONAL_BALANCE_CACHE_MAX_AGE_S,
                )

            try:
                yes_v, no_v = await self._fetch_conditional_pair_via_clob_sync(
                    yes_token_id,
                    no_token_id,
                    condition_id=condition_id,
                    log=False,
                    rpc_url=rpc_url,
                    wallet_address=wallet_address,
                    sync_update=False,
                )
            except Exception:
                stale = self._cached_conditional_pair(
                    yes_token_id,
                    no_token_id,
                    max_age_s=_CONDITIONAL_BALANCE_CACHE_MAX_AGE_S,
                )
                self._register_conditional_balance_failure()
                if stale is not None and max(stale[0], stale[1]) > 0:
                    return stale
                try:
                    yes_v, no_v = await self._apply_chain_balance_fallback(
                        yes_token_id,
                        no_token_id,
                        0.0,
                        0.0,
                        log=False,
                        rpc_url=rpc_url,
                        wallet_address=wallet_address,
                    )
                except Exception:
                    return stale
                if max(yes_v, no_v) > 0:
                    self._cond_balance_last_any_mono = time.monotonic()
                    self._store_conditional_pair(yes_token_id, no_token_id, yes_v, no_v)
                    return yes_v, no_v
                return stale

            self._cond_balance_failures = 0
            self._cond_balance_rate_limited_alerted = False
            self._cond_balance_last_any_mono = time.monotonic()
            self._store_conditional_pair(yes_token_id, no_token_id, yes_v, no_v)
            return yes_v, no_v

    def create_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        neg_risk: bool = False,
        tick_size: str | None = None,
        expiration: str | None = None,
        builder_code: str | None = None,
    ) -> Any:
        exp_int = 0
        if expiration is not None and str(expiration).strip() not in ("", "0"):
            exp_int = int(expiration)
        bc = (builder_code or self._builder_code or _ZERO_BUILDER_CODE).strip()
        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=BUY if side.upper() == "BUY" else SELL,
            expiration=exp_int,
            builder_code=bc,
        )
        options = PartialCreateOrderOptions(
            tick_size=tick_size if tick_size else None,
            neg_risk=neg_risk if neg_risk else None,
        )
        return self._client.create_order(order_args, options)

    def create_market_order(
        self,
        token_id: str,
        amount: float,
        side: str,
        order_type: str = "FAK",
        *,
        price: float = 0.0,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
        user_usdc_balance: float = 0.0,
        builder_code: str | None = None,
    ) -> Any:
        """Sign a market order (amount = pUSD for BUY, shares for SELL). V2: fees set at match time."""
        ot = _order_type_from_str(order_type)
        bc = (builder_code or self._builder_code or _ZERO_BUILDER_CODE).strip()
        mo = MarketOrderArgs(
            token_id=token_id,
            amount=float(amount),
            side=BUY if side.upper() == "BUY" else SELL,
            order_type=ot,
            price=float(price),
            user_usdc_balance=float(user_usdc_balance or 0),
            builder_code=bc,
        )
        options = PartialCreateOrderOptions(
            tick_size=tick_size if tick_size else None,
            neg_risk=neg_risk if neg_risk else None,
        )
        return self._client.create_market_order(mo, options)

    async def post_order(
        self,
        signed_order: Any,
        order_type: str = "FOK",
    ) -> dict[str, Any]:
        ot = _order_type_from_str(order_type)
        return await asyncio.to_thread(
            self._client.post_order,
            signed_order,
            ot,
            False,
        )

    async def cancel_order(self, order_id: str) -> Any:
        """Best-effort cancel by order id."""
        oid = (order_id or "").strip()
        if not oid:
            return None
        c = self._client
        cancel_v2 = getattr(c, "cancel_order", None)
        if callable(cancel_v2):
            return await asyncio.to_thread(cancel_v2, OrderPayload(orderID=oid))
        cancel_v1 = getattr(c, "cancel", None)
        if callable(cancel_v1):
            return await asyncio.to_thread(cancel_v1, oid)
        cancel_many = getattr(c, "cancel_orders", None)
        if callable(cancel_many):
            return await asyncio.to_thread(cancel_many, [oid])
        raise RuntimeError("ClobClient has no cancel_order/cancel/cancel_orders method")
