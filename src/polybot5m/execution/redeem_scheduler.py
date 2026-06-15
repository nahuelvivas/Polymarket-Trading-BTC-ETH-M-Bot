"""Serial staggered redeem queue — one relayer action at a time per deposit wallet."""

from __future__ import annotations

import asyncio
import heapq
import json
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from polybot5m.config import Settings
from polybot5m.execution.deposit_wallet import resolve_deposit_wallet_address
from polybot5m.execution.executor import post_redeem_monitor_orderbooks
from polybot5m.execution.redeem import redeem_positions_batch
from polybot5m.time_utils import format_utc_iso_z

REDEEM_SYMBOL_ORDER = ("btc", "eth")


def symbol_redeem_slot(symbol: str) -> int:
    sym = str(symbol).lower().strip()
    if sym in REDEEM_SYMBOL_ORDER:
        return REDEEM_SYMBOL_ORDER.index(sym)
    return len(REDEEM_SYMBOL_ORDER)


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(order=True)
class _HeapEntry:
    scheduled_at: datetime
    seq: int
    job: Any = field(compare=False)


@dataclass
class RedeemJob:
    condition_id: str
    adapter_address: str
    collateral: str
    symbol: str
    epoch: str
    tag: str
    market_index: int
    epoch_end: datetime
    slug: str
    summary: dict[str, Any]
    export_path: Path | None
    yes_token: str
    no_token: str
    poll_interval: float
    clob_url: str
    paper_trading: bool
    dry_run: bool
    scheduled_at: datetime | None = None
    retry_count: int = 0
    _done: asyncio.Future[None] | None = field(default=None, repr=False)


class RedeemScheduler:
    """
    Queue redeems by scheduled time (epoch_end + redeem_delay + symbol_slot * gap).
    Executes one relayer redeem at a time; failed jobs are re-queued for retry.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._lm = settings.liquidity_maker
        self._exe = settings.execution
        self._heap: list[_HeapEntry] = []
        self._seq = 0
        self._serial = asyncio.Lock()
        self._wake = asyncio.Event()
        self._worker: asyncio.Task[None] | None = None
        self._running = False
        self._pending_count = 0
        self._wallet_addr = ""
        if (self._exe.private_key or "").strip():
            self._wallet_addr = resolve_deposit_wallet_address(
                private_key=self._exe.private_key,
                chain_id=self._exe.chain_id,
                configured_funder=self._exe.funder or "",
            )

    def start(self) -> None:
        if self._worker is None:
            self._running = True
            self._worker = asyncio.create_task(self._worker_loop())

    async def shutdown(self) -> None:
        self._running = False
        self._wake.set()
        if self._worker is not None:
            try:
                await self._worker
            except asyncio.CancelledError:
                pass
            self._worker = None

    @property
    def pending_count(self) -> int:
        return self._pending_count + len(self._heap)

    def _schedule_at(self, job: RedeemJob) -> datetime:
        gap = float(getattr(self._lm, "redeem_per_symbol_gap_seconds", 10.0) or 10.0)
        delay = int(getattr(self._lm, "redeem_delay_seconds", 120) or 120)
        slot = symbol_redeem_slot(job.symbol)
        return job.epoch_end + timedelta(seconds=delay + slot * gap)

    async def schedule(self, job: RedeemJob, *, wait: bool = False) -> None:
        """Enqueue redeem; optionally block until this job finishes."""
        if wait:
            job._done = asyncio.get_running_loop().create_future()
        job.scheduled_at = self._schedule_at(job)
        self._seq += 1
        heapq.heappush(
            self._heap,
            _HeapEntry(job.scheduled_at, self._seq, job),
        )
        self._pending_count += 1
        self.start()
        self._wake.set()
        if wait and job._done is not None:
            await job._done

    async def drain(self) -> None:
        """Wait until all queued and in-flight redeems complete."""
        while self.pending_count > 0 or self._serial.locked():
            self._wake.set()
            await asyncio.sleep(0.25)

    async def _worker_loop(self) -> None:
        while self._running or self._heap:
            if not self._heap:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    if not self._running:
                        break
                    continue
                continue

            entry = self._heap[0]
            now = _utc_now()
            if entry.scheduled_at > now:
                wait_s = min(1.0, (entry.scheduled_at - now).total_seconds())
                if wait_s > 0:
                    await asyncio.sleep(wait_s)
                continue

            heapq.heappop(self._heap)
            job: RedeemJob = entry.job
            self._pending_count = max(0, self._pending_count - 1)

            async with self._serial:
                await self._execute_job(job)

            if job._done is not None and not job._done.done():
                job._done.set_result(None)

        self._running = False

    async def _execute_job(self, job: RedeemJob) -> None:
        tag = job.tag
        summary = job.summary
        lm = self._lm
        exe = self._exe
        settings = self._settings

        summary["phase"] = "REDEEM"
        if job.paper_trading:
            summary["redeem_tx"] = "paper"
            summary["phase"] = "IDLE"
            return

        now = _utc_now()
        if job.scheduled_at and job.scheduled_at > now:
            wait_s = (job.scheduled_at - now).total_seconds()
            print(f"  {tag} REDEEM waiting {int(wait_s)}s (stagger slot={symbol_redeem_slot(job.symbol)})...")
            await asyncio.sleep(wait_s)

        if job.retry_count > 0:
            print(f"  {tag} REDEEM retry #{job.retry_count} (backlog) condition={job.condition_id[:18]}...")

        if job.dry_run:
            summary["redeem_tx"] = "dry_run"
            summary["phase"] = "IDLE"
            return

        redeem_result = await redeem_positions_batch(
            condition_ids=[job.condition_id],
            private_key=exe.private_key,
            rpc_url=exe.rpc_url,
            chain_id=exe.chain_id,
            use_relayer=True,
            cred_index=job.market_index,
            ctf_address=job.adapter_address,
            collateral_token=job.collateral,
            api_key=os.getenv("POLYBOT5MBES_EXECUTION__BUILDER_API_KEY", "") or None,
            api_secret=os.getenv("POLYBOT5MBES_EXECUTION__BUILDER_API_SECRET", "") or None,
            api_passphrase=os.getenv("POLYBOT5MBES_EXECUTION__BUILDER_API_PASSPHRASE", "") or None,
            builder_cred_rotation_seconds=float(getattr(exe, "builder_cred_rotation_seconds", 0.0) or 0.0),
            builder_cred_rotation_stagger_markets=bool(
                getattr(exe, "builder_cred_rotation_stagger_markets", False)
            ),
            deposit_wallet_address=self._wallet_addr or "",
        )

        err = redeem_result.get("error")
        if err:
            summary["error"] = f"Redeem failed: {err}"
            print(f"  {tag} REDEEM ERROR: {err}")
            max_retries = int(getattr(lm, "redeem_max_retries", 5) or 5)
            if job.retry_count < max_retries:
                job.retry_count += 1
                retry_gap = float(getattr(lm, "redeem_retry_delay_seconds", 10.0) or 10.0)
                job.scheduled_at = _utc_now() + timedelta(seconds=retry_gap)
                self._seq += 1
                heapq.heappush(
                    self._heap,
                    _HeapEntry(job.scheduled_at, self._seq, job),
                )
                self._pending_count += 1
                self._wake.set()
                print(
                    f"  {tag} REDEEM re-queued in {retry_gap:g}s "
                    f"(attempt {job.retry_count}/{max_retries})",
                )
        else:
            summary["redeem_tx"] = redeem_result.get("tx_hash")
            print(f"  {tag} REDEEM OK tx={summary['redeem_tx']}")

        post_redeem_s = float(getattr(lm, "post_redeem_monitor_seconds", 0.0) or 0.0)
        if post_redeem_s > 0 and not err:
            await post_redeem_monitor_orderbooks(
                job.clob_url,
                job.yes_token,
                job.no_token,
                post_redeem_s,
                tag,
                job.poll_interval,
            )

        if job.export_path is not None:
            try:
                job.export_path.parent.mkdir(parents=True, exist_ok=True)
                rows: list[dict[str, Any]] = []
                p = job.export_path
                if p.exists() and p.stat().st_size > 0:
                    with open(p) as f:
                        rows = json.load(f)
                rows.append(
                    {
                        "ts_utc": format_utc_iso_z(_utc_now()),
                        "type": "redeem",
                        "symbol": job.symbol,
                        "epoch": job.epoch,
                        "slug": job.slug,
                        "condition_id": job.condition_id,
                        "adapter": job.adapter_address,
                        "tx_hash": summary.get("redeem_tx"),
                        "error": summary.get("error"),
                        "retry_count": job.retry_count,
                    },
                )
                with open(p, "w") as f:
                    json.dump(rows, f, indent=2)
            except Exception as e:
                print(f"  Export write error: {e}")

        summary["phase"] = "IDLE"
