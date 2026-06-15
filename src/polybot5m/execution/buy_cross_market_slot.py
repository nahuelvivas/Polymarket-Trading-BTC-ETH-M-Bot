"""Serialize V2 entry buys across parallel markets: one buy attempt per global tick, cooldown between symbols."""

from __future__ import annotations

import asyncio
class CrossMarketBuySlotCoordinator:
    """
    Background task advances ``tick_cycle`` every ``cycle_interval_s``.
    At most one BUY post per tick; after an accepted buy, wait ``cooldown_cycles`` more ticks
    before any market may buy again.
    """

    def __init__(self, *, cycle_interval_s: float, cooldown_cycles: int) -> None:
        self._interval = max(0.05, float(cycle_interval_s))
        self._cooldown = max(0, int(cooldown_cycles))
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self.tick_cycle = 0
        self._next_allowed_tick = 0
        self._posts_this_tick = False

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._tick_loop())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _tick_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self._interval)
            async with self._lock:
                self.tick_cycle += 1
                self._posts_this_tick = False

    async def try_reserve_buy_slot(self) -> tuple[bool, int]:
        """
        If True, caller may POST one BUY; must call ``finish_buy_attempt`` afterward.
        Second return value is ``tick_cycle`` at reservation (for finish).
        """
        async with self._lock:
            t = int(self.tick_cycle)
            if t < self._next_allowed_tick:
                return False, t
            if self._posts_this_tick:
                return False, t
            self._posts_this_tick = True
            return True, t

    async def finish_buy_attempt(self, tick_at_reserve: int, accepted: bool) -> None:
        async with self._lock:
            if accepted:
                self._next_allowed_tick = int(tick_at_reserve) + 1 + self._cooldown
                return
            if int(self.tick_cycle) == int(tick_at_reserve):
                self._posts_this_tick = False
