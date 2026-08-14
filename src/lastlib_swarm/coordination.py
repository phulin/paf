from __future__ import annotations

import asyncio
import heapq
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TypedDict

from lastlib_swarm.models import Stage


class PriorityLimiter:
    """A concurrency limiter that grants slots to the highest-priority waiter."""

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.available = capacity
        self._sequence = 0
        self._waiters: list[tuple[float, int, asyncio.Future[None]]] = []

    def _wake(self) -> None:
        while self.available and self._waiters:
            _, _, waiter = heapq.heappop(self._waiters)
            if waiter.done():
                continue
            self.available -= 1
            waiter.set_result(None)

    async def acquire(self, priority: float) -> None:
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[None] = loop.create_future()
        self._sequence += 1
        heapq.heappush(self._waiters, (-priority, self._sequence, waiter))
        self._wake()
        try:
            await waiter
        except asyncio.CancelledError:
            if waiter.done() and not waiter.cancelled():
                self.release()
            else:
                waiter.cancel()
            raise

    def release(self) -> None:
        if self.available >= self.capacity:
            raise RuntimeError("priority limiter released without an acquired slot")
        self.available += 1
        self._wake()

    @asynccontextmanager
    async def slot(self, priority: float) -> AsyncIterator[None]:
        await self.acquire(priority)
        try:
            yield
        finally:
            self.release()


@dataclass
class CoordinatorBuildLease:
    priority: float
    label: str
    stage: Stage
    preemptible: bool
    preempt_requested: asyncio.Event


class CoordinatorBuildSnapshot(TypedDict):
    owner: str
    owner_stage: str
    queued: int
    queued_jobs: list[str]


class CoordinatorBuildQueue:
    """Priority gate acquired only by coordinator-owned Lake builds."""

    def __init__(self) -> None:
        self._sequence = 0
        self._active: CoordinatorBuildLease | None = None
        self._waiters: list[
            tuple[float, int, asyncio.Future[CoordinatorBuildLease], CoordinatorBuildLease]
        ] = []

    def _wake(self) -> None:
        if self._active is not None:
            return
        while self._waiters:
            _, _, future, lease = heapq.heappop(self._waiters)
            if future.done():
                continue
            self._active = lease
            future.set_result(lease)
            return

    async def acquire(
        self,
        *,
        priority: float,
        label: str,
        stage: Stage,
        preemptible: bool = False,
    ) -> CoordinatorBuildLease:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[CoordinatorBuildLease] = loop.create_future()
        lease = CoordinatorBuildLease(
            priority=priority,
            label=label,
            stage=stage,
            preemptible=preemptible,
            preempt_requested=asyncio.Event(),
        )
        self._sequence += 1
        heapq.heappush(self._waiters, (-priority, self._sequence, future, lease))
        if (
            self._active is not None
            and self._active.preemptible
            and priority > self._active.priority
        ):
            self._active.preempt_requested.set()
        self._wake()
        try:
            return await future
        except asyncio.CancelledError:
            if future.done() and not future.cancelled() and self._active is lease:
                self.release(lease)
            else:
                future.cancel()
            raise

    def release(self, lease: CoordinatorBuildLease) -> None:
        if self._active is not lease:
            raise RuntimeError("coordinator build lease released by a non-owner")
        self._active = None
        self._wake()

    def snapshot(self) -> CoordinatorBuildSnapshot:
        queued = [lease for _, _, future, lease in self._waiters if not future.done()]
        return {
            "owner": self._active.label if self._active is not None else "",
            "owner_stage": self._active.stage.value if self._active is not None else "",
            "queued": len(queued),
            "queued_jobs": [
                lease.label for lease in sorted(queued, key=lambda item: -item.priority)
            ],
        }
