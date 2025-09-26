from __future__ import annotations
import heapq
import time
from typing import List

from .layer import Layer
from .compositor import Compositor

class Scheduler:
    """Wakes layers on their cadence; presents on any change or CFR deadline."""
    def __init__(self, layers: List[Layer], cfr_hz: int | None = 30):
        self.layers = sorted(layers, key=lambda L: getattr(L, "z", 0))
        self.cfr = int(cfr_hz) if cfr_hz else None
        self.heap: list[tuple[float, int]] = []
        now = time.time()
        for i, _ in enumerate(self.layers):
            heapq.heappush(self.heap, (now, i))
        self.next_cfr = now + (1 / self.cfr) if self.cfr else float("inf")

    def run_forever(self, compositor: Compositor, on_present):
        while True:
            now = time.time()
            wake_at = min(self.heap[0][0], self.next_cfr)
            if now < wake_at:
                time.sleep(max(0.0, wake_at - now))
                now = time.time()

            dirty = []
            while self.heap and self.heap[0][0] <= now:
                _, idx = heapq.heappop(self.heap)
                L = self.layers[idx]
                rects = L.tick(now)
                if getattr(L, "visible", True):
                    for r in rects:
                        dirty.append((L, r))
                heapq.heappush(self.heap, (now + L.min_interval, idx))

            must_cfr = self.cfr and now >= self.next_cfr
            if dirty or must_cfr:
                if dirty:
                    compositor.compose(self.layers)
                    frame = compositor.present()
                else:
                    frame = compositor.front
                on_present(frame)
                if self.cfr:
                    self.next_cfr = now + 1 / self.cfr
