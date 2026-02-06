from __future__ import annotations
from typing import List, Tuple
from PIL import Image

DirtyRect = Tuple[int, int, int, int]  # x, y, w, h

class Layer:
    """Base class for an on-screen element that owns an offscreen RGBA surface."""
    z: int = 0

    def __init__(self, x: int, y: int, w: int, h: int, min_interval: float = 1.0, scale: float = 1.0):
        self.bounds = (x, y, w, h)
        self.min_interval = max(0.001, float(min_interval))
        self.surface = Image.new("RGBA", (w, h), (0, 0, 0, 0))
        self._last_hash: int | None = None
        self.visible: bool = True
        try:
            self.scale = float(scale or 1.0)
        except (TypeError, ValueError):
            self.scale = 1.0

    def s(self, value: float, minimum: int = 0) -> int:
        scaled = int(round(value * self.scale))
        return max(minimum, scaled)

    def tick(self, now: float) -> List[DirtyRect]:
        """Subclasses: redraw self.surface if needed and return dirty rects (layer-local)."""
        return []

    # Helpers
    def _mark_all_dirty_if_changed(self) -> List[DirtyRect]:
        h = hash(self.surface.tobytes())
        if h != self._last_hash:
            self._last_hash = h
            w, hgt = self.surface.size
            return [(0, 0, w, hgt)]
        return []

    # Visibility helpers ---------------------------------------------------
    def set_visible(self, visible: bool) -> None:
        flag = bool(visible)
        if flag == self.visible:
            return
        self.visible = flag
        if self.visible:
            # Force next tick to mark content dirty so compositor refreshes.
            self._last_hash = None

    def is_visible(self) -> bool:
        return self.visible
