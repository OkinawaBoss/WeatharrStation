from __future__ import annotations
from typing import Tuple

Rect = Tuple[int, int, int, int]  # x, y, w, h

def intersect(a: Rect, b: Rect) -> Rect | None:
    ax, ay, aw, ah = a
    bx, by, bw, bh = b
    x1, y1 = max(ax, bx), max(ay, by)
    x2, y2 = min(ax + aw, bx + bw), min(ay + ah, by + bh)
    if x2 <= x1 or y2 <= y1:
        return None
    return (x1, y1, x2 - x1, y2 - y1)
