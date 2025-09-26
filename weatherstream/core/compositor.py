from __future__ import annotations
from typing import List, Tuple
from PIL import Image

# (Layer, (lx,ly,lw,lh))
DirtyRef = Tuple["Layer", Tuple[int, int, int, int]]

class Compositor:
    def __init__(self, w: int, h: int):
        self.w, self.h = w, h
        self.front = Image.new("RGBA", (w, h), (0, 0, 0, 255))
        self.back = Image.new("RGBA", (w, h), (0, 0, 0, 255))

    def compose(self, layers: List["Layer"]) -> None:
        """Rebuild the entire frame front-to-back from the visible layers."""
        self.back.paste((0, 0, 0, 255), (0, 0, self.w, self.h))
        for layer in layers:
            if not getattr(layer, "visible", True):
                continue
            x, y, w, h = layer.bounds
            if w <= 0 or h <= 0:
                continue
            self.back.paste(layer.surface, (x, y), layer.surface)

    def present(self) -> Image.Image:
        self.front, self.back = self.back, self.front
        return self.front
