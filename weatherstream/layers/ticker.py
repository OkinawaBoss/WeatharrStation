from __future__ import annotations
from PIL import Image, ImageDraw, ImageFont

from weatherstream.core.layer import Layer

def _font(size=36):
    try:
        return ImageFont.truetype("assets/fonts/Inter-Regular.ttf", size)
    except Exception:
        return ImageFont.load_default()

class TickerLayer(Layer):
    def __init__(self, x:int, y:int, w:int, h:int, min_interval:float, px_per_sec:int, get_text, scale: float = 1.0):
        super().__init__(x, y, w, h, min_interval=min_interval, scale=scale)
        self.speed = float(px_per_sec)
        self.get_text = get_text
        self._font = _font(self.s(24, 10))
        self._strip: Image.Image | None = None
        self._offset: float = 0.0
        self._last_text: str = ""

    def _ensure_strip(self):
        text = (self.get_text() or "").strip()
        if not text:
            text = " "
        if text == self._last_text and self._strip is not None:
            return
        self._last_text = text

        spacer = "    •    "
        long_text = (text + spacer) * 8
        dummy = Image.new("RGBA", (1, 1), (0, 0, 0, 0))
        d = ImageDraw.Draw(dummy)
        bbox = d.textbbox((0, 0), long_text, font=self._font)
        text_w = max(1, bbox[2] - bbox[0])
        text_h = max(1, bbox[3] - bbox[1])
        tmp = Image.new("RGBA", (text_w, self.bounds[3]), (0,0,0,0))
        d = ImageDraw.Draw(tmp)
        y = max(0, (self.bounds[3] - text_h) // 2)
        d.text((-bbox[0], y - bbox[1]), long_text, font=self._font, fill=(255,255,255,255))
        self._strip = tmp
        self._offset = 0.0

    def tick(self, now: float):
        self._ensure_strip()
        if self._strip is None:
            return []

        # Clear
        self.surface.paste((0,0,0,180), (0,0,*self.surface.size))
        w, h = self.surface.size
        x0 = int(self._offset) % self._strip.width
        part1 = self._strip.crop((x0, 0, min(x0+w, self._strip.width), h))
        self.surface.paste(part1, (0,0), part1)
        if part1.width < w:
            part2 = self._strip.crop((0,0, w - part1.width, h))
            self.surface.paste(part2, (part1.width,0), part2)

        self._offset += self.speed * self.min_interval
        return self._mark_all_dirty_if_changed()
