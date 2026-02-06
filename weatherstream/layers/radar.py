from __future__ import annotations
from collections import deque
from typing import Callable, List, Tuple
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from weatherstream.core.layer import Layer


def _font(size: int) -> ImageFont.FreeTypeFont:
    try:
        here = Path(__file__).resolve()
        for parent in (here.parent, *here.parents[1:4]):
            candidate = parent / "assets" / "fonts" / "Inter-Regular.ttf"
            if candidate.exists():
                return ImageFont.truetype(str(candidate), size)
    except Exception:
        return ImageFont.load_default()


class RadarLayer(Layer):
    """
    A cheap radar animation layer.
    - Use `ingest_frame(Image.Image)` to add frames (they are pre-scaled once).
    - Optionally provide `get_new_frames()` callable that returns a list of PIL images; we ingest them when present.
    - min_interval controls playback rate (e.g., 0.1s => 10 FPS).
    """
    def __init__(self,x:int,y:int,w:int,h:int,min_interval:float=0.1, get_new_frames:Callable[[],List[Tuple[Image.Image, str]]] | None=None, frame_hold:int=3, scale: float = 1.0):
        super().__init__(x,y,w,h,min_interval=min_interval, scale=scale)
        self.frames: deque[Image.Image] = deque(maxlen=12)
        self.labels: deque[str] = deque(maxlen=12)
        self.idx = 0
        self.get_new_frames = get_new_frames
        self.frame_hold = max(1, int(frame_hold))
        self._hold_counter = 0
        self.font = _font(self.s(32, 10))

    def ingest_frame(self, img: Image.Image, label: str | None = ""):
        if img is None:
            return
        try:
            scaled = img.convert("RGBA").resize((self.bounds[2], self.bounds[3]), Image.BILINEAR)
            self.frames.append(scaled)
            self.labels.append(label or "")
        except Exception:
            pass

    def tick(self, now: float):
        # Pull any new frames from supplier
        if self.get_new_frames:
            try:
                for item in self.get_new_frames() or []:
                    if isinstance(item, tuple) and len(item) >= 1:
                        img = item[0]
                        label = item[1] if len(item) > 1 else ""
                        self.ingest_frame(img, label)
                    else:
                        self.ingest_frame(item, "")
            except Exception:
                pass

        if not self.frames:
            # simple blank background
            self.surface.paste((24,32,44,235), (0,0,*self.surface.size))
            return self._mark_all_dirty_if_changed()

        frame = self.frames[self.idx % len(self.frames)]
        self.surface.paste(frame, (0,0))

        label = ""
        if self.labels:
            label = self.labels[self.idx % len(self.labels)]
        if label:
            draw = ImageDraw.Draw(self.surface)
            bbox = draw.textbbox((0, 0), label, font=self.font)
            text_w = bbox[2] - bbox[0]
            text_h = bbox[3] - bbox[1]
            pad_x = self.s(18, 1)
            pad_y = self.s(12, 1)
            x = max(self.s(16, 1), self.surface.width - text_w - pad_x * 2)
            y = max(self.s(16, 1), self.surface.height - text_h - pad_y * 2)
            draw.rectangle((x - pad_x, y - pad_y, x + text_w + pad_x, y + text_h + pad_y), fill=(8, 12, 24, 170))
            draw.text((x, y), label, font=self.font, fill=(235, 242, 255, 255))

        self._hold_counter += 1
        if self._hold_counter >= self.frame_hold:
            self._hold_counter = 0
            self.idx += 1
        return self._mark_all_dirty_if_changed()
