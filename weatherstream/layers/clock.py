# weatherstream/layers/clock.py
from __future__ import annotations

from PIL import ImageDraw, ImageFont

from weatherstream.core.layer import Layer
from weatherstream.utils import now_local


def _font(size: int) -> ImageFont.FreeTypeFont:
    try:
        from pathlib import Path

        here = Path(__file__).resolve()
        for parent in (here.parent, *here.parents[1:4]):
            candidate = parent / "assets" / "fonts" / "Inter-Regular.ttf"
            if candidate.exists():
                return ImageFont.truetype(str(candidate), size)
    except Exception:
        return ImageFont.load_default()


class ClockLayer(Layer):
    """Simple clock/date overlay."""

    name = "clock"

    def __init__(
        self,
        *,
        x: int,
        y: int,
        w: int,
        h: int,
        min_interval: float = 1.0,
        temp_supplier=None,
        scale: float = 1.0,
    ):
        super().__init__(x, y, w, h, min_interval=min_interval, scale=scale)
        self.temp_supplier = temp_supplier
        self.font_time = _font(self.s(72, 12))
        self.font_date = _font(self.s(36, 10))
        self.font_temp = _font(self.s(36, 10))
        self._state: tuple[str, str, str] | None = None

    def _current_temp(self) -> str:
        if callable(self.temp_supplier):
            try:
                value = self.temp_supplier() or ""
            except Exception:
                value = ""
            return str(value).strip()
        return ""

    def tick(self, now: float):
        now_dt = now_local()
        time_str = now_dt.strftime("%I:%M:%S %p").lstrip("0")
        date_str = now_dt.strftime("%A, %B %d").replace(" 0", " ")
        temp_str = self._current_temp()

        state = (time_str, date_str, temp_str)
        if state == self._state:
            return []
        self._state = state

        draw = ImageDraw.Draw(self.surface)
        draw.rectangle((0, 0, self.surface.width, self.surface.height), fill=(0, 0, 0, 0))

        right = self.surface.width - self.s(16)
        time_box = draw.textbbox((0, 0), time_str, font=self.font_time)
        draw.text(
            (right - (time_box[2] - time_box[0]), self.s(0)),
            time_str,
            font=self.font_time,
            fill=(235, 242, 255, 255),
        )

        date_box = draw.textbbox((0, 0), date_str, font=self.font_date)
        draw.text(
            (right - (date_box[2] - date_box[0]), self.s(82)),
            date_str,
            font=self.font_date,
            fill=(210, 220, 230, 255),
        )

        if temp_str:
            temp_box = draw.textbbox((0, 0), temp_str, font=self.font_temp)
            draw.text(
                (right - (temp_box[2] - temp_box[0]), self.s(132)),
                temp_str,
                font=self.font_temp,
                fill=(255, 230, 140, 255),
            )

        return self._mark_all_dirty_if_changed()
