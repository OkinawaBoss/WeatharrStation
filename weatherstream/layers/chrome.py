from __future__ import annotations
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


class ChromeLayer(Layer):
    """Static background chrome (header + ticker tray)."""

    name = "chrome"

    def __init__(
        self,
        *,
        width: int,
        height: int,
        location_name: str,
        title: str = "Weatharr Station",
        min_interval: float = 60.0,
        scale: float = 1.0,
    ):
        super().__init__(0, 0, width, height, min_interval=min_interval, scale=scale)
        self.location = location_name
        self.title = title
        self.font_large = _font(self.s(68, 12))
        self.font_small = _font(self.s(42, 10))
        self._logo: Image.Image | None = None

    def _load_logo(self) -> None:
        if self._logo is not None:
            return
        here = Path(__file__).resolve()
        logo_path = None
        for parent in (here.parent, *here.parents[1:4]):
            candidate = parent / "assets" / "icons" / "NOAA_logo.png"
            if candidate.exists():
                logo_path = candidate
                break
        if not logo_path:
            self._logo = None
            return
        try:
            self._logo = Image.open(logo_path).convert("RGBA")
        except Exception:
            self._logo = None

    def tick(self, now: float):
        draw = ImageDraw.Draw(self.surface)

        # Background fill
        draw.rectangle((0, 0, self.surface.width, self.surface.height), fill=(12, 16, 22, 255))

        # Header bar
        header_bottom = self.s(220, 1)
        draw.rectangle((0, 0, self.surface.width, header_bottom), fill=(20, 28, 40, 235))

        # Title + location
        draw.text((self.s(64), self.s(52)), self.title, font=self.font_large, fill=(235, 242, 255, 255))
        draw.text((self.s(64), self.s(120)), self.location, font=self.font_small, fill=(210, 220, 230, 255))

        # NOAA logo centered
        self._load_logo()
        if self._logo:
            size = self.s(108, 1)
            try:
                logo = self._logo.resize((size, size), Image.LANCZOS)
            except Exception:
                logo = self._logo
            lx = self.surface.width // 2 - logo.width // 2
            ly = self.s(56)
            self.surface.paste(logo, (lx, ly), logo)

        # Ticker tray accent
        tray_h = self.s(64, 1)
        tray_rect = (
            self.s(48),
            self.surface.height - tray_h - self.s(20),
            self.surface.width - self.s(48),
            self.surface.height - self.s(20),
        )
        draw.rounded_rectangle(tray_rect, radius=self.s(24, 1), fill=(16, 24, 40, 235))

        return self._mark_all_dirty_if_changed()
