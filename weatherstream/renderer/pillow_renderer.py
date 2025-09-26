from __future__ import annotations
from PIL import Image, ImageDraw, ImageFont
import numpy as np
from pathlib import Path
from functools import lru_cache

# ---------- font helpers ----------
def _load_font(preferred: str | None, size: int):
    candidates: list[Path] = []
    if preferred:
        candidates.append(Path(preferred))

    here = Path(__file__).resolve()
    # Try repo assets
    for up in range(1, 7):
        candidates.append(here.parents[up-1] / "assets" / "fonts" / "Inter-Regular.ttf")

    # Common macOS fonts
    candidates += [
        Path("/Library/Fonts/Arial.ttf"),
        Path("/System/Library/Fonts/Supplemental/Arial.ttf"),
        Path("/System/Library/Fonts/Supplemental/Arial Unicode.ttf"),
    ]

    for p in candidates:
        try:
            if p.exists():
                return ImageFont.truetype(str(p), size=size)
        except Exception:
            continue

    print("[WeatherStream] WARNING: No TTF font found; using ImageFont.load_default()")
    return ImageFont.load_default()

# ---------- icon helpers ----------
@lru_cache(maxsize=64)
def _open_icon(path_str: str, size: int) -> Image.Image:
    im = Image.open(path_str).convert("RGBA")
    if im.width != size or im.height != size:
        im = im.resize((size, size), Image.LANCZOS)
    return im

# ---------- canvas ----------
class Canvas:
    def __init__(self, width: int, height: int, font_path: str):
        self.width = width
        self.height = height
        self._bg_color = (10, 16, 26, 255)
        self.font_large = _load_font(font_path, 82)
        self.font = _load_font(font_path, 56)
        self.font_medium = _load_font(font_path, 44)
        self.font_small = _load_font(font_path, 34)
        self.font_tiny = _load_font(font_path, 26)
        self.img: Image.Image | None = None
        self.draw: ImageDraw.ImageDraw | None = None
        self.reset()

    def reset(self) -> None:
        """Clear this surface WITHOUT reallocating; keep it transparent by default."""
        # Fill the existing image with the clear color (no new Image allocations)
        self.img.paste(self._clear_color, (0, 0, self.width, self.height))
        # Rebind the drawing context (cheap)
        self.draw = ImageDraw.Draw(self.img, "RGBA")

    def round_rect(self, xy, radius: int = 24, fill=(20, 28, 40, 255)):
        self.draw.rounded_rectangle(xy, radius=radius, fill=fill)

    def text(
        self,
        xy,
        text: str,
        font=None,
        fill=(235, 242, 255, 255),
        stroke_width: int = 0,
        stroke_fill=(0, 0, 0, 255),
    ):
        self.draw.text(
            xy,
            text,
            font=font or self.font,
            fill=fill,
            stroke_width=stroke_width,
            stroke_fill=stroke_fill,
        )

    def text_size(self, text: str, font=None):
        font = font or self.font
        return self.draw.textbbox((0, 0), text, font=font)

    def wrap_text(self, text: str, font, max_width: int, max_lines: int = 2, ellipsis: bool = True):
        """
        Returns a list of lines that fit max_width. Adds ellipsis on the last line if truncated.
        """
        words = (text or "").split()
        if not words:
            return []
        lines = []
        cur = words[0]
        for w in words[1:]:
            trial = cur + " " + w
            wbox = self.draw.textbbox((0, 0), trial, font=font)
            if (wbox[2] - wbox[0]) <= max_width:
                cur = trial
            else:
                lines.append(cur)
                cur = w
                if len(lines) == max_lines - 1:
                    break
        else:
            lines.append(cur)

        # If we broke early, make the last line fit with ellipsis
        if len(lines) < max_lines:
            if cur not in lines:
                lines.append(cur)
        else:
            if ellipsis and words:
                # ensure last line with ellipsis fits
                while True:
                    bbox = self.draw.textbbox((0, 0), lines[-1] + "…", font=font)
                    if (bbox[2] - bbox[0]) <= max_width or not lines[-1]:
                        lines[-1] = lines[-1] + "…"
                        break
                    lines[-1] = lines[-1].rsplit(" ", 1)[0] if " " in lines[-1] else lines[-1][:-1]
        return lines[:max_lines]

    def text_block(
        self,
        x: int,
        y: int,
        lines: list[str],
        font,
        line_gap: int = 6,
        fill=(235, 242, 255, 255),
        stroke_width: int = 0,
        stroke_fill=(0, 0, 0, 255),
    ):
        yy = y
        for ln in lines:
            self.draw.text(
                (x, yy),
                ln,
                font=font,
                fill=fill,
                stroke_width=stroke_width,
                stroke_fill=stroke_fill,
            )
            box = self.draw.textbbox((0, 0), ln, font=font)
            line_h = (box[3] - box[1])
            yy += line_h + line_gap

    def paste_icon(self, path: str, xy: tuple[int, int], size: int):
        try:
            im = _open_icon(path, size)
            self.img.alpha_composite(im, dest=xy)
        except Exception:
            pass

    def paste_image(self, image: Image.Image, xy: tuple[int, int], size: tuple[int, int] | None = None):
        try:
            im = image.convert("RGBA") if image.mode != "RGBA" else image
            if size and im.size != size:
                im = im.resize(size, Image.LANCZOS)
            self.img.alpha_composite(im, dest=xy)
        except Exception:
            pass

    def to_ndarray(self):
        return np.array(self.img, dtype=np.uint8)

    def to_bytes(self) -> bytes:
        """Return the raw RGBA bytes for streaming without an intermediate ndarray."""
        return self.img.tobytes()
