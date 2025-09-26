from __future__ import annotations
from typing import Callable, List, Dict, Any
from PIL import ImageDraw, ImageFont
from weatherstream.core.layer import Layer
from weatherstream.icons import pick_icon, find_icon_path

def _font(s):
    try:
        return ImageFont.truetype("assets/fonts/Inter-Regular.ttf", s)
    except Exception:
        return ImageFont.load_default()

class DailyLayer(Layer):
    """
    get_days(): list of dicts:
      { "name": "MON", "high": 90, "low": 74, "unit": "F", "short": "Sunny", "is_day": True }
    """
    def __init__(self, x:int,y:int,w:int,h:int, get_days:Callable[[],List[Dict[str,Any]]], min_interval:float=30.0):
        super().__init__(x,y,w,h,min_interval=min_interval)
        self.get_days = get_days
        self.f_sm = _font(32)
        self.f_big = _font(64)
        self.f_tiny = _font(26)

    def tick(self, now: float):
        draw = ImageDraw.Draw(self.surface)
        draw.rectangle((0,0,*self.surface.size), fill=(32,44,62,235))
        days = self.get_days() or []

        if not days:
            draw.text((12,12),"No data",font=self.f_sm,fill=(255,255,255,255))
            return self._mark_all_dirty_if_changed()

        n = min(7, len(days))
        gutter = 10
        pw = (self.surface.width - gutter*(n-1))//n
        top = 16; bottom = self.surface.height - 16

        for i,day in enumerate(days[:n]):
            x0 = i*(pw+gutter)
            draw.rounded_rectangle((x0, top, x0+pw, bottom), radius=20, fill=(26,38,54,235))
            # title
            draw.text((x0+16, top+20), str(day.get("name","DAY")).upper(), font=self.f_sm, fill=(255,232,150,255))
            # icon
            ip = find_icon_path(pick_icon(day.get("short"), day.get("is_day")))
            if ip:
                try:
                    from PIL import Image
                    icon = Image.open(ip).convert("RGBA").resize((72,72))
                    self.surface.paste(icon, (x0+pw//2-36, top+70), icon)
                except Exception:
                    pass
            # temps
            hi = day.get("high"); lo = day.get("low"); unit = day.get("unit","F")
            hi_txt = "--" if hi is None else str(int(round(hi)))
            lo_txt = "--" if lo is None else str(int(round(lo)))
            draw.text((x0+pw//2-24, top+160), f"{hi_txt}°", font=self.f_big, fill=(255,255,255,255))
            draw.text((x0+pw//2-40, bottom-48), f"LOW {lo_txt}°{unit}", font=self.f_tiny, fill=(215,225,235,255))

        return self._mark_all_dirty_if_changed()
