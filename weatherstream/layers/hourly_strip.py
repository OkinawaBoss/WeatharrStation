from __future__ import annotations
from typing import Callable, List, Dict, Any
from PIL import ImageDraw, ImageFont, Image
from weatherstream.core.layer import Layer
from weatherstream.icons import pick_icon, find_icon_path

def _font(s): 
    try: return ImageFont.truetype("assets/fonts/Inter-Regular.ttf", s)
    except Exception: return ImageFont.load_default()

class HourlyStripLayer(Layer):
    """
    get_periods() -> list of up to 12 dicts:
      { "temperature": int|None, "unit": "F", "prob": int|None, "label": "14:00",
        "short": "Sunny", "is_day": bool }
    """
    def __init__(self,x:int,y:int,w:int,h:int,get_periods:Callable[[],List[Dict[str,Any]]],min_interval:float=30.0, scale: float = 1.0):
        super().__init__(x,y,w,h,min_interval=min_interval, scale=scale)
        self.get_periods=get_periods
        self.f_sm=_font(self.s(20, 8)); self.f_tiny=_font(self.s(14, 7))

    def tick(self, now: float):
        draw=ImageDraw.Draw(self.surface)
        draw.rectangle((0,0,*self.surface.size), fill=(24,32,44,235))

        periods=self.get_periods() or []
        if not periods:
            draw.text((self.s(12), self.s(12)),"No data",font=self.f_sm,fill=(255,255,255,255))
            return self._mark_all_dirty_if_changed()

        left=self.s(12, 1); top=self.s(8, 1)
        col_w=max(1,(self.surface.width-2*left)//max(1,len(periods)))
        for i,p in enumerate(periods[:12]):
            x=left+i*col_w
            ip=find_icon_path(pick_icon(p.get("short"), p.get("is_day")))
            if ip:
                try:
                    icon_size = self.s(40, 1)
                    icon=Image.open(ip).convert("RGBA").resize((icon_size, icon_size))
                    self.surface.paste(icon,(x,top),icon)
                except Exception:
                    pass
            t=p.get("temperature"); u=p.get("unit","F")
            draw.text((x, top+self.s(44, 1)), f"{'--' if t is None else t}°{u}", font=self.f_sm, fill=(255,255,255,255))
            pr=p.get("prob"); pr_txt="--" if pr is None else f"{int(pr)}%"
            draw.text((x, top+self.s(44, 1)+self.s(22, 1)), pr_txt, font=self.f_tiny, fill=(210,220,230,255))
            draw.text((x, top+self.s(44, 1)+self.s(22, 1)+self.s(18, 1)), str(p.get("label","--:--")), font=self.f_tiny, fill=(210,220,230,255))

        return self._mark_all_dirty_if_changed()
