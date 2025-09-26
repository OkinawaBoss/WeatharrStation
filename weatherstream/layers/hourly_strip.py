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
    def __init__(self,x:int,y:int,w:int,h:int,get_periods:Callable[[],List[Dict[str,Any]]],min_interval:float=30.0):
        super().__init__(x,y,w,h,min_interval=min_interval)
        self.get_periods=get_periods
        self.f_sm=_font(20); self.f_tiny=_font(14)

    def tick(self, now: float):
        draw=ImageDraw.Draw(self.surface)
        draw.rectangle((0,0,*self.surface.size), fill=(24,32,44,235))

        periods=self.get_periods() or []
        if not periods:
            draw.text((12,12),"No data",font=self.f_sm,fill=(255,255,255,255))
            return self._mark_all_dirty_if_changed()

        left=12; top=8
        col_w=max(1,(self.surface.width-2*left)//max(1,len(periods)))
        for i,p in enumerate(periods[:12]):
            x=left+i*col_w
            ip=find_icon_path(pick_icon(p.get("short"), p.get("is_day")))
            if ip:
                try:
                    icon=Image.open(ip).convert("RGBA").resize((40,40))
                    self.surface.paste(icon,(x,top),icon)
                except Exception:
                    pass
            t=p.get("temperature"); u=p.get("unit","F")
            draw.text((x, top+44), f"{'--' if t is None else t}°{u}", font=self.f_sm, fill=(255,255,255,255))
            pr=p.get("prob"); pr_txt="--" if pr is None else f"{int(pr)}%"
            draw.text((x, top+44+22), pr_txt, font=self.f_tiny, fill=(210,220,230,255))
            draw.text((x, top+44+22+18), str(p.get("label","--:--")), font=self.f_tiny, fill=(210,220,230,255))

        return self._mark_all_dirty_if_changed()
