from __future__ import annotations
from typing import Callable, Dict, Any, List
from PIL import ImageDraw, ImageFont
from weatherstream.core.layer import Layer
from weatherstream.icons import pick_icon, find_icon_path

def _font(s):
    try:
        return ImageFont.truetype("assets/fonts/Inter-Regular.ttf", s)
    except Exception:
        return ImageFont.load_default()

def _wrap(draw, text, font, width, lines):
    if not text: return []
    words=text.split(); out=[]; cur=""
    for w in words:
        t=(cur+" "+w).strip()
        if draw.textbbox((0,0),t,font=font)[2] <= width:
            cur=t
        else:
            out.append(cur); cur=w
            if len(out)>=lines: return out
    if cur: out.append(cur)
    return out[:lines]

class ForecastTextLayer(Layer):
    """get_periods() -> list of 2 dicts: {name,temp,unit,wind,wind_dir,precip,short,detailed,is_day}"""
    def __init__(self,x:int,y:int,w:int,h:int,get_periods:Callable[[],List[Dict[str,Any]]],min_interval:float=30.0):
        super().__init__(x,y,w,h,min_interval=min_interval)
        self.get_periods=get_periods
        self.f_sm = _font(34)
        self.f_tiny = _font(24)

    def tick(self, now: float):
        draw=ImageDraw.Draw(self.surface)
        draw.rectangle((0,0,*self.surface.size), fill=(28,40,56,235))
        periods=self.get_periods() or []

        if not periods:
            draw.text((12,12),"No forecast available",font=self.f_sm,fill=(255,255,255,255))
            return self._mark_all_dirty_if_changed()

        panels=min(2,len(periods))
        pad=16; panel_w=self.surface.width//panels
        for i in range(panels):
            p=periods[i]
            x=i*panel_w
            draw.rounded_rectangle((x+12,12,x+panel_w-12,self.surface.height-12), radius=24, fill=(32,46,64,235))
            draw.text((x+pad,24), str(p.get("name","")).upper(), font=self.f_sm, fill=(255,230,120,255))
            t=p.get("temperature"); u=p.get("unit","F")
            if t is not None:
                draw.text((x+pad, 24+36), f"{t}°{u}", font=self.f_sm, fill=(255,255,255,255))
            wd=p.get("wind_dir",""); wv=p.get("wind","")
            if wd or wv:
                draw.text((x+pad, 24+70), f"WIND {wd} {wv}".strip(), font=self.f_tiny, fill=(215,225,235,255))
            pr=p.get("precip")
            if pr is not None:
                draw.text((x+pad, 24+96), f"PRECIP {int(pr)}%", font=self.f_tiny, fill=(215,225,235,255))

            ip = find_icon_path(pick_icon(p.get("short"), p.get("is_day")))
            if ip:
                try:
                    from PIL import Image
                    icon=Image.open(ip).convert("RGBA").resize((80,80))
                    self.surface.paste(icon,(x+panel_w-100,32),icon)
                except Exception:
                    pass
            text=p.get("detailed") or p.get("short") or ""
            lines=_wrap(draw, text.upper(), self.f_sm, panel_w-2*pad, 10)
            yy=24+140
            for line in lines:
                draw.text((x+pad,yy), line, font=self.f_sm, fill=(235,242,255,255))
                yy+=38

        return self._mark_all_dirty_if_changed()
