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
    def __init__(self,x:int,y:int,w:int,h:int,get_periods:Callable[[],List[Dict[str,Any]]],min_interval:float=30.0, scale: float = 1.0):
        super().__init__(x,y,w,h,min_interval=min_interval, scale=scale)
        self.get_periods=get_periods
        self.f_sm = _font(self.s(34, 10))
        self.f_tiny = _font(self.s(24, 8))

    def tick(self, now: float):
        draw=ImageDraw.Draw(self.surface)
        draw.rectangle((0,0,*self.surface.size), fill=(28,40,56,235))
        periods=self.get_periods() or []

        if not periods:
            draw.text((self.s(12), self.s(12)),"No forecast available",font=self.f_sm,fill=(255,255,255,255))
            return self._mark_all_dirty_if_changed()

        panels=min(2,len(periods))
        pad=self.s(16, 1); panel_w=self.surface.width//panels
        panel_pad = self.s(12, 1)
        panel_top = self.s(12)
        panel_bottom = self.surface.height - self.s(12)
        for i in range(panels):
            p=periods[i]
            x=i*panel_w
            draw.rounded_rectangle((x+panel_pad,panel_top,x+panel_w-panel_pad,panel_bottom), radius=self.s(24, 1), fill=(32,46,64,235))
            title_y = self.s(24)
            draw.text((x+pad,title_y), str(p.get("name","")).upper(), font=self.f_sm, fill=(255,230,120,255))
            t=p.get("temperature"); u=p.get("unit","F")
            if t is not None:
                draw.text((x+pad, title_y + self.s(36)), f"{t}°{u}", font=self.f_sm, fill=(255,255,255,255))
            wd=p.get("wind_dir",""); wv=p.get("wind","")
            if wd or wv:
                draw.text((x+pad, title_y + self.s(70)), f"WIND {wd} {wv}".strip(), font=self.f_tiny, fill=(215,225,235,255))
            pr=p.get("precip")
            if pr is not None:
                draw.text((x+pad, title_y + self.s(96)), f"PRECIP {int(pr)}%", font=self.f_tiny, fill=(215,225,235,255))

            ip = find_icon_path(pick_icon(p.get("short"), p.get("is_day")))
            if ip:
                try:
                    from PIL import Image
                    icon_size = self.s(80, 1)
                    icon = Image.open(ip).convert("RGBA").resize((icon_size, icon_size))
                    self.surface.paste(icon,(x+panel_w-icon_size-self.s(20), self.s(32)),icon)
                except Exception:
                    pass
            text=p.get("detailed") or p.get("short") or ""
            lines=_wrap(draw, text.upper(), self.f_sm, panel_w-2*pad, 10)
            yy=title_y + self.s(140)
            for line in lines:
                draw.text((x+pad,yy), line, font=self.f_sm, fill=(235,242,255,255))
                yy+=self.s(38)

        return self._mark_all_dirty_if_changed()
