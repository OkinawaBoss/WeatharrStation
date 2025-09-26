from __future__ import annotations
from typing import Callable, List, Dict, Any
from PIL import ImageDraw, ImageFont
from weatherstream.core.layer import Layer

def _font(s):
    try:
        return ImageFont.truetype("assets/fonts/Inter-Regular.ttf", s)
    except Exception:
        return ImageFont.load_default()

class LatestLayer(Layer):
    """
    get_rows() -> list of dicts: {name, temp, condition, wind}
    """
    def __init__(self,x:int,y:int,w:int,h:int,get_rows:Callable[[],List[Dict[str,Any]]],min_interval:float=15.0):
        super().__init__(x,y,w,h,min_interval=min_interval)
        self.get_rows=get_rows
        self.f_sm = _font(30)
        self.f_tiny = _font(24)

    def tick(self, now: float):
        draw=ImageDraw.Draw(self.surface)
        draw.rectangle((0,0,*self.surface.size), fill=(24,32,44,235))
        rows=self.get_rows() or []
        if not rows:
            draw.text((12,12),"No recent observations",font=self.f_sm,fill=(255,255,255,255))
            return self._mark_all_dirty_if_changed()

        headers=["Station","Temperature","Condition","Wind"]
        cols=[0.0,0.28,0.55,0.78]
        x=[int(24 + (self.surface.width-48)*f) for f in cols]
        y=24
        for lab,xx in zip(headers,x):
            draw.text((xx,y), lab, font=self.f_sm, fill=(255,255,255,255))
        y+=40

        col_right=[x[1],x[2],x[3],self.surface.width-12]
        col_width=[r-l-8 for l,r in zip(x,col_right)]
        max_rows=10
        for r in rows[:max_rows]:
            draw.text((x[0], y), str(r.get("name","")), font=self.f_tiny, fill=(235,242,255,255))
            draw.text((x[1], y), str(r.get("temp","")), font=self.f_tiny, fill=(235,242,255,255))
            # wrap condition and wind to fit
            cond=str(r.get("condition",""))
            wind=str(r.get("wind",""))
            # crude wrapping:
            def wrap(txt,w):
                out=[]; cur=""
                for wword in txt.split():
                    t=(cur+" "+wword).strip()
                    if draw.textbbox((0,0), t, font=self.f_tiny)[2] <= w:
                        cur=t
                    else:
                        out.append(cur); cur=wword
                if cur: out.append(cur)
                return out[:2]
            cy=y
            for line in wrap(cond,col_width[2]):
                draw.text((x[2],cy), line, font=self.f_tiny, fill=(235,242,255,255)); cy+=26
            wy=y
            for line in wrap(wind,col_width[3]):
                draw.text((x[3],wy), line, font=self.f_tiny, fill=(235,242,255,255)); wy+=26
            y += max(cy,wy,y+26) - y + 10
            if y > self.surface.height-24:
                break

        return self._mark_all_dirty_if_changed()
