from __future__ import annotations
from typing import Callable, List, Dict, Any
from PIL import ImageDraw, ImageFont
from weatherstream.core.layer import Layer

def _font(s):
    try:
        return ImageFont.truetype("assets/fonts/Inter-Regular.ttf", s)
    except Exception:
        return ImageFont.load_default()

class HourlyGraphLayer(Layer):
    """
    get_points() -> list of {temp:float|None, precip:int|None, cloud:int|None, label:str}
    """
    def __init__(self,x:int,y:int,w:int,h:int,get_points:Callable[[],List[Dict[str,Any]]],min_interval:float=15.0):
        super().__init__(x,y,w,h,min_interval=min_interval)
        self.get_points=get_points
        self.f_sm = _font(28)
        self.f_tiny = _font(22)

    def tick(self, now: float):
        draw=ImageDraw.Draw(self.surface)
        draw.rectangle((0,0,*self.surface.size), fill=(24,32,44,235))
        pts=self.get_points() or []
        if not pts:
            draw.text((12,12),"Hourly data unavailable",font=self.f_sm,fill=(255,255,255,255))
            return self._mark_all_dirty_if_changed()

        left=80; right=self.surface.width-80
        top=60; bottom=self.surface.height-120
        draw.line((left,top,left,bottom), fill=(120,140,160,255), width=2)
        draw.line((left,bottom,right,bottom), fill=(120,140,160,255), width=2)

        temps=[p["temp"] for p in pts if p.get("temp") is not None]
        tmin=min(temps) if temps else 0; tmax=max(temps) if temps else 100
        if tmax-tmin<10: pad=(10-(tmax-tmin))/2; tmin-=pad; tmax+=pad
        y_min=tmin-2; y_max=tmax+2

        def x_for(i): 
            n=max(1,len(pts)-1)
            return left + int((i/n)*(right-left))
        def y_for_temp(v):
            if y_max==y_min: return bottom
            return int(bottom - ((v-y_min)/(y_max-y_min))*(bottom-top))
        def y_for_pct(pct):
            return int(bottom - (pct/100.0)*(bottom-top))

        # ticks
        for i in range(5):
            frac=i/4
            y=bottom-int(frac*(bottom-top))
            tv=y_min+(y_max-y_min)*frac
            draw.line((left-8,y,left,y), fill=(160,180,200,255), width=2)
            draw.text((16,y-16), f"{tv:.0f}°F", font=self.f_tiny, fill=(200,210,220,255))
        for v in (0,25,50,75,100):
            y=y_for_pct(v)
            draw.line((right,y,right+8,y), fill=(100,160,220,255), width=2)
            draw.text((right+20,y-16), f"{v}%", font=self.f_tiny, fill=(200,210,220,255))

        temp_pts=[]; precip_pts=[]; cloud_pts=[]
        for i,p in enumerate(pts):
            x=x_for(i)
            if p.get("temp") is not None: temp_pts.append((x,y_for_temp(p["temp"])))
            if p.get("precip") is not None: precip_pts.append((x,y_for_pct(p["precip"])))
            if p.get("cloud") is not None: cloud_pts.append((x,y_for_pct(p["cloud"])))

        if len(temp_pts)>1: draw.line(temp_pts, fill=(255,162,57,255), width=6)
        if len(precip_pts)>1: draw.line(precip_pts, fill=(30,144,255,255), width=5)
        if len(cloud_pts)>1: draw.line(cloud_pts, fill=(200,200,200,255), width=5)
        for x,y in temp_pts: draw.ellipse((x-6,y-6,x+6,y+6), fill=(255,255,255,255))

        # x labels
        ly=bottom+6
        for i,p in enumerate(pts):
            x=x_for(i)
            draw.text((x-20,ly), str(p.get("label","")), font=self.f_tiny, fill=(210,220,230,255))

        return self._mark_all_dirty_if_changed()
