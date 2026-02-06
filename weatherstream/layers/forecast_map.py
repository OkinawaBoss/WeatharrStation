from __future__ import annotations
from typing import Callable, List, Dict, Any, Tuple
from PIL import Image, ImageDraw, ImageFont
from weatherstream.core.layer import Layer
from weatherstream.icons import pick_icon, find_icon_path

def _font(s):
    try:
        return ImageFont.truetype("assets/fonts/Inter-Regular.ttf", s)
    except Exception:
        return ImageFont.load_default()

class ForecastMapLayer(Layer):
    """
    get_points(): list of {lat, lon, name, forecast_short, forecast_temp, is_day}
    get_map(): returns PIL.Image or None (already pre-fetched/scaled ok)
    get_bounds(): (lat_min, lon_min, lat_max, lon_max) or None -> auto-fit
    """
    def __init__(self, x:int,y:int,w:int,h:int,
                 get_points:Callable[[],List[Dict[str,Any]]],
                 get_map:Callable[[],Image.Image|None],
                 get_bounds:Callable[[],Tuple[float,float,float,float] | None],
                 min_interval:float=10.0,
                 scale: float = 1.0):
        super().__init__(x,y,w,h,min_interval=min_interval, scale=scale)
        self.get_points = get_points
        self.get_map = get_map
        self.get_bounds = get_bounds
        self.f_sm = _font(self.s(32, 10))
        self.f_tiny = _font(self.s(24, 8))

    def tick(self, now: float):
        draw = ImageDraw.Draw(self.surface)
        draw.rectangle((0,0,*self.surface.size), fill=(24,32,44,235))
        pts = self.get_points() or []
        mimg = self.get_map()
        if mimg:
            try:
                base = mimg.resize(self.surface.size).convert("RGBA")
                tint = Image.new("RGBA", base.size, (8, 12, 24, 96))
                base = Image.alpha_composite(base, tint)
                self.surface.paste(base, (0, 0))
            except Exception:
                draw.rectangle((0,0,*self.surface.size), fill=(24,32,44,235))
        # fallback grid
        else:
            grid = (40,60,80,160)
            for frac in (0.25,0.5,0.75):
                y = int(frac*self.surface.height); x=int(frac*self.surface.width)
                draw.line((0,y,self.surface.width,y), fill=grid, width=self.s(2, 1))
                draw.line((x,0,x,self.surface.height), fill=grid, width=self.s(2, 1))

        if not pts:
            draw.text((self.s(12), self.s(12)),"Forecast data unavailable",font=self.f_sm,fill=(255,255,255,255))
            return self._mark_all_dirty_if_changed()

        b = self.get_bounds()
        if b:
            lat_min, lon_min, lat_max, lon_max = b
        else:
            lats=[p["lat"] for p in pts if p.get("lat") is not None]
            lons=[p["lon"] for p in pts if p.get("lon") is not None]
            lat_min=min(lats); lat_max=max(lats); lon_min=min(lons); lon_max=max(lons)
            if lat_max-lat_min<0.5: lat_min-=0.25; lat_max+=0.25
            if lon_max-lon_min<0.5: lon_min-=0.25; lon_max+=0.25
        lon_span=max(lon_max-lon_min,1e-6)
        lat_span=max(lat_max-lat_min,1e-6)

        def project(lat,lon):
            x=int(((lon-lon_min)/lon_span)*self.surface.width)
            y=self.surface.height - int(((lat-lat_min)/lat_span)*self.surface.height)
            return x,y

        label_pos=[]
        for p in pts:
            lat,lon=p.get("lat"),p.get("lon")
            if lat is None or lon is None: continue
            x,y=project(lat,lon)

            ip = find_icon_path(pick_icon(p.get("forecast_short"), p.get("is_day")))
            if ip:
                try:
                    icon_size = self.s(64, 1)
                    icon = Image.open(ip).convert("RGBA").resize((icon_size, icon_size))
                    self.surface.paste(icon,(x-(icon_size//2),y-(icon_size//2)),icon)
                except Exception:
                    pass
            dot = self.s(6, 1)
            draw.ellipse((x-dot,y-dot,x+dot,y+dot), outline=(255,255,255,220), width=self.s(2, 1))

            label_x,label_y=x+self.s(16, 1),y-self.s(24, 1)
            for ex,ey in label_pos:
                if abs(label_x-ex)<self.s(100, 1) and abs(label_y-ey)<self.s(32, 1):
                    label_y+=self.s(36, 1)
            label_pos.append((label_x,label_y))
            temp = p.get("forecast_temp","--")
            draw.text(
                (label_x,label_y),
                str(p.get("name","City")),
                font=self.f_sm,
                fill=(250,252,255,255),
                stroke_width=self.s(4, 1),
                stroke_fill=(0, 0, 0, 220),
            )
            draw.text(
                (label_x,label_y+self.s(38, 1)),
                str(temp),
                font=self.f_sm,
                fill=(255,230,120,255),
                stroke_width=self.s(4, 1),
                stroke_fill=(0, 0, 0, 220),
            )

        return self._mark_all_dirty_if_changed()
