from __future__ import annotations
from typing import Callable, Dict, Any, List, Tuple
from PIL import ImageDraw, ImageFont
from weatherstream.core.layer import Layer
from weatherstream.icons import pick_icon, find_icon_path

def _font(size):
    try:
        return ImageFont.truetype("assets/fonts/Inter-Regular.ttf", size)
    except Exception:
        return ImageFont.load_default()

class CurrentLayer(Layer):
    """
    Draws current conditions panel.
    get_data(): dict with keys:
      temp_f, observed_conditions/forecast_short, wind_dir, wind_speed_mph,
      station_name, humidity, dew_f, heat_index, pressure_inhg, visibility_mi, ceiling_ft, forecast_is_day
    """
    def __init__(self, x:int, y:int, w:int, h:int, get_data: Callable[[], Dict[str,Any]], min_interval:float=5.0):
        super().__init__(x,y,w,h,min_interval=min_interval)
        self.get_data = get_data
        self.f_big = _font(72)
        self.f_sm = _font(36)
        self.f_tiny = _font(26)

    def tick(self, now: float):
        d = self.get_data() or {}
        draw = ImageDraw.Draw(self.surface)
        # clear
        draw.rectangle((0,0,*self.surface.size), fill=(20,30,44,235))

        temp_f = d.get("temp_f")
        temp_text = f"{temp_f:.1f}°F" if isinstance(temp_f,(int,float)) else "--°F"
        cond = d.get("observed_conditions") or d.get("forecast_short") or "--"
        wind = "Calm"
        if d.get("wind_speed_mph") is not None:
            wd = d.get("wind_dir","--")
            wind = f"{wd} {d['wind_speed_mph']:.1f} mph"

        # Icon
        icon_key = pick_icon(cond, d.get("forecast_is_day"))
        ip = find_icon_path(icon_key)
        if ip:
            try:
                from PIL import Image
                icon = Image.open(ip).convert("RGBA").resize((140,140))
                self.surface.paste(icon, (24,24), icon)
            except Exception:
                pass

        draw.text((180, 20), temp_text, fill=(255,255,255,255), font=self.f_big)
        draw.text((180, 120), cond, fill=(230,240,255,255), font=self.f_sm)
        draw.text((180, 172), f"Wind {wind}", fill=(210,220,230,255), font=self.f_sm)

        x0, y0 = 32, 220
        rows = [
            ("Humidity", f"{int(d['humidity'])}%" if d.get("humidity") is not None else "--"),
            ("Dewpoint", f"{d['dew_f']:.1f}°F" if d.get("dew_f") is not None else "--"),
            ("Heat Index", f"{d['heat_index']:.1f}°F" if d.get("heat_index") is not None else "--"),
            ("Pressure", f"{d['pressure_inhg']:.2f} inHg" if d.get("pressure_inhg") is not None else "--"),
            ("Visibility", f"{d['visibility_mi']:.1f} mi" if d.get("visibility_mi") is not None else "--"),
            ("Ceiling", f"{int(d['ceiling_ft'])} ft" if d.get("ceiling_ft") is not None else "Unlimited"),
       ]
        col_w = self.surface.width // 2
        for i,(k,v) in enumerate(rows):
            cx = x0 + (i%2)*col_w
            cy = y0 + (i//2)*60
            draw.text((cx, cy), k, font=self.f_tiny, fill=(200,210,220,255))
            draw.text((cx, cy+28), v, font=self.f_tiny, fill=(255,255,255,255))

        return self._mark_all_dirty_if_changed()
