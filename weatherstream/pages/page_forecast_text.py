from __future__ import annotations
from typing import List, Dict, Any
from weatherstream.layers.chrome import Chrome
from weatherstream.pages import Page, overlays, content_bounds
from weatherstream.core.datastore import DataStore
from weatherstream.layers.forecast_text import ForecastTextLayer

def build(cfg, data_store: DataStore) -> Page:
    layers = overlays(cfg, data_store)
    x, y, w, h = content_bounds(cfg)

    def get_periods() -> List[Dict[str, Any]]:
        # Return the next two periods; each like:
        # {"name": "Tonight", "temperature": 78, "unit":"F", "wind":"5 mph", "wind_dir":"SE",
        #  "precip": 10, "short": "Partly Cloudy", "detailed": "...", "is_day": False}
        return data_store.read().get("forecast_periods", []) or []

    layers.append(ForecastTextLayer(x=x, y=y, w=w, h=h, get_periods=get_periods, min_interval=30.0))
    return Page("forecast_text", layers)
