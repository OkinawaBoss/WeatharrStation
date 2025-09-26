from __future__ import annotations
from typing import List, Dict, Any, Tuple
from PIL import Image
from weatherstream.layers.chrome import Chrome
from weatherstream.pages import Page, overlays, content_bounds
from weatherstream.core.datastore import DataStore
from weatherstream.layers.forecast_map import ForecastMapLayer

def build(cfg, data_store: DataStore) -> Page:
    layers = overlays(cfg, data_store)
    x, y, w, h = content_bounds(cfg)

    def get_points() -> List[Dict[str, Any]]:
        return data_store.read().get("forecast_points", []) or []

    def get_map() -> Image.Image | None:
        return data_store.read().get("forecast_map_image")

    def get_bounds() -> Tuple[float, float, float, float] | None:
        return data_store.read().get("forecast_map_bounds")

    layers.append(ForecastMapLayer(x=x, y=y, w=w, h=h,
                                   get_points=get_points, get_map=get_map, get_bounds=get_bounds,
                                   min_interval=10.0))
    return Page("forecast_map", layers)
