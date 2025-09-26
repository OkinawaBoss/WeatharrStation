from __future__ import annotations
from typing import List, Dict, Any, Tuple
from PIL import Image
from weatherstream.layers.chrome import Chrome
from weatherstream.pages import Page, overlays, content_bounds
from weatherstream.core.datastore import DataStore
from weatherstream.layers.regional import RegionalLayer

def build(cfg, data_store: DataStore) -> Page:
    layers = overlays(cfg, data_store)
    x, y, w, h = content_bounds(cfg)

    def get_points() -> List[Dict[str, Any]]:
        return data_store.read().get("regional_points", []) or []

    def get_map() -> Image.Image | None:
        return data_store.read().get("regional_map_image")

    def get_bounds() -> Tuple[float, float, float, float] | None:
        return data_store.read().get("regional_map_bounds")

    layers.append(RegionalLayer(x=x, y=y, w=w, h=h,
                                get_points=get_points, get_map=get_map, get_bounds=get_bounds,
                                min_interval=15.0))
    return Page("regional", layers)
