from __future__ import annotations
from typing import List, Dict, Any
from weatherstream.layers.chrome import Chrome
from weatherstream.pages import Page, overlays, content_bounds
from weatherstream.core.datastore import DataStore
from weatherstream.layers.hourly_graph import HourlyGraphLayer

def build(cfg, data_store: DataStore) -> Page:
    layers = overlays(cfg, data_store)
    x, y, w, h = content_bounds(cfg)

    def get_points() -> List[Dict[str, Any]]:
        # Expect items like {"temp": 84.0, "precip": 10, "cloud": 40, "label": "2 PM"}
        return data_store.read().get("hourly_points", []) or []

    layers.append(HourlyGraphLayer(x=x, y=y, w=w, h=h, get_points=get_points, min_interval=15.0))
    return Page("hourly_graph", layers)
