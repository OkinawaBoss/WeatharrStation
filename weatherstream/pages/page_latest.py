from __future__ import annotations
from typing import List, Dict, Any
from weatherstream.layers.chrome import Chrome
from weatherstream.pages import Page, overlays, content_bounds
from weatherstream.core.datastore import DataStore
from weatherstream.layers.latest import LatestLayer

def build(cfg, data_store: DataStore) -> Page:
    layers = overlays(cfg, data_store)
    x, y, w, h = content_bounds(cfg)

    def get_rows() -> List[Dict[str, Any]]:
        # Expect rows like {"name": "...", "temp": "88.0°F", "condition": "...", "wind": "..."}
        return data_store.read().get("latest_rows", []) or []

    layers.append(LatestLayer(x=x, y=y, w=w, h=h, get_rows=get_rows, min_interval=15.0))
    return Page("latest", layers)
