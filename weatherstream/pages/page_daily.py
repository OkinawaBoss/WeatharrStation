from __future__ import annotations
from typing import List, Dict, Any
from weatherstream.layers.chrome import Chrome
from weatherstream.pages import Page, overlays, content_bounds
from weatherstream.core.datastore import DataStore
from weatherstream.layers.daily import DailyLayer

def build(cfg, data_store: DataStore) -> Page:
    layers = overlays(cfg, data_store)
    x, y, w, h = content_bounds(cfg, top_gutter=120)

    def get_days() -> List[Dict[str, Any]]:
        # Expect items like {"name":"MON","high":88,"low":74,"unit":"F","short":"Sunny","is_day":True}
        return data_store.read().get("daily_days", []) or []

    layers.append(DailyLayer(x=x, y=y, w=w, h=h, get_days=get_days, min_interval=30.0))
    return Page("daily", layers)
