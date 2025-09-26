from __future__ import annotations
from weatherstream.layers.chrome import Chrome
from weatherstream.pages import Page, overlays, content_bounds
from weatherstream.core.datastore import DataStore
from weatherstream.layers.current import CurrentLayer

def build(cfg, data_store: DataStore) -> Page:
    layers = overlays(cfg, data_store)
    x, y, w, h = content_bounds(cfg, top_gutter=140)
    layers.append(CurrentLayer(
        x=x, y=y, w=w, h=min(320, h),
        get_data=lambda: data_store.read().get("current", {}),
        min_interval=5.0
    ))
    return Page("current", layers)
