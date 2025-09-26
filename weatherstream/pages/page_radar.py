from __future__ import annotations
from typing import List
from PIL import Image
from weatherstream.layers.chrome import Chrome
from weatherstream.pages import Page, overlays, content_bounds
from weatherstream.core.datastore import DataStore
from weatherstream.layers.radar import RadarLayer

def build(cfg, data_store: DataStore) -> Page:
    layers = overlays(cfg, data_store)
    x, y, w, h = content_bounds(cfg, top_gutter=100)
    # Expect data_store.read()["radar_new_frames"] to be a callable returning List[PIL.Image]
    def _get_new_frames() -> List[Image.Image]:
        src = data_store.read().get("radar_new_frames")
        return src() if callable(src) else []
    layers.append(RadarLayer(x=x, y=y, w=w, h=h, min_interval=0.1, get_new_frames=_get_new_frames))
    return Page("radar", layers)
