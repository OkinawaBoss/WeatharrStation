from __future__ import annotations
from dataclasses import dataclass
from typing import List, Callable, Iterable, Optional

from weatherstream.core.layer import Layer
from weatherstream.core.datastore import DataStore
from weatherstream.layers.clock import ClockLayer
from weatherstream.layers.ticker import TickerLayer

@dataclass
class Page:
    name: str
    layers: List[Layer]  # z-order is the list order; later items draw above earlier items

# --- Layout helpers ----------------------------------------------------------

def overlays(cfg, data_store: DataStore) -> List[Layer]:
    """Clock (top-right) + Ticker (bottom) used by most pages."""
    clock_w, clock_h = 360, 120
    clock_x = cfg.width - clock_w - 32
    clock_y = 24

    ticker_h = 64
    ticker_y = cfg.height - ticker_h - 20

    return [
        ClockLayer(x=clock_x, y=clock_y, w=clock_w, h=clock_h, min_interval=1.0),
        TickerLayer(
            x=48, y=ticker_y, w=cfg.width - 96, h=ticker_h,
            min_interval=1/30.0,
            px_per_sec=getattr(cfg, "ticker_speed_px_per_sec", 120),
            get_text=lambda: (data_store.read().get("ticker_text") or "Weatharr running").strip(),
        ),
    ]

def content_bounds(cfg, top_gutter: int = 140, bottom_gutter_extra: int = 20) -> tuple[int,int,int,int]:
    """
    A reasonable 'main content' box that avoids the ticker and leaves room at top.
    Returns (x, y, w, h).
    """
    x = 48
    y = top_gutter
    w = cfg.width - 96
    ticker_h = 64
    h = cfg.height - (y + ticker_h + 20 + bottom_gutter_extra)
    return x, y, w, h

# --- Optional: build all pages ------------------------------------------------

def build_all(cfg, data_store: DataStore) -> List[Page]:
    """Convenience to instantiate every page; you can filter/order in main.py."""
    from . import page_current, page_radar, page_forecast_map, page_regional
    from . import page_hourly_graph, page_latest, page_daily, page_forecast_text

    return [
        page_current.build(cfg, data_store),
        page_radar.build(cfg, data_store),
        page_forecast_map.build(cfg, data_store),
        page_regional.build(cfg, data_store),
        page_hourly_graph.build(cfg, data_store),
        page_latest.build(cfg, data_store),
        page_daily.build(cfg, data_store),
        page_forecast_text.build(cfg, data_store),
    ]
