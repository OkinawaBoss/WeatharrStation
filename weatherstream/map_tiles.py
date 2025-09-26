from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from typing import Iterable, List, Optional, Tuple

import requests
from PIL import Image


_TILE_SIZE = 256
_OSM_TILE_URL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
_RAINVIEWER_META_URL = "https://api.rainviewer.com/public/weather-maps.json"
_RAINVIEWER_TILE_URL = "https://tilecache.rainviewer.com/v2/radar/{time}/256/{z}/{x}/{y}/2/1_1.png"

_IMAGE_CACHE: dict[tuple, tuple[float, Image.Image]] = {}
_META_CACHE: dict[str, tuple[float, dict]] = {}


@dataclass
class MapComposition:
    image: Image.Image
    zoom: int
    x0: float
    x1: float
    y0: float
    y1: float
    bounds: Tuple[float, float, float, float]


def _cache_get(key: tuple, ttl: int) -> Optional[Image.Image]:
    entry = _IMAGE_CACHE.get(key)
    if not entry:
        return None
    ts, img = entry
    if time.time() - ts > ttl:
        return None
    return img.copy()


def _cache_put(key: tuple, img: Image.Image) -> None:
    _IMAGE_CACHE[key] = (time.time(), img)


def _meta_cache_get(key: str, ttl: int) -> Optional[dict]:
    entry = _META_CACHE.get(key)
    if not entry:
        return None
    ts, data = entry
    if time.time() - ts > ttl:
        return None
    return data


def _meta_cache_put(key: str, data: dict) -> None:
    _META_CACHE[key] = (time.time(), data)


def _lon_to_tile(lon: float, zoom: int) -> float:
    return (lon + 180.0) / 360.0 * (2 ** zoom)


def _lat_to_tile(lat: float, zoom: int) -> float:
    lat_rad = math.radians(lat)
    return (1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * (2 ** zoom)


def _tile_to_lon(x: float, zoom: int) -> float:
    return x / (2 ** zoom) * 360.0 - 180.0


def _tile_to_lat(y: float, zoom: int) -> float:
    n = math.pi - 2.0 * math.pi * y / (2 ** zoom)
    return math.degrees(math.atan(math.sinh(n)))


def _auto_zoom(lat_min: float, lon_min: float, lat_max: float, lon_max: float, width: int, height: int) -> int:
    target_tiles_x = max(1.0, width / _TILE_SIZE)
    target_tiles_y = max(1.0, height / _TILE_SIZE)
    for zoom in range(11, 4, -1):
        x_span = abs(_lon_to_tile(lon_max, zoom) - _lon_to_tile(lon_min, zoom))
        y_span = abs(_lat_to_tile(lat_min, zoom) - _lat_to_tile(lat_max, zoom))
        if x_span <= target_tiles_x + 1 and y_span <= target_tiles_y + 1:
            return zoom
    return 6


def _fetch_tile(url: str, headers: dict[str, str], ttl: int = 900) -> Optional[Image.Image]:
    key = ("tile", url)
    cached = _cache_get(key, ttl)
    if cached is not None:
        return cached
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
    except Exception:
        return None
    try:
        img = Image.open(BytesIO(resp.content)).convert("RGBA")
    except Exception:
        return None
    _cache_put(key, img)
    return img.copy()


def _assemble_tiles(tile_urls: Iterable[Tuple[int, int, str]], headers: dict[str, str]) -> Optional[Image.Image]:
    entries = list(tile_urls)
    xs = sorted(set(x for x, _, _ in entries))
    ys = sorted(set(y for _, y, _ in entries))
    if not xs or not ys:
        return None
    canvas = Image.new("RGBA", (len(xs) * _TILE_SIZE, len(ys) * _TILE_SIZE), (0, 0, 0, 0))
    lookup = {(x, y): url for x, y, url in entries}
    for xi, x in enumerate(xs):
        for yi, y in enumerate(ys):
            tile_url = lookup.get((x, y))
            if not tile_url:
                continue
            tile = _fetch_tile(tile_url, headers)
            if tile is None:
                continue
            canvas.paste(tile, (xi * _TILE_SIZE, yi * _TILE_SIZE))
    return canvas


def _crop_and_scale(img: Image.Image, x_min: float, x_max: float, y_min: float, y_max: float, width: int, height: int) -> Image.Image:
    if img is None:
        return Image.new("RGBA", (width, height), (0, 0, 0, 255))
    tile_x_start = math.floor(x_min)
    tile_y_start = math.floor(y_min)
    offset_x = int(round((x_min - tile_x_start) * _TILE_SIZE))
    offset_y = int(round((y_min - tile_y_start) * _TILE_SIZE))
    pixel_width = int(round((x_max - x_min) * _TILE_SIZE))
    pixel_height = int(round((y_max - y_min) * _TILE_SIZE))
    if pixel_width <= 0 or pixel_height <= 0:
        return img.resize((width, height), Image.LANCZOS)
    crop_box = (
        offset_x,
        offset_y,
        offset_x + pixel_width,
        offset_y + pixel_height,
    )
    cropped = img.crop(crop_box)
    if cropped.size != (width, height):
        cropped = cropped.resize((width, height), Image.BICUBIC)
    return cropped


def compose_base_map(
    lat_min: float,
    lon_min: float,
    lat_max: float,
    lon_max: float,
    width: int,
    height: int,
    user_agent: str,
    zoom: Optional[int] = None,
) -> Optional[MapComposition]:
    if lat_min >= lat_max or lon_min >= lon_max:
        return None

    lat_min = max(lat_min, -85.0)
    lat_max = min(lat_max, 85.0)

    zoom = zoom or _auto_zoom(lat_min, lon_min, lat_max, lon_max, width, height)
    x0 = _lon_to_tile(lon_min, zoom)
    x1 = _lon_to_tile(lon_max, zoom)
    y0 = _lat_to_tile(lat_max, zoom)
    y1 = _lat_to_tile(lat_min, zoom)

    desired_ratio = width / float(height)
    span_x = x1 - x0
    span_y = y1 - y0
    if span_x <= 0 or span_y <= 0:
        return None
    actual_ratio = span_x / span_y
    if actual_ratio > desired_ratio:
        needed_y = span_x / desired_ratio
        extra = (needed_y - span_y) / 2.0
        y0 -= extra
        y1 += extra
    else:
        needed_x = span_y * desired_ratio
        extra = (needed_x - span_x) / 2.0
        x0 -= extra
        x1 += extra

    tile_urls = []
    headers = {"User-Agent": user_agent}
    for tx in range(math.floor(x0), math.ceil(x1)):
        for ty in range(math.floor(y0), math.ceil(y1)):
            url = _OSM_TILE_URL.format(z=zoom, x=tx, y=ty)
            tile_urls.append((tx, ty, url))

    tiles = _assemble_tiles(tile_urls, headers)
    if tiles is None:
        return None

    base = _crop_and_scale(tiles, x0, x1, y0, y1, width, height)

    lon_min_adj = _tile_to_lon(x0, zoom)
    lon_max_adj = _tile_to_lon(x1, zoom)
    lat_max_adj = _tile_to_lat(y0, zoom)
    lat_min_adj = _tile_to_lat(y1, zoom)

    bounds = (lat_min_adj, lon_min_adj, lat_max_adj, lon_max_adj)
    return MapComposition(base, zoom, x0, x1, y0, y1, bounds)


def _get_rainviewer_meta() -> Optional[dict]:
    cached = _meta_cache_get("rainviewer", ttl=120)
    if cached is not None:
        return cached
    try:
        resp = requests.get(_RAINVIEWER_META_URL, timeout=10)
        resp.raise_for_status()
    except Exception:
        return None
    try:
        data = resp.json()
    except Exception:
        return None
    _meta_cache_put("rainviewer", data)
    return data


def _compose_radar_overlay(view: MapComposition, timestamp: int, user_agent: str) -> Optional[Image.Image]:
    tile_urls = []
    headers = {"User-Agent": user_agent}
    for tx in range(math.floor(view.x0), math.ceil(view.x1)):
        for ty in range(math.floor(view.y0), math.ceil(view.y1)):
            url = _RAINVIEWER_TILE_URL.format(time=timestamp, z=view.zoom, x=tx, y=ty)
            tile_urls.append((tx, ty, url))

    overlay_tiles = _assemble_tiles(tile_urls, headers)
    if overlay_tiles is None:
        return None
    return _crop_and_scale(overlay_tiles, view.x0, view.x1, view.y0, view.y1, view.image.width, view.image.height)


def _compose_radar_frames_sync(
    center_lat: float,
    center_lon: float,
    width: int,
    height: int,
    user_agent: str,
    zoom: Optional[int] = None,
    span_degrees: float = 6.0,
    max_frames: int = 8,
) -> Tuple[List[dict], Optional[MapComposition]]:
    half_span = max(1.0, span_degrees) / 2.0
    lat_min = center_lat - half_span
    lat_max = center_lat + half_span
    lon_span = half_span * max(1.0, width / max(height, 1)) / max(math.cos(math.radians(center_lat)), 0.25)
    lon_min = center_lon - lon_span
    lon_max = center_lon + lon_span

    base_view = compose_base_map(lat_min, lon_min, lat_max, lon_max, width, height, user_agent, zoom)
    if base_view is None:
        return [], None

    meta = _get_rainviewer_meta()
    if not meta:
        return [], base_view

    radar_past = (meta.get("radar") or {}).get("past") or []
    radar_now = (meta.get("radar") or {}).get("nowcast") or []
    timestamps = [item.get("time") for item in radar_past + radar_now if item.get("time")]
    if not timestamps:
        return [], base_view

    timestamps = sorted(set(timestamps))
    timestamps = timestamps[-max_frames:]

    frames: List[dict] = []
    for ts in timestamps:
        overlay = _compose_radar_overlay(base_view, ts, user_agent)
        frame_image = base_view.image.copy()
        if overlay is not None:
            frame_image.alpha_composite(overlay)
        label = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone().strftime("%I:%M %p")
        frames.append({"image": frame_image, "label": label, "timestamp": ts})

    return frames, base_view


class RadarFrameManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._frames: List[dict] = []
        self._view: Optional[MapComposition] = None
        self._thread: Optional[threading.Thread] = None
        self._last_params: Optional[tuple] = None
        self._last_fetch_ts: float = 0.0
        self._ready_event = threading.Event()

    def get_frames(self) -> List[dict]:
        with self._lock:
            return list(self._frames)

    def get_base_view(self) -> Optional[MapComposition]:
        with self._lock:
            return self._view

    def ensure_fetch(
        self,
        center_lat: float,
        center_lon: float,
        width: int,
        height: int,
        user_agent: str,
        zoom: Optional[int] = None,
        span_degrees: float = 6.0,
        max_frames: int = 8,
        force: bool = False,
    ) -> None:
        params = (
            round(center_lat, 4),
            round(center_lon, 4),
            int(width),
            int(height),
            user_agent,
            zoom,
            round(span_degrees, 3),
            max_frames,
        )
        with self._lock:
            if not force and self._thread and self._thread.is_alive() and params == self._last_params:
                return
            self._last_params = params
            def _target():
                try:
                    frames, view = _compose_radar_frames_sync(
                        center_lat,
                        center_lon,
                        width,
                        height,
                        user_agent,
                        zoom=zoom,
                        span_degrees=span_degrees,
                        max_frames=max_frames,
                    )
                except Exception:
                    frames, view = [], None
                with self._lock:
                    if frames:
                        self._frames = frames
                        self._view = view
                        self._last_fetch_ts = time.time()
                        self._ready_event.set()
                    elif view is not None and self._view is None:
                        self._view = view
                        self._ready_event.set()
            self._thread = threading.Thread(target=_target, name="radar-fetch", daemon=True)
            self._thread.start()


_RADAR_MANAGER = RadarFrameManager()


def ensure_radar_frames(announce: bool = True, **kwargs) -> None:
    if announce:
        print("[WeatherStream] Starting radar frame fetch", flush=True)
    _RADAR_MANAGER.ensure_fetch(**kwargs)


def get_cached_radar_frames() -> List[dict]:
    return _RADAR_MANAGER.get_frames()


def get_cached_radar_view() -> Optional[MapComposition]:
    return _RADAR_MANAGER.get_base_view()


def wait_for_radar_frames(timeout: float = 0.0) -> bool:
    if timeout is None:
        timeout = 0.0
    return _RADAR_MANAGER._ready_event.wait(timeout)


def compose_radar_map(
    center_lat: float,
    center_lon: float,
    width: int,
    height: int,
    user_agent: str,
    zoom: Optional[int] = None,
    span_degrees: float = 6.0,
) -> Optional[Image.Image]:
    frames, _ = _compose_radar_frames_sync(center_lat, center_lon, width, height, user_agent, zoom, span_degrees=span_degrees, max_frames=1)
    if frames:
        return frames[-1]["image"].copy()
    return None
