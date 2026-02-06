from __future__ import annotations
import os
import sys
import threading
import tempfile
import time
import xml.etree.ElementTree as ET
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from pathlib import Path
from typing import Optional, Callable, Iterable

from weatherstream.config import parse_args
from weatherstream.nws import NWSClient

# New lightweight runtime
from weatherstream.core.datastore import DataStore
from weatherstream.core.scheduler import Scheduler
from weatherstream.core.compositor import Compositor
from weatherstream.core.layer import Layer

# Layers
from weatherstream.layers.chrome import ChromeLayer
from weatherstream.layers.clock import ClockLayer
from weatherstream.layers.current import CurrentLayer
from weatherstream.layers.daily import DailyLayer
from weatherstream.layers.forecast_map import ForecastMapLayer
from weatherstream.layers.forecast_text import ForecastTextLayer
from weatherstream.layers.hourly_graph import HourlyGraphLayer
from weatherstream.layers.latest import LatestLayer
from weatherstream.layers.radar import RadarLayer
from weatherstream.layers.regional import RegionalLayer
from weatherstream.layers.ticker import TickerLayer

# We stream raw RGBA to ffmpeg (you already have this)
from weatherstream.output.stream_ffmpeg import FFMPEGStreamer
from weatherstream.data.zipcodes import resolve_zip
from weatherstream.utils import (
    prepare_current_conditions,
    compute_bounds,
    haversine_miles,
    parse_iso_datetime,
    parse_valid_time,
    to_local,
    c_to_f,
    ms_to_mph,
    safe_round,
    format_cardinal,
    set_timezone,
)
from weatherstream.data.major_cities import major_cities_near, canonical_city_name
from weatherstream import map_tiles


BASE_WIDTH = 1920
BASE_HEIGHT = 1080


# -----------------------------
# Minimal RSS support (stdlib)
# -----------------------------
class _RssTitleCache:
    _UA = "WeatherStreamRSS/1.0"

    def __init__(self, urls: list[str], refresh_sec: int = 300, max_items_per_feed: int = 10):
        self.urls = urls or []
        self.refresh_sec = max(30, int(refresh_sec or 300))
        self.max_items_per_feed = max(1, int(max_items_per_feed or 10))
        self._last_fetch = 0.0
        self._titles: list[str] = []

    def _http_get(self, url: str, timeout: int = 10) -> bytes | None:
        try:
            req = Request(url, headers={"User-Agent": self._UA})
            with urlopen(req, timeout=timeout) as r:
                return r.read()
        except (URLError, HTTPError, TimeoutError, ValueError):
            return None

    def _parse_titles(self, xml_bytes: bytes, max_items: int) -> list[str]:
        out: list[str] = []
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError:
            return out

        # RSS 2.0
        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            if title:
                out.append(title)
                if len(out) >= max_items:
                    return out

        # Atom
        if not out:
            for entry in root.findall(".//{*}entry"):
                title = (entry.findtext("{*}title") or "").strip()
                if title:
                    out.append(title)
                    if len(out) >= max_items:
                        return out
        return out

    def _refresh_if_needed(self) -> None:
        now = time.time()
        if now - self._last_fetch < self.refresh_sec:
            return
        titles: list[str] = []
        for url in self.urls:
            data = self._http_get(url)
            if not data:
                continue
            titles.extend(self._parse_titles(data, self.max_items_per_feed))
        # De-dupe, preserve order
        seen = set()
        uniq: list[str] = []
        for t in titles:
            key = t.lower()
            if key in seen:
                continue
            seen.add(key)
            uniq.append(t)
        self._titles = uniq
        self._last_fetch = now

    def get_titles(self) -> list[str]:
        if not self.urls:
            return []
        self._refresh_if_needed()
        return list(self._titles)


def _make_datastore(cfg, render_width: int, render_height: int, scale: float) -> DataStore:
    """Background data refresh thread; fetches NOAA data and map assets."""

    # If coords not provided yet, NWSClient will still be constructed once lat/lon are set.
    lat = cfg.lat or 29.735
    lon = cfg.lon or -94.977
    client = NWSClient(lat=lat, lon=lon, user_agent=cfg.user_agent, cache_ttl=180)
    radar_state: dict[str, float] = {"last_ts": 0.0}

    # RSS cache (constructed even if empty URL list; get_titles() will return [])
    rss_cache = _RssTitleCache(
        urls=getattr(cfg, "rss_urls", []) or [],
        refresh_sec=getattr(cfg, "rss_refresh_sec", 300),
        max_items_per_feed=getattr(cfg, "rss_max_items", 10),
    )

    def _alerts_text() -> str:
        try:
            alerts = client.alerts() or {}
            feats = alerts.get("features") or []
            headlines: list[str] = []
            for feat in feats[:10]:
                props = (feat or {}).get("properties") or {}
                headline = (props.get("headline") or "").strip()
                if headline:
                    headlines.append(headline)
            if headlines:
                return "  •  ".join(headlines)
            return "No active alerts"
        except Exception:
            return "Weather data unavailable"

    def _format_hour_label(dt):
        local = to_local(dt)
        hour = local.strftime("%I").lstrip("0") or "12"
        ampm = local.strftime("%p")[0]
        return f"{hour}{ampm}"

    def _build_cloud_lookup(grid_data: dict) -> dict:
        lookup: dict = {}
        values = (grid_data.get("properties") or {}).get("skyCover") or {}
        for item in values.get("values", []):
            valid = item.get("validTime")
            if not valid:
                continue
            try:
                start, _ = parse_valid_time(valid)
            except ValueError:
                continue
            key = to_local(start).replace(minute=0, second=0, microsecond=0)
            if item.get("value") is not None:
                lookup[key] = item.get("value")
        return lookup

    def _build_hourly_points(hourly_json: dict, grid_data: dict) -> list[dict]:
        periods = (hourly_json.get("properties") or {}).get("periods") or []
        periods = periods[:12]
        clouds = _build_cloud_lookup(grid_data)
        points: list[dict] = []
        for period in periods:
            start_raw = period.get("startTime")
            try:
                start_dt = parse_iso_datetime(start_raw)
            except Exception:
                continue
            local_start = to_local(start_dt).replace(minute=0, second=0, microsecond=0)
            precip_val = (period.get("probabilityOfPrecipitation") or {}).get("value")
            points.append(
                {
                    "time": start_dt,
                    "label": _format_hour_label(start_dt),
                    "temp": period.get("temperature"),
                    "precip": precip_val if precip_val is not None else 0,
                    "cloud": clouds.get(local_start),
                }
            )
        return points

    def _build_daily_periods(forecast_json: dict) -> list[dict]:
        periods = (forecast_json.get("properties") or {}).get("periods") or []
        days: list[dict] = []
        for idx, period in enumerate(periods):
            if not period.get("isDaytime"):
                continue
            low_temp = None
            if idx + 1 < len(periods):
                nxt = periods[idx + 1]
                if not nxt.get("isDaytime"):
                    low_temp = nxt.get("temperature")
            days.append(
                {
                    "name": (period.get("name") or "DAY").upper(),
                    "high": period.get("temperature"),
                    "low": low_temp,
                    "unit": period.get("temperatureUnit", "F"),
                    "short": period.get("shortForecast"),
                    "is_day": bool(period.get("isDaytime", True)),
                }
            )
            if len(days) >= 7:
                break
        return days

    def _build_forecast_periods(forecast_json: dict) -> list[dict]:
        periods = (forecast_json.get("properties") or {}).get("periods") or []
        out: list[dict] = []
        for period in periods[:2]:
            precip_val = (period.get("probabilityOfPrecipitation") or {}).get("value")
            out.append(
                {
                    "name": period.get("name"),
                    "temperature": period.get("temperature"),
                    "unit": period.get("temperatureUnit", "F"),
                    "wind": period.get("windSpeed"),
                    "wind_dir": period.get("windDirection"),
                    "precip": precip_val,
                    "short": period.get("shortForecast"),
                    "detailed": period.get("detailedForecast"),
                    "is_day": bool(period.get("isDaytime", True)),
                }
            )
        return out

    def _build_latest_rows(observations: list[dict]) -> list[dict]:
        rows: list[dict] = []
        for entry in observations:
            obs_props = (entry.get("observation") or {}).get("properties", {})
            station_props = (entry.get("station") or {}).get("properties", {})
            raw_name = station_props.get("name") or station_props.get("stationIdentifier") or "Station"
            name = canonical_city_name(raw_name)
            temp_c = (obs_props.get("temperature") or {}).get("value")
            temp_f = safe_round(c_to_f(temp_c), 1) if temp_c is not None else None
            temp_display = f"{temp_f:.1f}°F" if temp_f is not None else "--"
            wind_speed_ms = (obs_props.get("windSpeed") or {}).get("value")
            wind_speed_mph = safe_round(ms_to_mph(wind_speed_ms), 1) if wind_speed_ms is not None else None
            wind_dir = format_cardinal((obs_props.get("windDirection") or {}).get("value"))
            wind_display = "Calm" if wind_speed_mph is None else f"{wind_dir} {wind_speed_mph:.1f} mph"
            condition = obs_props.get("textDescription") or "--"
            rows.append(
                {
                    "name": name,
                    "temp": temp_display,
                    "condition": condition,
                    "wind": wind_display,
                }
            )
        return rows

    def _pick_observation_points(observations: list[dict], targets: Iterable[dict]) -> list[dict]:
        inventory: list[dict] = []
        points: list[dict] = []
        seen: set[str] = set()
        for entry in observations:
            obs_props = (entry.get("observation") or {}).get("properties", {})
            station = (entry.get("station") or {}).get("properties", {})
            coords = ((entry.get("station") or {}).get("geometry") or {}).get("coordinates") or [None, None]
            lon_obs, lat_obs = (coords + [None, None])[:2]
            if lat_obs is None or lon_obs is None:
                continue
            raw_name = station.get("name") or station.get("stationIdentifier") or "Station"
            city = canonical_city_name(raw_name)
            key = city.upper()
            if key in seen:
                continue
            seen.add(key)

            temp_c = (obs_props.get("temperature") or {}).get("value")
            temp_f = safe_round(c_to_f(temp_c), 1) if temp_c is not None else None
            temp_display = f"{temp_f:.1f}°F" if temp_f is not None else "--"

            timestamp = obs_props.get("timestamp")
            is_day = True
            if timestamp:
                try:
                    obs_dt = to_local(parse_iso_datetime(timestamp))
                    is_day = 6 <= obs_dt.hour < 18
                except Exception:
                    pass

            point = {
                "name": city,
                "lat": lat_obs,
                "lon": lon_obs,
                "temp": temp_display,
                "condition": obs_props.get("textDescription"),
                "is_day": is_day,
            }
            points.append(point)
            inventory.append({
                "name": city,
                "lat": lat_obs,
                "lon": lon_obs,
                "temp": temp_display,
                "condition": obs_props.get("textDescription"),
                "is_day": is_day,
            })

        if points:
            points = points[:8]

        if not points:
            for entry in observations[:6]:
                obs_props = (entry.get("observation") or {}).get("properties", {})
                station = (entry.get("station") or {}).get("properties", {})
                coords = ((entry.get("station") or {}).get("geometry") or {}).get("coordinates") or [None, None]
                lon_obs, lat_obs = (coords + [None, None])[:2]
                if lat_obs is None or lon_obs is None:
                    continue
                raw_name = station.get("name") or station.get("stationIdentifier") or "Station"
                temp_c = (obs_props.get("temperature") or {}).get("value")
                temp_f = safe_round(c_to_f(temp_c), 1) if temp_c is not None else None
                temp_display = f"{temp_f:.1f}°F" if temp_f is not None else "--"
                points.append(
                    {
                        "name": canonical_city_name(raw_name),
                        "lat": lat_obs,
                        "lon": lon_obs,
                        "temp": temp_display,
                        "condition": obs_props.get("textDescription"),
                        "is_day": True,
                    }
                )
                if len(points) >= 8:
                    break

        if not inventory:
            inventory = [{
                "name": canonical_city_name((entry.get("station") or {}).get("properties", {}).get("name")),
                "lat": ((entry.get("station") or {}).get("geometry") or {}).get("coordinates", [None, None])[1],
                "lon": ((entry.get("station") or {}).get("geometry") or {}).get("coordinates", [None, None])[0],
                "temp": "--",
                "condition": (entry.get("observation") or {}).get("properties", {}).get("textDescription"),
                "is_day": True,
            } for entry in observations[:8] if entry]

        major_points: list[dict] = []
        for target in targets:
            best = None
            best_dist = float("inf")
            for record in inventory:
                if record.get("lat") is None or record.get("lon") is None:
                    continue
                dist = haversine_miles(record["lat"], record["lon"], target["lat"], target["lon"])
                if dist < best_dist:
                    best = record
                    best_dist = dist
            if best and best_dist <= target.get("max_obs_distance", 80.0):
                major_points.append(
                    {
                        "name": target["name"],
                        "lat": target["lat"],
                        "lon": target["lon"],
                        "temp": best["temp"],
                        "condition": best["condition"],
                        "is_day": best.get("is_day", True),
                    }
                )
        if major_points:
            return major_points[:8]
        return points[:8]

    def _forecast_points(targets: Iterable[dict], forecast_json: dict) -> list[dict]:
        points: list[dict] = []
        for target in targets:
            try:
                period = client.point_forecast(target["lat"], target["lon"])
            except Exception:
                period = None
            if not period:
                continue
            temp = period.get("temperature")
            unit = period.get("temperatureUnit", "F")
            points.append(
                {
                    "name": target["name"],
                    "lat": target["lat"],
                    "lon": target["lon"],
                    "forecast_temp": f"{temp}°{unit}" if temp is not None else "--",
                    "forecast_short": period.get("shortForecast"),
                    "is_day": period.get("isDaytime", True),
                }
            )
        if not points:
            try:
                first_day = next(
                    p for p in (forecast_json.get("properties") or {}).get("periods", []) if p.get("isDaytime")
                )
            except Exception:
                first_day = None
            if first_day:
                temp = first_day.get("temperature")
                unit = first_day.get("temperatureUnit", "F")
                points.append(
                    {
                        "name": cfg.location_name,
                        "lat": lat,
                        "lon": lon,
                        "forecast_temp": f"{temp}°{unit}" if temp is not None else "--",
                        "forecast_short": first_day.get("shortForecast"),
                        "is_day": first_day.get("isDaytime", True),
                    }
                )
        return points[:8]

    def _compose_map(points: list[dict]) -> tuple[object | None, tuple[float, float, float, float] | None]:
        if not points:
            return None, None
        coords = [
            (p.get("lat"), p.get("lon"))
            for p in points
            if p.get("lat") is not None and p.get("lon") is not None
        ]
        bounds = compute_bounds(coords, lat, lon, pad_degrees=0.2, min_span=2.0, max_span=None)
        map_width = max(200, render_width - int(round(192 * scale)))
        map_height = max(200, render_height - int(round(472 * scale)))
        try:
            view = map_tiles.compose_base_map(
                bounds[0],
                bounds[1],
                bounds[2],
                bounds[3],
                map_width,
                map_height,
                cfg.user_agent,
            )
        except Exception:
            view = None
        if view is not None:
            return view.image.copy(), view.bounds
        return None, bounds

    def _radar_getter() -> list:
        frames = map_tiles.get_cached_radar_frames()
        if not frames:
            return []
        last = radar_state.get("last_ts", 0.0)
        new_items = []
        for frame in frames:
            ts = frame.get("timestamp") or 0
            if ts > last:
                image = frame.get("image")
                label = frame.get("label") or ""
                if image is not None:
                    new_items.append((image.copy(), label))
        if new_items:
            radar_state["last_ts"] = max(frame.get("timestamp", last) or last for frame in frames)
        return new_items

    def _compose_ticker_text() -> str:
        """NOAA alerts + RSS titles + NOAA alerts, as one continuous string."""
        sep = "  •  "
        alerts = _alerts_text().strip()
        rss_titles = rss_cache.get_titles()
        if rss_titles:
            middle = sep.join(t.strip() for t in rss_titles if t.strip())
            if alerts:
                return f"{alerts}{sep}{middle}{sep}{alerts}"
            return middle
        # No RSS available; just alerts
        return alerts

    def fetch_all():
        data: dict[str, object] = {}

        # Build ticker text (single string with desired order)
        data["ticker_text"] = _compose_ticker_text()

        try:
            forecast_json = client.forecast()
        except Exception:
            forecast_json = {}
        try:
            hourly_json = client.hourly()
        except Exception:
            hourly_json = {}
        try:
            grid_data = client.forecast_grid_data()
        except Exception:
            grid_data = {}
        try:
            observations = client.latest_observations(limit=40)
        except Exception:
            observations = []

        data["current"] = prepare_current_conditions(forecast_json, observations[0] if observations else None)
        data["daily_days"] = _build_daily_periods(forecast_json)
        data["forecast_periods"] = _build_forecast_periods(forecast_json)
        data["hourly_points"] = _build_hourly_points(hourly_json, grid_data)
        data["latest_rows"] = _build_latest_rows(observations)

        major_targets = major_cities_near(lat, lon, max_distance=360.0, max_results=12)
        data["regional_points"] = _pick_observation_points(observations, major_targets)
        data["forecast_points"] = _forecast_points(major_targets, forecast_json)

        regional_image, regional_bounds = _compose_map(data["regional_points"])
        forecast_image, forecast_bounds = _compose_map(data["forecast_points"])
        data["regional_map_image"] = regional_image
        data["regional_map_bounds"] = regional_bounds
        data["forecast_map_image"] = forecast_image
        data["forecast_map_bounds"] = forecast_bounds

        radar_width = max(200, render_width - int(round(160 * scale)))
        radar_height = max(200, render_height - int(round(432 * scale)))
        try:
            map_tiles.ensure_radar_frames(
                announce=False,
                center_lat=lat,
                center_lon=lon,
                width=radar_width,
                height=radar_height,
                user_agent=cfg.user_agent,
                span_degrees=3.0,
                max_frames=6,
            )
        except Exception:
            pass
        data["radar_new_frames"] = _radar_getter

        return data

    ds = DataStore(fetcher=fetch_all, interval_sec=cfg.data_interval_sec)
    ds.start()
    return ds


class PageCycler:
    """Toggles visibility of page layer groups on a fixed cadence."""

    def __init__(self, pages: list[dict[str, object]], interval_sec: float):
        self.pages = pages
        self.interval = max(1.0, float(interval_sec or 1.0))
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._index = 0

    def activate(self, index: int) -> None:
        if not self.pages:
            return
        self._index = index % len(self.pages)
        for idx, page in enumerate(self.pages):
            visible = idx == self._index
            for layer in page.get("layers", []):
                if isinstance(layer, Layer):
                    layer.set_visible(visible)

    def start(self) -> None:
        if not self.pages:
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, name="page-cycler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=1.0)
            self._thread = None

    def _loop(self) -> None:
        while not self._stop_event.wait(self.interval):
            if not self.pages:
                continue
            self.activate(self._index + 1)


def _build_layers(
    cfg,
    data_store: DataStore,
    render_width: int,
    render_height: int,
    scale: float,
) -> tuple[list[Layer], PageCycler]:
    layers: list[Layer] = []
    pages: list[dict[str, object]] = []

    def _s(val: float, minimum: int = 0) -> int:
        return max(minimum, int(round(val * scale)))

    def content_bounds(top_gutter: int = 220, bottom_extra: int = 24) -> tuple[int, int, int, int]:
        x = _s(48)
        y = top_gutter
        w = max(_s(320, 1), render_width - _s(96))
        ticker_h = _s(64, 1)
        h = max(_s(160, 1), render_height - (y + ticker_h + _s(20) + bottom_extra))
        return x, y, w, h

    def current_temp_text() -> str:
        current = data_store.read().get("current") or {}
        temp_f = current.get("temp_f")
        if isinstance(temp_f, (float, int)):
            return f"{int(round(temp_f))}°F"
        forecast_temp = current.get("forecast_temp")
        if forecast_temp is not None:
            unit = current.get("forecast_unit", "F")
            return f"{forecast_temp}°{unit}"
        return ""

    # Clock (top-right)
    clock_w, clock_h = _s(480, 1), _s(200, 1)
    clock_x = render_width - clock_w - _s(48)
    clock_y = _s(24)
    clock_layer = ClockLayer(
        x=clock_x,
        y=clock_y,
        w=clock_w,
        h=clock_h,
        min_interval=1.0,
        temp_supplier=current_temp_text,
        scale=scale,
    )
    clock_layer.z = 200
    layers.append(clock_layer)

    # Ticker (bottom) — reads the composed string from the datastore
    ticker_h = _s(64, 1)
    ticker_y = render_height - ticker_h - _s(20)
    ticker_layer = TickerLayer(
        x=_s(48),
        y=ticker_y,
        w=render_width - _s(96),
        h=ticker_h,
        min_interval=1 / 30.0,
        px_per_sec=max(1, int(round(cfg.ticker_speed_px_per_sec * scale))),
        get_text=lambda: (data_store.read().get("ticker_text") or "Weather data loading...").strip(),
        scale=scale,
    )
    ticker_layer.z = 200
    layers.append(ticker_layer)

    def add_page(name: str, builder: Callable[[tuple[int, int, int, int]], list[Layer]], *, top: int) -> None:
        bounds = content_bounds(_s(top), _s(24))
        chrome = ChromeLayer(
            width=render_width,
            height=render_height,
            location_name=cfg.location_name,
            scale=scale,
        )
        chrome.z = 0
        page_layers = [chrome]
        for lyr in builder(bounds):
            lyr.z = max(getattr(lyr, "z", 50), 50)
            page_layers.append(lyr)
        for lyr in page_layers:
            lyr.set_visible(False)
        pages.append({"name": name, "layers": page_layers})
        layers.extend(page_layers)

    add_page(
        "current",
        lambda b: [
            CurrentLayer(
                x=b[0],
                y=b[1],
                w=b[2],
                h=min(_s(420, 1), b[3]),
                get_data=lambda: data_store.read().get("current", {}),
                min_interval=5.0,
                scale=scale,
            )
        ],
        top=240,
    )

    add_page(
        "radar",
        lambda b: [
            RadarLayer(
                x=b[0],
                y=b[1],
                w=b[2],
                h=b[3],
                min_interval=0.25,
                get_new_frames=lambda: (lambda fn: fn() if callable(fn) else [])(
                    data_store.read().get("radar_new_frames")
                ),
                frame_hold=3,
                scale=scale,
            )
        ],
        top=220,
    )

    add_page(
        "forecast_map",
        lambda b: [
            ForecastMapLayer(
                x=b[0],
                y=b[1],
                w=b[2],
                h=b[3],
                get_points=lambda: data_store.read().get("forecast_points", []) or [],
                get_map=lambda: (lambda im: im.copy() if im is not None else None)(
                    data_store.read().get("forecast_map_image")
                ),
                get_bounds=lambda: data_store.read().get("forecast_map_bounds"),
                min_interval=15.0,
                scale=scale,
            )
        ],
        top=240,
    )

    add_page(
        "regional",
        lambda b: [
            RegionalLayer(
                x=b[0],
                y=b[1],
                w=b[2],
                h=b[3],
                get_points=lambda: data_store.read().get("regional_points", []) or [],
                get_map=lambda: (lambda im: im.copy() if im is not None else None)(
                    data_store.read().get("regional_map_image")
                ),
                get_bounds=lambda: data_store.read().get("regional_map_bounds"),
                min_interval=20.0,
                scale=scale,
            )
        ],
        top=240,
    )

    add_page(
        "hourly_graph",
        lambda b: [
            HourlyGraphLayer(
                x=b[0],
                y=b[1],
                w=b[2],
                h=b[3],
                get_points=lambda: data_store.read().get("hourly_points", []) or [],
                min_interval=15.0,
                scale=scale,
            )
        ],
        top=260,
    )

    add_page(
        "daily",
        lambda b: [
            DailyLayer(
                x=b[0],
                y=b[1],
                w=b[2],
                h=b[3],
                get_days=lambda: data_store.read().get("daily_days", []) or [],
                min_interval=30.0,
                scale=scale,
            )
        ],
        top=260,
    )

    add_page(
        "forecast_text",
        lambda b: [
            ForecastTextLayer(
                x=b[0],
                y=b[1],
                w=b[2],
                h=b[3],
                get_periods=lambda: data_store.read().get("forecast_periods", []) or [],
                min_interval=30.0,
                scale=scale,
            )
        ],
        top=260,
    )

    add_page(
        "latest",
        lambda b: [
            LatestLayer(
                x=b[0],
                y=b[1],
                w=b[2],
                h=b[3],
                get_rows=lambda: data_store.read().get("latest_rows", []) or [],
                min_interval=15.0,
                scale=scale,
            )
        ],
        top=252,
    )

    cycler = PageCycler(pages, cfg.page_duration_sec)
    if pages:
        cycler.activate(0)

    return layers, cycler


def main(argv: Optional[list[str]] = None) -> int:
    cfg = parse_args(argv)

    # Resolve ZIP to coordinates, falling back to config defaults if lookup fails.
    if (cfg.lat is None or cfg.lon is None) and cfg.zip:
        lookup = resolve_zip(cfg.zip)
        if lookup:
            if cfg.lat is None:
                cfg.lat = lookup.get("lat")
            if cfg.lon is None:
                cfg.lon = lookup.get("lon")
            if cfg.location_name == "Weatharr Station":
                city = lookup.get("city") or ""
                state = lookup.get("state") or ""
                cfg.location_name = f"{city}, {state}".strip(", ") or cfg.location_name

    set_timezone(getattr(cfg, "timezone", None), cfg.lat, cfg.lon)

    def _discover_music_dir() -> Path | None:
        music_dir = Path(cfg.music_dir or "").expanduser()
        if music_dir.exists():
            return music_dir
        here = Path(__file__).resolve()
        for parent in (here.parent, *here.parents[1:4]):
            candidate = parent / "assets" / "music"
            if candidate.exists():
                return candidate
        return None

    def _build_music_playlist() -> str | None:
        music_dir = _discover_music_dir()
        if not music_dir:
            return None
        tracks = [
            p for p in sorted(music_dir.iterdir())
            if p.is_file() and p.suffix.lower() in {".mp3", ".m4a", ".aac", ".wav", ".flac", ".ogg", ".oga"}
        ]
        if not tracks:
            return None
        playlist_path = Path(tempfile.gettempdir()) / "weatherstream_music_playlist.txt"
        with playlist_path.open("w", encoding="utf-8") as fh:
            for track in tracks:
                escaped = track.as_posix().replace("'", "'\\''")
                fh.write(f"file '{escaped}'\n")
        return str(playlist_path)

    music_playlist_path = _build_music_playlist()
    music_fifo_path = None

    page_cycler: PageCycler | None = None

    output_width = int(cfg.width or BASE_WIDTH)
    output_height = int(cfg.height or BASE_HEIGHT)
    if output_width <= 0:
        output_width = BASE_WIDTH
    if output_height <= 0:
        output_height = BASE_HEIGHT
    scale = min(output_width / BASE_WIDTH, output_height / BASE_HEIGHT)
    render_width = output_width
    render_height = output_height

    # Output streamer (encoder CFR = cfg.output_fps). We will re-send last frame if nothing changed.
    streamer = FFMPEGStreamer(
        width=render_width,
        height=render_height,
        fps=cfg.output_fps,
        out_url=getattr(cfg, "out", None) or getattr(cfg, "out_url", None) or "file:out.ts",
        out_width=output_width,
        out_height=output_height,

        voice_fifo=getattr(cfg, "voice_fifo", None),
        music_fifo=music_fifo_path,
        music_playlist=music_playlist_path,

        video_encoder=getattr(cfg, "video_encoder", "auto"),
        encoder_preset=getattr(cfg, "encoder_preset", "veryfast"),

        vb_kbps=getattr(cfg, "video_kbps", 3500),
        ab_kbps=getattr(cfg, "audio_kbps", 128),

        gop_seconds=getattr(cfg, "gop_seconds", 1.0),
        force_cfr=getattr(cfg, "force_cfr", False),
        use_wallclock_ts=getattr(cfg, "use_wallclock_ts", False),

        srt_latency_ms=getattr(cfg, "srt_latency_ms", 120),
        udp_pkt_size=getattr(cfg, "udp_pkt_size", 1316),

        pat_period=getattr(cfg, "pat_period", 0.5),
        pcr_period_ms=getattr(cfg, "pcr_period_ms", 40),
        flush_packets=getattr(cfg, "flush_packets", False),

        print_cmd=True,
    )
    streamer.start()

    # Data loop
    data_store = _make_datastore(cfg, render_width, render_height, scale)

    # Layers + compositor
    layers, page_cycler = _build_layers(cfg, data_store, render_width, render_height, scale)
    comp = Compositor(w=render_width, h=render_height)

    # Event-driven scheduler (per-layer cadence + CFR wrapper)
    sched = Scheduler(layers=layers, cfr_hz=cfg.output_fps)

    if page_cycler:
        page_cycler.start()

    # The present() call gives an RGBA frame. Send bytes to ffmpeg.
    def on_present(image):
        try:
            streamer.send(image.tobytes())
        except Exception as e:
            print(f"[stream] write failed: {e!r}", flush=True)

    def _build_disable_checker():
        plugin_key = os.environ.get("WEATHARR_PLUGIN_KEY")
        if not plugin_key:
            return None
        interval = float(os.environ.get("WEATHARR_DISABLE_CHECK_INTERVAL", "5"))
        station_id_raw = os.environ.get("WEATHARR_STATION_ID", "1")
        try:
            station_id = max(1, int(station_id_raw))
        except (TypeError, ValueError):
            station_id = 1
        last_check = 0.0
        django_ready = False

        def _parse_bool(val, default: bool | None = None) -> bool | None:
            if isinstance(val, bool):
                return val
            if isinstance(val, int) and val in (0, 1):
                return bool(val)
            if isinstance(val, str):
                normalized = val.strip().lower()
                if normalized in ("true", "1", "yes", "y", "on"):
                    return True
                if normalized in ("false", "0", "no", "n", "off"):
                    return False
            return default

        def _should_stop():
            nonlocal last_check, django_ready
            now = time.time()
            if now - last_check < interval:
                return False
            last_check = now
            try:
                import django
                if not django_ready:
                    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dispatcharr.settings")
                    django.setup()
                    django_ready = True
                from apps.plugins.models import PluginConfig
                cfg = PluginConfig.objects.filter(key=plugin_key).first()
                if not cfg or not cfg.enabled:
                    return True
                settings = cfg.settings or {}
                field_name = f"station_{station_id}_enabled"
                default_enabled = station_id == 1
                enabled_val = _parse_bool(settings.get(field_name), default=default_enabled)
                if enabled_val is False:
                    return True
                return False
            except Exception:
                return False

        return _should_stop

    should_stop = _build_disable_checker()

    # Run until Ctrl+C
    try:
        sched.run_forever(compositor=comp, on_present=on_present, should_stop=should_stop)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            data_store.stop()
        except Exception:
            pass
        try:
            if page_cycler:
                page_cycler.stop()
        except Exception:
            pass
        try:
            streamer.stop()
        except Exception:
            pass
        if music_playlist_path:
            try:
                Path(music_playlist_path).unlink(missing_ok=True)
            except Exception:
                pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
