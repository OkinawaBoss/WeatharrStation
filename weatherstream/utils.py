from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore
from typing import Iterable, Optional, Tuple


def c_to_f(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return value * 9.0 / 5.0 + 32.0


def ms_to_mph(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return value * 2.23693629


def meters_to_miles(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return value * 0.000621371


def meters_to_feet(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return value * 3.2808399


def pascal_to_inhg(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return value * 0.00029529983071445


def safe_round(value: Optional[float], digits: int = 0) -> Optional[float]:
    if value is None:
        return None
    return round(value, digits)


def format_cardinal(degrees: Optional[float]) -> str:
    if degrees is None:
        return "--"
    dirs = [
        "N", "NNE", "NE", "ENE",
        "E", "ESE", "SE", "SSE",
        "S", "SSW", "SW", "WSW",
        "W", "WNW", "NW", "NNW",
    ]
    idx = int((degrees % 360) / 22.5 + 0.5) % 16
    return dirs[idx]


def compute_heat_index(temp_f: Optional[float], humidity: Optional[float]) -> Optional[float]:
    if temp_f is None or humidity is None:
        return None
    if temp_f < 80 or humidity < 40:
        return temp_f
    t = temp_f
    rh = humidity
    hi = (
        -42.379
        + 2.04901523 * t
        + 10.14333127 * rh
        - 0.22475541 * t * rh
        - 0.00683783 * t * t
        - 0.05481717 * rh * rh
        + 0.00122874 * t * t * rh
        + 0.00085282 * t * rh * rh
        - 0.00000199 * t * t * rh * rh
    )
    if rh < 13 and 80 <= t <= 112:
        hi -= ((13 - rh) / 4) * math.sqrt((17 - abs(t - 95)) / 17)
    elif rh > 85 and 80 <= t <= 87:
        hi += ((rh - 85) / 10) * ((87 - t) / 5)
    return hi


def parse_iso_datetime(value: str) -> datetime:
    if not value:
        raise ValueError("Empty ISO datetime")
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


_DURATION_RE = re.compile(
    r"P"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T"
    r"(?:(?P<hours>\d+)H)?"
    r"(?:(?P<minutes>\d+)M)?"
    r"(?:(?P<seconds>\d+)S)?"
    r")?"
)


def parse_iso_duration(value: str) -> timedelta:
    match = _DURATION_RE.fullmatch(value)
    if not match:
        return timedelta(0)
    parts = {k: int(v) if v else 0 for k, v in match.groupdict().items()}
    return timedelta(**parts)


def parse_valid_time(value: str) -> Tuple[datetime, datetime]:
    start_str, duration_str = value.split("/")
    start_dt = parse_iso_datetime(start_str)
    duration = parse_iso_duration(duration_str)
    return start_dt, start_dt + duration


_TZ: timezone | None = None
_TZ_NAME: str | None = None


def timezone_from_coordinates(lat: float | None, lon: float | None) -> Optional[str]:
    if lat is None or lon is None:
        return None
    url = f"https://api.weather.gov/points/{lat},{lon}"
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code != 200:
            return None
        data = resp.json()
        return (data.get("properties") or {}).get("timeZone")
    except Exception:
        return None


def set_timezone(name: str | None = None, lat: float | None = None, lon: float | None = None) -> None:
    global _TZ, _TZ_NAME
    tz_name = name
    if not tz_name:
        tz_name = timezone_from_coordinates(lat, lon)

    if not tz_name or ZoneInfo is None:
        if name and ZoneInfo is None:
            print("[timezone] zoneinfo not available; using system local time")
        _TZ = None
        _TZ_NAME = None
        return

    try:
        _TZ = ZoneInfo(tz_name)
        _TZ_NAME = tz_name
    except Exception:
        print(f"[timezone] unknown timezone '{tz_name}'; using system local time")
        _TZ = None
        _TZ_NAME = None


def now_local() -> datetime:
    if _TZ is not None:
        return datetime.now(_TZ)
    return datetime.now().astimezone()


def to_local(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone()


def compute_bounds(
    coordinates: Iterable[Tuple[float, float]],
    fallback_lat: float,
    fallback_lon: float,
    pad_degrees: float = 0.3,
    min_span: float = 1.0,
    max_span: Optional[float] = None,
) -> Tuple[float, float, float, float]:
    lats = [lat for lat, lon in coordinates if lat is not None and lon is not None]
    lons = [lon for lat, lon in coordinates if lat is not None and lon is not None]

    if not lats or not lons:
        half = max(min_span / 2.0, 0.5)
        return (
            fallback_lat - half,
            fallback_lon - half,
            fallback_lat + half,
            fallback_lon + half,
        )

    lat_min = min(lats)
    lat_max = max(lats)
    lon_min = min(lons)
    lon_max = max(lons)

    lat_span = max(lat_max - lat_min, min_span)
    lon_span = max(
        lon_max - lon_min,
        min_span / max(math.cos(math.radians((lat_min + lat_max) / 2.0)), 0.25),
    )

    lat_pad = pad_degrees
    lon_pad = pad_degrees / max(math.cos(math.radians((lat_min + lat_max) / 2.0)), 0.25)

    lat_center = (lat_min + lat_max) / 2.0
    lon_center = (lon_min + lon_max) / 2.0

    lat_min = lat_center - lat_span / 2.0 - lat_pad
    lat_max = lat_center + lat_span / 2.0 + lat_pad
    lon_min = lon_center - lon_span / 2.0 - lon_pad
    lon_max = lon_center + lon_span / 2.0 + lon_pad

    if max_span is not None:
        lat_center = (lat_min + lat_max) / 2.0
        lon_center = (lon_min + lon_max) / 2.0
        max_lat_span = max_span
        cos_lat = max(math.cos(math.radians(lat_center)), 0.25)
        max_lon_span = max_span / cos_lat
        current_lat_span = lat_max - lat_min
        current_lon_span = lon_max - lon_min
        if current_lat_span > max_lat_span:
            half = max_lat_span / 2.0
            lat_min = lat_center - half
            lat_max = lat_center + half
        if current_lon_span > max_lon_span:
            half = max_lon_span / 2.0
            lon_min = lon_center - half
            lon_max = lon_center + half

    return lat_min, lon_min, lat_max, lon_max


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> Optional[float]:
    if None in (lat1, lon1, lat2, lon2):
        return None
    r = 3958.8  # earth radius in miles
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def prepare_current_conditions(forecast_json: dict, observation_bundle: dict | None) -> dict:
    obs_props = ((observation_bundle or {}).get("observation") or {}).get("properties", {})
    station_props = ((observation_bundle or {}).get("station") or {}).get("properties", {})

    station_name = station_props.get("name") or station_props.get("stationIdentifier") or "Nearby Station"

    temp_c = (obs_props.get("temperature") or {}).get("value")
    dew_c = (obs_props.get("dewpoint") or {}).get("value")
    humidity = (obs_props.get("relativeHumidity") or {}).get("value")
    wind_speed_ms = (obs_props.get("windSpeed") or {}).get("value")
    wind_dir_val = (obs_props.get("windDirection") or {}).get("value")
    visibility_m = (obs_props.get("visibility") or {}).get("value")
    pressure_pa = (obs_props.get("barometricPressure") or {}).get("value")

    ceiling_m = None
    ceiling = obs_props.get("ceiling")
    if isinstance(ceiling, dict):
        ceiling_m = ceiling.get("value")
    if ceiling_m is None:
        for layer in obs_props.get("cloudLayers") or []:
            base = (layer.get("base") or {}).get("value")
            if base is not None:
                ceiling_m = base
                break

    temp_f = safe_round(c_to_f(temp_c), 1) if temp_c is not None else None
    dew_f = safe_round(c_to_f(dew_c), 1) if dew_c is not None else None
    humidity_pct = safe_round(humidity, 0) if humidity is not None else None
    wind_speed_mph = safe_round(ms_to_mph(wind_speed_ms), 1) if wind_speed_ms is not None else None
    wind_dir = format_cardinal(wind_dir_val)
    visibility_mi = safe_round(meters_to_miles(visibility_m), 1) if visibility_m is not None else None
    pressure_inhg = safe_round(pascal_to_inhg(pressure_pa), 2) if pressure_pa is not None else None
    heat_index = None
    if temp_f is not None and humidity_pct is not None:
        hi_val = compute_heat_index(temp_f, humidity_pct)
        heat_index = safe_round(hi_val, 1) if hi_val is not None else None
    ceiling_ft = safe_round(meters_to_feet(ceiling_m), 0) if ceiling_m is not None else None

    observed_conditions = obs_props.get("textDescription") or "Current conditions"
    observed_time = obs_props.get("timestamp")

    try:
        first_period = forecast_json["properties"]["periods"][0]
    except Exception:
        first_period = {}

    forecast_temp = first_period.get("temperature")
    forecast_unit = first_period.get("temperatureUnit", "F")
    forecast_short = first_period.get("shortForecast", "")
    forecast_is_day = bool(first_period.get("isDaytime", True))

    return {
        "station_name": station_name,
        "temp_f": temp_f,
        "dew_f": dew_f,
        "humidity": humidity_pct,
        "wind_speed_mph": wind_speed_mph,
        "wind_dir": wind_dir,
        "visibility_mi": visibility_mi,
        "pressure_inhg": pressure_inhg,
        "heat_index": heat_index,
        "ceiling_ft": ceiling_ft,
        "observed_conditions": observed_conditions,
        "observed_time": observed_time,
        "forecast_temp": forecast_temp,
        "forecast_unit": forecast_unit,
        "forecast_short": forecast_short,
        "forecast_is_day": forecast_is_day,
    }
