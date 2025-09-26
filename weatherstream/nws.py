from __future__ import annotations
import time
from io import BytesIO
from typing import Any, Dict, Optional

import requests
from PIL import Image


class NWSClient:
    def __init__(self, lat: float, lon: float, user_agent: str, cache_ttl: int = 180):
        self.lat = lat
        self.lon = lon
        self.ua = user_agent
        self.ttl = cache_ttl
        self._cache: Dict[str, tuple[float, Any]] = {}
        self._binary_cache: Dict[str, tuple[float, Any]] = {}
        self._forecast_url: Optional[str] = None
        self._hourly_url: Optional[str] = None
        self._observation_stations_url: Optional[str] = None
        self._gridpoint_url: Optional[str] = None
        self._radar_station: Optional[str] = None
        self._points_data: Optional[Dict[str, Any]] = None

    def _get(self, url: str) -> Any:
        now = time.time()
        cached = self._cache.get(url)
        if cached and now - cached[0] < self.ttl:
            return cached[1]
        r = requests.get(
            url,
            headers={
                "User-Agent": self.ua,
                "Accept": "application/geo+json",
            },
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        self._cache[url] = (now, data)
        return data

    def _resolve_points(self) -> None:
        if self._forecast_url and self._hourly_url:
            return
        base = f"https://api.weather.gov/points/{self.lat},{self.lon}"
        data = self._get(base)
        self._points_data = data
        props = data.get("properties", {})
        self._forecast_url = props.get("forecast")
        self._hourly_url = props.get("forecastHourly")
        self._observation_stations_url = props.get("observationStations")
        self._gridpoint_url = props.get("forecastGridData")
        self._radar_station = props.get("radarStation")
        if not self._forecast_url or not self._hourly_url:
            raise RuntimeError(
                "NWS did not return forecast URLs for the given coordinates."
            )

    @property
    def radar_station(self) -> Optional[str]:
        if not self._radar_station:
            self._resolve_points()
        return self._radar_station

    def observation_stations(self, limit: int | None = None) -> list[str]:
        self._resolve_points()
        if not self._observation_stations_url:
            return []
        data = self._get(self._observation_stations_url)
        stations = data.get("observationStations", [])
        if limit is not None:
            return stations[:limit]
        return stations

    def station_metadata(self, station_url: str) -> Any:
        return self._get(station_url)

    def latest_observation(self) -> Optional[Dict[str, Any]]:
        stations = self.observation_stations(limit=1)
        if not stations:
            return None
        return self._get(stations[0] + "/observations/latest")

    def latest_observations(self, limit: int = 6) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for station_url in self.observation_stations(limit=limit):
            try:
                obs = self._get(station_url + "/observations/latest")
                meta = self.station_metadata(station_url)
            except Exception:
                continue
            if not obs:
                continue
            items.append({"station": meta, "observation": obs})
        return items

    def forecast_grid_data(self) -> Dict[str, Any]:
        self._resolve_points()
        if not self._gridpoint_url:
            return {}
        try:
            return self._get(self._gridpoint_url)
        except Exception:
            return {}

    def point_forecast(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        url = f"https://api.weather.gov/points/{lat},{lon}"
        try:
            point = self._get(url)
        except Exception:
            return None
        props = point.get("properties", {})
        forecast_url = props.get("forecast")
        if not forecast_url:
            return None
        try:
            forecast = self._get(forecast_url)
        except Exception:
            return None
        periods = forecast.get("properties", {}).get("periods", [])
        for p in periods:
            if p.get("isDaytime"):
                return p
        return periods[0] if periods else None

    def radar_composite(self) -> Optional[Image.Image]:
        url = "https://radar.weather.gov/ridge/standard/CONUS_Composite_Reflectivity.png"
        now = time.time()
        cached = self._binary_cache.get(url)
        if cached and now - cached[0] < self.ttl:
            cached_image = cached[1]
            if isinstance(cached_image, Image.Image):
                return cached_image.copy()
        try:
            resp = requests.get(url, headers={"User-Agent": self.ua}, timeout=15)
            resp.raise_for_status()
        except Exception:
            return None
        try:
            image = Image.open(BytesIO(resp.content)).convert("RGBA")
        except Exception:
            return None
        self._binary_cache[url] = (now, image)
        return image.copy()

    def forecast(self) -> Any:
        self._resolve_points()
        return self._get(self._forecast_url)

    def hourly(self) -> Any:
        self._resolve_points()
        return self._get(self._hourly_url)

    def alerts(self) -> Any:
        url = f"https://api.weather.gov/alerts/active?point={self.lat},{self.lon}"
        return self._get(url)
