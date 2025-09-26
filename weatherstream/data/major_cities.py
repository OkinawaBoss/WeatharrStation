from __future__ import annotations

import csv
from dataclasses import dataclass
from functools import lru_cache
from math import atan2, cos, radians, sin, sqrt
from pathlib import Path
from typing import Iterable, List, Sequence


def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 3958.8
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = sin(dphi / 2.0) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2.0) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return r * c


DATA_PATH = Path(__file__).with_name("us_cities.csv")


@dataclass(frozen=True)
class City:
    name: str
    lat: float
    lon: float
    population: int


_MANUAL_ALIAS_KEYWORDS: dict[str, Sequence[str]] = {
    "Houston": ["HOUSTON", "INTERCONTINENTAL", "ELLINGTON", "HOBBY"],
    "Galveston": ["GALVESTON"],
    "Lake Charles": ["LAKE CHARLES"],
    "Baton Rouge": ["BATON ROUGE"],
    "New Orleans": ["NEW ORLEANS"],
    "Austin": ["AUSTIN"],
    "San Antonio": ["SAN ANTONIO"],
    "Dallas": ["DALLAS"],
    "Corpus Christi": ["CORPUS", "CORPUS CHRISTI"],
    "Victoria": ["VICTORIA"],
}


@lru_cache(maxsize=1)
def _city_catalog() -> tuple[City, ...]:
    catalog: list[City] = []
    if not DATA_PATH.exists():
        return tuple()
    with DATA_PATH.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            name = (row.get("name") or "").strip()
            if not name:
                continue
            try:
                lat = float(row.get("lat", ""))
                lon = float(row.get("lon", ""))
                pop_raw = (row.get("pop") or "0").replace(",", "").strip()
                population = int(float(pop_raw))
            except (TypeError, ValueError):
                continue
            catalog.append(City(name=name, lat=lat, lon=lon, population=population))
    catalog.sort(key=lambda c: c.population, reverse=True)
    return tuple(catalog)


@lru_cache(maxsize=1)
def _alias_items() -> tuple[tuple[str, str], ...]:
    items: list[tuple[str, str]] = []
    for canonical, variants in _MANUAL_ALIAS_KEYWORDS.items():
        for keyword in variants:
            keyword = keyword.upper().strip()
            if keyword:
                items.append((keyword, canonical))
    for city in _city_catalog():
        key = city.name.upper().strip()
        if key:
            items.append((key, city.name))
    # Longer keywords first so "SAN ANTONIO" wins before "SAN"
    items.sort(key=lambda kv: len(kv[0]), reverse=True)
    return tuple(items)


def canonical_city_name(raw: str) -> str:
    upper = (raw or "").upper()
    if not upper:
        return "Station"
    for keyword, canonical in _alias_items():
        if keyword in upper:
            return canonical
    return (raw or "").split(",")[0].strip() or "Station"


def _iter_cities() -> tuple[City, ...]:
    return _city_catalog()


def _select_candidates(
    lat: float,
    lon: float,
    *,
    max_distance: float,
    population_cutoffs: Sequence[int],
) -> list[tuple[float, City]]:
    catalog = _iter_cities()
    if not catalog:
        return []
    for threshold in population_cutoffs:
        candidates: list[tuple[float, City]] = []
        for city in catalog:
            if city.population < threshold:
                continue
            dist = _haversine_miles(lat, lon, city.lat, city.lon)
            if dist <= max_distance:
                candidates.append((dist, city))
        if candidates:
            return candidates
    return []


def major_cities_near(
    lat: float,
    lon: float,
    *,
    max_distance: float = 350.0,
    min_population: int = 150_000,
    max_results: int = 10,
) -> List[dict]:
    catalog = _iter_cities()
    if not catalog:
        return []

    population_cutoffs: tuple[int, ...] = (
        min_population,
        100_000,
        50_000,
        25_000,
        10_000,
        0,
    )

    selected = _select_candidates(
        lat,
        lon,
        max_distance=max_distance,
        population_cutoffs=population_cutoffs,
    )

    if not selected:
        # Fall back to the closest large cities even if they are just outside the radius
        selected = [
            (_haversine_miles(lat, lon, city.lat, city.lon), city)
            for city in catalog[: max_results * 5]
        ]

    selected.sort(key=lambda item: (item[0], -item[1].population))
    trimmed = selected[:max_results]

    results: List[dict] = []
    for dist, city in trimmed:
        obs_radius = max(60.0, min(120.0, dist * 0.75 + 35.0))
        results.append(
            {
                "name": city.name,
                "lat": city.lat,
                "lon": city.lon,
                "population": city.population,
                "max_obs_distance": obs_radius,
                "max_home_distance": max_distance,
            }
        )
    return results


def nearest_observation_target(lat: float, lon: float, targets: Iterable[dict]) -> dict | None:
    best = None
    best_dist = float("inf")
    for target in targets:
        dist = _haversine_miles(lat, lon, target["lat"], target["lon"])
        if dist < best_dist:
            best = target
            best_dist = dist
    return best
