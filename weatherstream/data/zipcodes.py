from __future__ import annotations

from functools import lru_cache
from typing import Optional

import requests


def _normalize_zip(zip_code: str | int | None) -> str | None:
    if zip_code is None:
        return None
    if isinstance(zip_code, int):
        return f"{zip_code:05d}"
    digits = [ch for ch in str(zip_code) if ch.isdigit()]
    if len(digits) < 5:
        return None
    return "".join(digits[:5])


@lru_cache(maxsize=1024)
def _lookup_remote(code: str) -> Optional[dict]:
    url = f"https://api.zippopotam.us/us/{code}"
    try:
        resp = requests.get(url, timeout=8)
        if resp.status_code != 200:
            return None
        data = resp.json()
    except Exception:
        return None

    places = data.get("places") or []
    if not places:
        return None

    first = places[0]
    try:
        lat = float(first.get("latitude"))
        lon = float(first.get("longitude"))
    except (TypeError, ValueError):
        return None

    return {
        "zip": code,
        "lat": lat,
        "lon": lon,
        "city": (first.get("place name") or "").strip(),
        "state": (first.get("state abbreviation") or first.get("state") or "").strip(),
    }


def resolve_zip(zip_code: str | int) -> Optional[dict]:
    code = _normalize_zip(zip_code)
    if not code:
        return None
    result = _lookup_remote(code)
    if result:
        return dict(result)
    return None
