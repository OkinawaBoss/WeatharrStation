from __future__ import annotations
from datetime import datetime
from typing import List, Tuple

def _next_hour_rain(hourly_json) -> Tuple[str | None, int | None]:
    periods = (hourly_json or {}).get("properties", {}).get("periods", [])[:6]
    for p in periods:
        pop = p.get("probabilityOfPrecipitation", {}).get("value") or 0
        if pop >= 30:
            t = p.get("startTime", "")
            try:
                hhmm = t[11:16]
            except Exception:
                hhmm = "the next hour"
            return hhmm, int(pop)
    return None, None

def _next_day_chance(forecast_json) -> str | None:
    periods = (forecast_json or {}).get("properties", {}).get("periods", [])
    for p in periods:
        if not p.get("isDaytime"):
            continue
        sf = (p.get("shortForecast") or "").lower()
        if "chance" in sf:
            return f"It looks like {sf} on {p.get('name', 'the next day')}."
    return None

def build_narration(forecast_json, hourly_json, location_name: str) -> List[str]:
    lines: List[str] = []

    # Current quick opener
    try:
        p0 = forecast_json["properties"]["periods"][0]
        t = p0["temperature"]
        u = p0["temperatureUnit"]
        short = p0.get("shortForecast", "").lower()
        lines.append(f"In {location_name}, it's {t} degrees {u} and {short}.")
    except Exception:
        pass

    # Rain in the next few hours?
    hhmm, pop = _next_hour_rain(hourly_json)
    if hhmm and pop is not None:
        lines.append(f"We have about a {pop} percent chance of showers around {hhmm}.")

    # Next day “chance” summary
    nd = _next_day_chance(forecast_json)
    if nd:
        lines.append(nd)

    return lines[:2]  # keep it snappy
