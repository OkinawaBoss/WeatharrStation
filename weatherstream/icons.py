from __future__ import annotations
from pathlib import Path
import re

# Map text → canonical icon key
def pick_icon(short_forecast: str | None, is_daytime: bool | None) -> str:
    s = (short_forecast or "").lower()
    def daynight(day_key: str, night_key: str) -> str:
        return day_key if is_daytime else night_key

    # Order matters (most specific first)
    if re.search(r"thunder|t\-?storm|lightning", s):
        return "thunderstorm"
    if re.search(r"snow|blizzard|flurries|sleet|wintry", s):
        return "snow"
    if re.search(r"rain|showers|drizzle", s):
        return "rain"
    if re.search(r"fog|mist|haze|smoke", s):
        return "fog"
    if re.search(r"windy|breezy|gust", s):
        return "wind"
    if re.search(r"partly|mostly", s) and re.search(r"cloud", s):
        return daynight("partly-cloudy-day", "partly-cloudy-night")
    if "cloud" in s:
        return "cloudy"
    if "clear" in s or "sunny" in s:
        return daynight("clear-day", "clear-night")
    # Fallback
    return daynight("clear-day", "clear-night") if is_daytime is not None else "clear-day"


def find_icon_path(name: str) -> Path | None:
    """
    Looks upward from this file for assets/icons/<name>.png
    """
    here = Path(__file__).resolve()
    for up in range(1, 7):
        p = here.parents[up-1] / "assets" / "icons" / f"{name}.png"
        if p.exists():
            return p
    return None
