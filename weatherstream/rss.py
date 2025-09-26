from __future__ import annotations
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import xml.etree.ElementTree as ET
import time
from typing import List, Tuple

USER_AGENT = "WeatherStreamRSS/1.0"

def _get(url: str, timeout: int = 10) -> bytes | None:
    try:
        req = Request(url, headers={"User-Agent": USER_AGENT})
        with urlopen(req, timeout=timeout) as r:
            return r.read()
    except (URLError, HTTPError, TimeoutError):
        return None

def parse_rss_titles(xml_bytes: bytes, max_items: int = 10) -> List[Tuple[str, str | None]]:
    """
    Returns a list of (title, link) tuples.
    Supports common RSS/Atom shapes with best-effort parsing.
    """
    out: List[Tuple[str, str | None]] = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return out

    # Try RSS 2.0: <channel><item><title>, <link>
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip() or None
        if title:
            out.append((title, link))
            if len(out) >= max_items:
                return out

    # Try Atom: <entry><title>, <link href="...">
    if not out:
        for entry in root.findall(".//{*}entry"):
            title = (entry.findtext("{*}title") or "").strip()
            link_el = entry.find("{*}link")
            link = link_el.get("href") if link_el is not None else None
            if title:
                out.append((title, link))
                if len(out) >= max_items:
                    return out

    return out

class RssTitleCache:
    def __init__(self, urls: list[str], refresh_sec: int = 300, max_items: int = 10):
        self.urls = urls
        self.refresh_sec = max(30, int(refresh_sec))
        self.max_items = max(1, int(max_items))
        self._last_fetch = 0.0
        self._titles: list[tuple[str, str | None]] = []

    def _refresh_if_needed(self) -> None:
        now = time.time()
        if now - self._last_fetch < self.refresh_sec:
            return
        titles: list[tuple[str, str | None]] = []
        for url in self.urls:
            data = _get(url)
            if not data:
                continue
            titles.extend(parse_rss_titles(data, self.max_items))
        # Simple de-dupe on title text
        seen = set()
        unique: list[tuple[str, str | None]] = []
        for t, l in titles:
            key = t.lower()
            if key in seen:
                continue
            seen.add(key)
            unique.append((t, l))
        self._titles = unique[: self.max_items * max(1, len(self.urls))]
        self._last_fetch = now

    def as_ticker_text(self, separator: str = "    •    ") -> str:
        self._refresh_if_needed()
        if not self._titles:
            return ""
        parts = []
        for title, _ in self._titles:
            parts.append(title)
        # Repeat once so the strip loops smoothly
        s = separator.join(parts)
        return f"{s}{separator}{s}"
