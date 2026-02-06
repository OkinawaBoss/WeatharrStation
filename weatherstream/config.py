from __future__ import annotations
import argparse
from dataclasses import dataclass, field
from pathlib import Path

@dataclass
class Config:
    # Location
    zip: str | None
    lat: float | None
    lon: float | None
    location_name: str

    # Output surface
    width: int
    height: int

    # Output stream pacing (encoder CFR only)
    output_fps: int
    video_kbps: int

    # Destination (udp/srt/file/http-ts, etc.)
    out_url: str

    # Data refresh
    data_interval_sec: int

    # Ticker defaults
    ticker_speed_px_per_sec: int

    # Misc
    user_agent: str
    page_duration_sec: int
    music_dir: str
    music_fifo: str | None
    timezone: str | None

    # RSS / News
    rss_urls: list[str] = field(default_factory=list)
    rss_refresh_sec: int = 300
    rss_max_items: int = 10


def parse_args(argv: list[str] | None = None) -> Config:
    p = argparse.ArgumentParser("weatherstream")

    loc = p.add_argument_group("Location")
    loc.add_argument("--zip", type=str, default=None, help="ZIP code (optional if --lat/--lon provided)")
    loc.add_argument("--lat", type=float, default=None, help="Latitude")
    loc.add_argument("--lon", type=float, default=None, help="Longitude")
    loc.add_argument("--location-name", type=str, default="Weatharr Station")

    out = p.add_argument_group("Output")
    out.add_argument("--w", "--width", dest="width", type=int, default=1920)
    out.add_argument("--h", "--height", dest="height", type=int, default=1080)
    out.add_argument("--output-fps", type=int, default=30, help="Encoder CFR; compositor is event-driven")
    out.add_argument("--video-kbps", type=int, default=3500, help="Video bitrate in kbps")
    out.add_argument("--out", dest="out_url", type=str, default="udp://127.0.0.1:5000?pkt_size=1316")

    data = p.add_argument_group("Data & UI")
    data.add_argument("--data-interval-sec", type=int, default=60, help="Background NOAA refresh interval")
    data.add_argument("--ticker-speed", dest="ticker_speed_px_per_sec", type=int, default=120)
    data.add_argument("--page-seconds", dest="page_duration_sec", type=int, default=12, help="Seconds to display each page")
    data.add_argument("--music-dir", type=str, default=None, help="Folder containing background music tracks")
    data.add_argument("--music-fifo", type=str, default=None, help="(Legacy) FIFO path for music PCM output")
    data.add_argument("--user-agent", type=str, default="WeatherStream/0.2 (+contact)")
    data.add_argument("--tz", "--timezone", dest="timezone", type=str, default=None,
                     help="IANA timezone name (e.g. 'America/Chicago'); defaults to system local time")

    rss = p.add_argument_group("News / RSS")
    rss.add_argument(
        "--rss-url",
        dest="rss_urls",
        action="append",
        default=[],
        help="RSS/Atom feed URL (use multiple --rss-url flags to add more than one)"
    )
    rss.add_argument(
        "--rss-refresh-sec",
        type=int,
        default=300,
        help="How often to refresh RSS feeds (seconds)"
    )
    rss.add_argument(
        "--rss-max-items",
        type=int,
        default=10,
        help="Max titles to keep per feed"
    )

    args = p.parse_args(argv)

    here = Path(__file__).resolve()
    default_music_dir = None
    for parent in (here.parent, *here.parents[1:4]):
        candidate = parent / "assets" / "music"
        if candidate.exists():
            default_music_dir = candidate
            break
    if not default_music_dir:
        default_music_dir = here.parent / "assets" / "music"
    music_dir = args.music_dir or str(default_music_dir)

    return Config(
        zip=args.zip,
        lat=args.lat,
        lon=args.lon,
        location_name=args.location_name,
        width=args.width,
        height=args.height,
        output_fps=args.output_fps,
        video_kbps=args.video_kbps,
        out_url=args.out_url,
        data_interval_sec=args.data_interval_sec,
        ticker_speed_px_per_sec=args.ticker_speed_px_per_sec,
        user_agent=args.user_agent,
        page_duration_sec=args.page_duration_sec,
        music_dir=music_dir,
        music_fifo=args.music_fifo,
        timezone=args.timezone,
        rss_urls=args.rss_urls,
        rss_refresh_sec=args.rss_refresh_sec,
        rss_max_items=args.rss_max_items,
    )
