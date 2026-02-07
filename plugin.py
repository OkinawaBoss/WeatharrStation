import os
import signal
import socket
import subprocess
import sys
import time
import threading
import uuid
from copy import deepcopy
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import shutil

from django.db import transaction
from django.utils import timezone

from apps.plugins.models import PluginConfig
from apps.channels.models import Channel, ChannelGroup, ChannelStream, Stream
from core.models import StreamProfile, CoreSettings

try:
    from weatherstream.data.zipcodes import resolve_zip
except Exception:  # pragma: no cover - fallback when weatherstream assets missing
    resolve_zip = None

try:  # pragma: no cover - Unix-only
    import fcntl
except Exception:  # pragma: no cover - Windows
    fcntl = None

_START_LOCK = threading.Lock()
_STATION_COUNT = 3
_RUNTIME_FIELDS = (
    "pid",
    "running",
    "last_started_at",
    "last_stopped_at",
    "stream_id",
    "channel_id",
    "channel_number",
    "location_label",
    "output_url",
    "encoding",
    "run_token",
    "pid_started_at",
)


_DEFAULT_FIELDS = [
    {
        "id": "fps",
        "label": "Default Frames Per Second",
        "type": "number",
        "default": 24,
        "help_text": "Default output frame rate (1–60).",
    },
    {
        "id": "resolution",
        "label": "Default Resolution",
        "type": "select",
        "default": "1920x1080",
        "help_text": "Default output resolution for WeatherStream feeds.",
        "options": [
            {"value": "3840x2160", "label": "4K (3840x2160)"},
            {"value": "1920x1080", "label": "1080p (1920x1080)"},
            {"value": "1280x720", "label": "720p (1280x720)"},
            {"value": "960x540", "label": "540p (960x540)"},
            {"value": "854x480", "label": "480p (854x480)"},
            {"value": "640x360", "label": "360p (640x360)"},
        ],
    },
    {
        "id": "video_kbps",
        "label": "Default Video Bitrate (kbps)",
        "type": "number",
        "default": 3500,
        "min": 500,
        "max": 20000,
        "step": 100,
        "help_text": "Default target video bitrate in kbps.",
    },
    # --- RSS settings (string input; supports multiple URLs separated by comma/semicolon/newlines)
    {
        "id": "rss_urls",
        "label": "Default RSS/Atom Feeds",
        "type": "string",
        "default": "",
        "help_text": "Default feed URLs, separated by commas (you can also paste with newlines/semicolons).",
    },
    {
        "id": "rss_refresh_sec",
        "label": "Default RSS Refresh (seconds)",
        "type": "number",
        "default": 300,
        "help_text": "Default RSS refresh interval. Default 300.",
    },
    {
        "id": "rss_max_items",
        "label": "Default Max Items Per Feed",
        "type": "number",
        "default": 3,
        "help_text": "Default titles per feed. Default 3.",
    },
]

_BASE_FIELDS = [
    {
        "id": "zip_code",
        "label": "ZIP Code",
        "type": "string",
        "default": "",
        "help_text": "5-digit ZIP used to localize the weather feed",
    },
    {
        "id": "timezone",
        "label": "Time Zone",
        "type": "string",
        "default": "",
        "help_text": "Optional IANA time zone (e.g., 'America/Chicago'). Leave blank to auto-detect from ZIP (WeatherStream will attempt this).",
    },
    {
        "id": "location_name",
        "label": "Location Name",
        "type": "string",
        "default": "",
        "help_text": "Optional display name (e.g., 'Salt Lake City, UT'). If blank, we’ll resolve from ZIP.",
    },
    {
        "id": "channel_number",
        "label": "Channel Number",
        "type": "number",
        "default": "",
        "help_text": "Optional channel number in the Weather group. If already taken there, the plugin will refuse to start.",
    },
]


def _build_station_fields(station_count: int) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    fields.append(
        {
            "id": "defaults_info",
            "label": "Defaults",
            "type": "info",
            "description": "These defaults apply to all stations.",
        }
    )
    fields.extend(deepcopy(_DEFAULT_FIELDS))
    for idx in range(1, station_count + 1):
        fields.append(
            {
                "id": f"station_{idx}_info",
                "label": f"Station {idx}",
                "type": "info",
                "description": "Configure a WeatherStream station and channel.",
            }
        )
        fields.append(
            {
                "id": f"station_{idx}_enabled",
                "label": f"Enable Station {idx}",
                "type": "boolean",
                "default": True if idx == 1 else False,
                "help_text": "Enable this station when starting the plugin.",
            }
        )
        for base in _BASE_FIELDS:
            field = deepcopy(base)
            if idx == 1:
                fields.append(field)
                continue
            field["id"] = f"station_{idx}_{base['id']}"
            fields.append(field)
    return fields


class Plugin:
    name = "Weatharr Station"
    version = "2.0"
    description = "Start a local WeatherStream broadcast and publish it as a channel."
    author = "OkinawaBoss"
    help_url = "https://github.com/OkinawaBoss/WeatharrStation"
    fields = _build_station_fields(_STATION_COUNT)

    _STOP_CONFIRM_TEMPLATE = {
        "required": True,
        "title": "Stop Weatharr Station?",
        "message": "This will terminate the running WeatherStream instance.",
    }
    _RESET_CONFIRM_TEMPLATE = {
        "required": True,
        "title": "Reset Weatharr Station settings?",
        "message": "This will stop the WeatherStream backend and restore default configuration values.",
    }

    actions = [
        {
            "id": "start",
            "label": "Start",
            "description": "Launch WeatherStream with the saved ZIP code",
            "button_label": "Start",
            "button_color": "green",
        },
        {
            "id": "stop",
            "label": "Stop",
            "description": "Terminate the WeatherStream process",
            "confirm": _STOP_CONFIRM_TEMPLATE,
            "button_label": "Stop",
            "button_color": "yellow",
        },
        {
            "id": "reset_defaults",
            "label": "Reset to Defaults",
            "description": "Restore Weatharr Station settings to their default values and stop any running backend.",
            "confirm": _RESET_CONFIRM_TEMPLATE,
            "button_label": "Reset",
            "button_color": "red",
        },
    ]

    def __init__(self) -> None:
        self._base_dir = Path(__file__).resolve().parent
        self._plugin_key = self._base_dir.name.replace(" ", "_").lower()
        self._log_path = self._base_dir / "weatharrstation.log"
        self._log_max_bytes = 5 * 1024 * 1024
        self._start_lock_path = self._base_dir / ".start.lock"
        self._station_count = _STATION_COUNT
        self._base_http_port = 5950
        self._http_port = self._base_http_port
        self._stream_url = self._station_stream_url(1)
        self._channel_group_name = "Weather"
        self._channel_title = "Weatharr Station"
        self._stream_title = "Weatharr Station Feed"

        # cache for stream profile id lookup
        self._stream_profile_id: Optional[int] = None

        # defaults (fps only; no UI field)
        self._output_defaults = {"fps": 24, "width": 1920, "height": 1080, "video_kbps": 3500}

        self._field_defaults = {field["id"]: field.get("default") for field in self.fields}

    # --- public entry point -------------------------------------------------
    def run(self, action: str, params: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        action = (action or "").lower()
        context = self._context_with_params(context, params)

        if action in {"", "status"}:
            response = self._handle_status(context)
        elif action == "reset_defaults":
            response = self._handle_reset_defaults(context)
        elif action == "start":
            response = self._handle_start(context)
        elif action == "stop":
            response = self._handle_stop(context)
        else:
            response = {"status": "error", "message": f"Unknown action '{action}'"}

        return self._finalize_response(response, context)

    def stop(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not context or "settings" not in context:
            try:
                cfg = PluginConfig.objects.get(key=self._plugin_key)
                settings = dict(cfg.settings or {})
            except PluginConfig.DoesNotExist:
                settings = {}
            context = {"settings": settings, "logger": None}
        return self._handle_stop(context)

    # --- action handlers ----------------------------------------------------
    def _handle_start(self, context: Dict[str, Any]) -> Dict[str, Any]:
        logger = context.get("logger")
        settings = dict(context.get("settings") or {})
        stations = [self._build_station_state(settings, idx) for idx in self._station_indices()]
        enabled_stations = [s for s in stations if s.get("enabled")]

        if not enabled_stations:
            return {
                "status": "error",
                "message": "No stations are enabled. Enable at least one station to start.",
                "settings": settings,
            }

        updates: Dict[str, Any] = {}
        clears: list[str] = []
        station_results: list[Dict[str, Any]] = []
        started = 0
        already_running = 0
        failed = 0

        with self._start_guard():
            for station in enabled_stations:
                result = self._start_station(station, settings, logger)
                station_results.append(
                    {
                        "station": station["id"],
                        "status": result.get("status"),
                        "message": result.get("message"),
                    }
                )
                updates.update(result.get("updates", {}))
                clears.extend(result.get("clears", []))
                if result.get("status") == "running":
                    if result.get("started"):
                        started += 1
                    else:
                        already_running += 1
                elif result.get("status") == "error":
                    failed += 1

        persisted = self._persist_settings(updates, clear=clears) if updates or clears else settings

        if started or already_running:
            status = "running"
            if failed:
                message = f"Started {started} station(s); {already_running} already running; {failed} failed."
            elif started:
                message = f"Started {started} station(s)."
            else:
                message = f"{already_running} station(s) already running."
        else:
            status = "error"
            message = "Failed to start any stations."

        return {
            "status": status,
            "message": message,
            "settings": persisted,
            "stations": station_results,
        }

    def _start_station(self, station: Dict[str, Any], settings: Dict[str, Any], logger: Any) -> Dict[str, Any]:
        idx = int(station["index"])
        runtime_keys = self._station_runtime_keys(idx)
        pid_key = runtime_keys["pid"]
        run_token_key = runtime_keys["run_token"]
        running_key = runtime_keys["running"]
        encoding_key = runtime_keys["encoding"]
        output_url_key = runtime_keys["output_url"]
        location_label_key = runtime_keys["location_label"]
        channel_id_key = runtime_keys["channel_id"]
        stream_id_key = runtime_keys["stream_id"]
        channel_number_key = runtime_keys["channel_number"]
        pid_started_key = runtime_keys["pid_started_at"]
        last_started_key = runtime_keys["last_started_at"]

        pid = settings.get(pid_key)
        run_token = settings.get(run_token_key)

        updates: Dict[str, Any] = {}
        clears: list[str] = []

        desired_encoding = self._resolve_output_settings_for_station(station)
        current_encoding = settings.get(encoding_key) or {}

        if pid and self._is_process_running(pid, run_token):
            if any(
                current_encoding.get(key) != desired_encoding.get(key)
                for key in ("fps", "width", "height", "video_kbps")
            ):
                if logger:
                    logger.info("Output settings changed; restarting WeatherStream for %s.", station["id"])
                self._terminate_process(pid, logger, expected_token=run_token)
                updates[running_key] = False
                clears.extend([pid_key, run_token_key])
                pid = None
            else:
                updates[output_url_key] = station["stream_url"]
                if not settings.get(running_key):
                    updates[running_key] = True
                return {
                    "status": "running",
                    "message": f"{station['id']} already running.",
                    "updates": updates,
                    "clears": clears,
                    "started": False,
                }

        if pid and not self._is_process_running(pid, run_token):
            clears.extend([pid_key, run_token_key])
            if settings.get(running_key):
                updates[running_key] = False
            pid = None

        zip_code = station.get("zip_code") or ""
        if not zip_code:
            return {
                "status": "error",
                "message": f"{station['id']} requires a ZIP code.",
                "updates": updates,
                "clears": clears,
            }
        if not zip_code.isdigit() or len(zip_code) != 5:
            return {
                "status": "error",
                "message": f"{station['id']} ZIP code must be a 5-digit number.",
                "updates": updates,
                "clears": clears,
            }

        if not self._is_port_available(station["port"]):
            return {
                "status": "error",
                "message": f"Port {station['port']} is already in use for {station['id']}.",
                "updates": updates,
                "clears": clears,
            }

        location_label = station.get("location_name") or self._resolve_location(zip_code)

        try:
            stream, channel = self._ensure_stream_and_channel(
                {
                    "stream_id": settings.get(stream_id_key),
                    "channel_id": settings.get(channel_id_key),
                    "channel_number": station.get("channel_number"),
                },
                location_label,
                station["stream_url"],
            )
        except Exception as exc:
            if logger:
                logger.exception("Failed to prepare WeatherStream channel resources")
            return {
                "status": "error",
                "message": f"Failed to prepare channel for {station['id']}: {exc}",
                "updates": updates,
                "clears": clears,
            }

        run_token = uuid.uuid4().hex
        try:
            pid = self._launch_process(
                zip_code,
                location_label,
                desired_encoding,
                logger,
                {
                    "timezone": station.get("timezone"),
                    "rss_urls": station.get("rss_urls"),
                    "rss_refresh_sec": station.get("rss_refresh_sec"),
                    "rss_max_items": station.get("rss_max_items"),
                },
                run_token,
                station["stream_url"],
                station["index"],
            )
        except Exception as exc:
            if logger:
                logger.exception("Failed to launch WeatherStream process")
            return {
                "status": "error",
                "message": f"Failed to start {station['id']}: {exc}",
                "updates": updates,
                "clears": clears,
            }

        now_iso = timezone.now().isoformat()
        updates.update(
            {
                pid_key: pid,
                run_token_key: run_token,
                pid_started_key: time.time(),
                running_key: True,
                last_started_key: now_iso,
                stream_id_key: stream.id,
                channel_id_key: channel.id,
                channel_number_key: channel.channel_number,
                location_label_key: location_label,
                output_url_key: station["stream_url"],
                encoding_key: desired_encoding,
            }
        )

        return {
            "status": "running",
            "message": f"{station['id']} started.",
            "updates": updates,
            "clears": clears,
            "started": True,
        }

    def _handle_stop(self, context: Dict[str, Any]) -> Dict[str, Any]:
        logger = context.get("logger")
        settings = dict(context.get("settings") or {})
        updates: Dict[str, Any] = {}
        clears: list[str] = []
        stopped = 0
        was_running = 0
        now_iso = timezone.now().isoformat()

        for idx in self._station_indices():
            runtime_keys = self._station_runtime_keys(idx)
            pid_key = runtime_keys["pid"]
            run_token_key = runtime_keys["run_token"]
            running_key = runtime_keys["running"]
            last_stopped_key = runtime_keys["last_stopped_at"]

            pid = settings.get(pid_key)
            run_token = settings.get(run_token_key)
            if pid:
                was_running += 1
                if self._terminate_process(pid, logger, expected_token=run_token):
                    stopped += 1
                clears.append(pid_key)
            if run_token:
                clears.append(run_token_key)
            if settings.get(running_key):
                updates[running_key] = False
            if pid or run_token:
                updates[last_stopped_key] = now_iso

        if not was_running:
            persisted = self._persist_settings(updates, clear=clears) if updates or clears else settings
            return {"status": "stopped", "message": "No stations are currently running.", "settings": persisted}

        persisted = self._persist_settings(updates, clear=clears)
        if stopped:
            return {
                "status": "stopped",
                "message": f"Stopped {stopped} station(s).",
                "settings": persisted,
            }
        return {
            "status": "stopped",
            "message": "No active WeatherStream processes found; state reset.",
            "settings": persisted,
        }

    def _handle_status(self, context: Dict[str, Any]) -> Dict[str, Any]:
        settings = dict(context.get("settings") or {})
        running, settings = self._refresh_running_state(settings)
        stations = [self._build_station_state(settings, idx) for idx in self._station_indices()]
        running_count = sum(1 for s in stations if s.get("runtime", {}).get("running"))
        if running_count:
            message = f"{running_count} station(s) running."
        else:
            message = "All stations are stopped."
        return {
            "status": "running" if running else "stopped",
            "message": message,
            "settings": settings,
            "stations": [
                {
                    "station": s["id"],
                    "running": bool(s.get("runtime", {}).get("running")),
                    "pid": s.get("runtime", {}).get("pid"),
                    "channel_id": s.get("runtime", {}).get("channel_id"),
                    "stream_id": s.get("runtime", {}).get("stream_id"),
                    "channel_number": s.get("runtime", {}).get("channel_number"),
                }
                for s in stations
            ],
        }

    def _handle_reset_defaults(self, context: Dict[str, Any]) -> Dict[str, Any]:
        logger = context.get("logger")
        settings = dict(context.get("settings") or {})
        running, settings = self._refresh_running_state(settings)
        if running:
            stop_context = dict(context)
            stop_context["settings"] = settings
            stop_result = self._handle_stop(stop_context)
            settings = dict(stop_result.get("settings") or {})

        default_values: Dict[str, Any] = {field["id"]: field.get("default") for field in self.fields}
        for idx in self._station_indices():
            default_values[self._station_runtime_key(idx, "running")] = False

        clear_keys = []
        for idx in self._station_indices():
            for name in _RUNTIME_FIELDS:
                key = self._station_runtime_key(idx, name)
                if key not in default_values:
                    clear_keys.append(key)

        persisted = self._persist_settings(default_values, clear=clear_keys)
        if logger:
            logger.info("Weatharr Station settings reset to defaults.")
        return {"status": "stopped", "message": "Weather stream settings restored to defaults.", "settings": persisted}

    def _finalize_response(self, response: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        response = dict(response or {})
        context_settings = dict(context.get("settings") or {})
        response_settings = response.get("settings")
        base_settings: Dict[str, Any] = dict(response_settings) if isinstance(response_settings, dict) else context_settings

        running, latest_settings = self._refresh_running_state(base_settings)
        response["actions"] = [
            {
                "id": a.get("id"),
                "label": a.get("label"),
                "description": a.get("description"),
                **({"confirm": a.get("confirm")} if a.get("confirm") else {}),
            }
            for a in self.actions
        ]
        if not isinstance(response_settings, dict) or response_settings is not latest_settings:
            response["settings"] = latest_settings
        if "status" not in response or response["status"] not in {"running", "stopped", "error"}:
            response["status"] = "running" if running else "stopped"
        return response

    def _refresh_running_state(self, settings: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
        current_settings = dict(settings or {})
        updates: Dict[str, Any] = {}
        clear_keys: list[str] = []
        any_running = False

        for idx in self._station_indices():
            runtime_keys = self._station_runtime_keys(idx)
            pid_key = runtime_keys["pid"]
            run_token_key = runtime_keys["run_token"]
            running_key = runtime_keys["running"]
            output_url_key = runtime_keys["output_url"]

            expected_url = self._station_stream_url(idx)
            if current_settings.get(output_url_key) != expected_url:
                updates[output_url_key] = expected_url

            pid = current_settings.get(pid_key)
            run_token = current_settings.get(run_token_key)
            is_running = self._is_process_running(pid, run_token)

            if is_running:
                any_running = True
                if not current_settings.get(running_key):
                    updates[running_key] = True
                continue

            if current_settings.get(running_key):
                updates[running_key] = False
            if pid:
                clear_keys.append(pid_key)
            if current_settings.get(run_token_key):
                clear_keys.append(run_token_key)

        if updates or clear_keys:
            current_settings = self._persist_settings(updates, clear=clear_keys)

        return any_running, current_settings

    # --- helpers ------------------------------------------------------------
    @contextmanager
    def _start_guard(self):
        with _START_LOCK:
            lock_file = None
            try:
                if fcntl:
                    lock_file = open(self._start_lock_path, "a+b")
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                yield
            finally:
                if lock_file and fcntl:
                    try:
                        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    except Exception:
                        pass
                    lock_file.close()

    def _station_indices(self) -> list[int]:
        return list(range(1, self._station_count + 1))

    def _station_field_id(self, idx: int, field: str) -> str:
        if field == "enabled":
            return f"station_{idx}_enabled"
        if idx == 1:
            return field
        return f"station_{idx}_{field}"

    def _station_runtime_key(self, idx: int, field: str) -> str:
        if idx == 1:
            return field
        return f"station_{idx}_{field}"

    def _station_runtime_keys(self, idx: int) -> dict[str, str]:
        return {name: self._station_runtime_key(idx, name) for name in _RUNTIME_FIELDS}

    def _station_port(self, idx: int) -> int:
        return int(self._base_http_port + (idx - 1))

    def _station_stream_url(self, idx: int) -> str:
        suffix = "" if idx == 1 else f"_{idx}"
        return f"http://127.0.0.1:{self._station_port(idx)}/weatharr{suffix}.ts"

    def _station_field_value(self, settings: Dict[str, Any], idx: int, field: str) -> Any:
        field_id = self._station_field_id(idx, field)
        if field_id in settings:
            return settings.get(field_id)
        return self._field_defaults.get(field_id)

    def _default_setting_value(self, settings: Dict[str, Any], field: str) -> Any:
        if field in settings:
            return settings.get(field)
        return self._field_defaults.get(field)

    def _build_station_state(self, settings: Dict[str, Any], idx: int) -> Dict[str, Any]:
        runtime_keys = self._station_runtime_keys(idx)
        enabled = bool(self._station_field_value(settings, idx, "enabled"))
        zip_code = (self._station_field_value(settings, idx, "zip_code") or "").strip()
        timezone_val = (self._station_field_value(settings, idx, "timezone") or "").strip()
        location_name = (self._station_field_value(settings, idx, "location_name") or "").strip()
        channel_number = self._station_field_value(settings, idx, "channel_number")
        fps = self._default_setting_value(settings, "fps")
        resolution = self._default_setting_value(settings, "resolution")
        video_kbps = self._default_setting_value(settings, "video_kbps")
        rss_urls = self._default_setting_value(settings, "rss_urls")
        rss_refresh_sec = self._default_setting_value(settings, "rss_refresh_sec")
        rss_max_items = self._default_setting_value(settings, "rss_max_items")

        runtime = {name: settings.get(key) for name, key in runtime_keys.items()}

        return {
            "index": idx,
            "id": f"station_{idx}",
            "enabled": enabled,
            "zip_code": zip_code,
            "timezone": timezone_val,
            "location_name": location_name,
            "channel_number": channel_number,
            "fps": fps,
            "resolution": resolution,
            "video_kbps": video_kbps,
            "rss_urls": rss_urls,
            "rss_refresh_sec": rss_refresh_sec,
            "rss_max_items": rss_max_items,
            "runtime": runtime,
            "stream_url": self._station_stream_url(idx),
            "port": self._station_port(idx),
        }

    def _resolve_output_settings_for_station(self, station: Dict[str, Any]) -> Dict[str, Any]:
        return self._resolve_output_settings(
            {
                "fps": station.get("fps"),
                "resolution": station.get("resolution"),
                "video_kbps": station.get("video_kbps"),
            }
        )

    def _is_port_available(self, port: int, host: str = "127.0.0.1") -> bool:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind((host, int(port)))
            return True
        except OSError:
            return False
        finally:
            try:
                sock.close()
            except Exception:
                pass

    def _sanitize_rss_urls(self, raw: str) -> list[str]:
        if not raw:
            return []
        normalized = raw.replace("\r", "\n").replace(";", ",").replace("\n", ",")
        urls = []
        for item in normalized.split(","):
            candidate = item.strip()
            if not candidate:
                continue
            if len(candidate) > 500:
                continue
            parsed = urlparse(candidate)
            if parsed.scheme not in {"http", "https"}:
                continue
            urls.append(candidate)
            if len(urls) >= 10:
                break
        return urls

    def _rotate_log_if_needed(self) -> None:
        try:
            if self._log_path.exists() and self._log_path.stat().st_size > self._log_max_bytes:
                backup = self._log_path.with_suffix(self._log_path.suffix + ".1")
                if backup.exists():
                    backup.unlink()
                self._log_path.rename(backup)
        except Exception:
            pass

    def _pid_matches_token(self, pid: int, expected_token: str) -> bool:
        token = self._read_proc_env_token(pid)
        if token:
            return token == expected_token
        cmdline = self._read_proc_cmdline(pid)
        if cmdline:
            return "weatherstream.main" in cmdline and "--out" in cmdline
        return False

    def _read_proc_env_token(self, pid: int) -> Optional[str]:
        try:
            with open(f"/proc/{pid}/environ", "rb") as fh:
                raw = fh.read().split(b"\0")
        except Exception:
            return None
        for entry in raw:
            if entry.startswith(b"WEATHARR_RUN_TOKEN="):
                return entry.split(b"=", 1)[1].decode("utf-8", "ignore")
        return None

    def _read_proc_cmdline(self, pid: int) -> Optional[str]:
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as fh:
                raw = fh.read().replace(b"\0", b" ").decode("utf-8", "ignore")
        except Exception:
            return None
        return raw.strip() or None

    def _context_with_params(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        base_context = dict(context or {})
        stored_settings = dict(base_context.get("settings") or {})

        field_updates: Dict[str, Any] = {}
        if params:
            for field in self.fields:
                fid = field["id"]
                if fid in params:
                    value = params[fid]
                    stored_settings[fid] = value
                    field_updates[fid] = value

        if field_updates:
            persisted = self._persist_settings(field_updates)
            stored_settings = dict(persisted)

        base_context["settings"] = stored_settings
        return base_context

    def _resolve_location(self, zip_code: str) -> Optional[str]:
        if resolve_zip is None:
            return None
        try:
            data = resolve_zip(zip_code)
        except Exception:
            return None
        if not data:
            return None
        city = (data.get("city") or "").strip()
        state = (data.get("state") or "").strip()
        if city and state:
            return f"{city}, {state}"
        return city or state or None

    def _ensure_stream_and_channel(
        self,
        settings: Dict[str, Any],
        location_label: Optional[str],
        stream_url: str,
    ) -> tuple[Stream, Channel]:
        # Names for creation only (we will not change existing)
        stream_name = self._stream_title if not location_label else f"{self._stream_title} ({location_label})"
        channel_name = self._channel_title if not location_label else f"{self._channel_title} - {location_label}"

        stream = self._get_or_create_stream(stream_name, settings.get("stream_id"), stream_url)
        channel_number = self._resolve_channel_number(settings)
        channel = self._get_or_create_channel(channel_name, stream, settings.get("channel_id"), channel_number)

        # Ensure ChannelStream mapping exists (safe to create if missing)
        ChannelStream.objects.get_or_create(channel=channel, stream=stream, defaults={"order": 0})
        return stream, channel

    def _resolve_channel_number(self, settings: Dict[str, Any]) -> Optional[int]:
        raw_number = settings.get("channel_number")
        if raw_number in (None, ""):
            return None
        try:
            number = int(raw_number)
        except (TypeError, ValueError):
            return None
        if number <= 0:
            return None
        return number

    def _resolve_output_settings(self, settings: Dict[str, Any]) -> Dict[str, Any]:
        fps = self._output_defaults["fps"]
        try:
            # allow override if someone set it in DB manually; otherwise default
            val = settings.get("fps")
            if val not in (None, ""):
                fps = max(1, min(60, int(val)))
        except (TypeError, ValueError):
            fps = self._output_defaults["fps"]

        width = self._output_defaults["width"]
        height = self._output_defaults["height"]
        raw_res = (settings.get("resolution") or "").strip().lower()
        if raw_res:
            try:
                parts = raw_res.split("x", 1)
                if len(parts) == 2:
                    w = int(parts[0])
                    h = int(parts[1])
                    if w > 0 and h > 0:
                        width = w
                        height = h
            except (TypeError, ValueError):
                pass
        video_kbps = self._output_defaults["video_kbps"]
        try:
            val = settings.get("video_kbps")
            if val not in (None, ""):
                video_kbps = max(500, min(20000, int(val)))
        except (TypeError, ValueError):
            video_kbps = self._output_defaults["video_kbps"]
        return {"fps": fps, "width": width, "height": height, "video_kbps": video_kbps}

    # --- stream profile lookup (ffmpeg) ------------------------------------
    def _get_stream_profile_id(self) -> int:
        if self._stream_profile_id is not None:
            return self._stream_profile_id

        # Find a profile named "proxy" (case-insensitive). Fall back to the first profile if needed.
        profile = (
            StreamProfile.objects.filter(name__iexact="proxy").first()
            or StreamProfile.objects.filter(name__icontains="proxy").first()
        )
        if not profile:
            profile = StreamProfile.objects.first()
        if not profile:
            raise RuntimeError("No stream profiles found. Create a stream profile (recommended name: 'proxy').")
        self._stream_profile_id = profile.id
        return self._stream_profile_id

    # --- create-only semantics ---------------------------------------------
    def _get_or_create_stream(self, name: str, stream_id: Optional[int], stream_url: str) -> Stream:
        if stream_id:
            try:
                return Stream.objects.get(id=stream_id)
            except Stream.DoesNotExist:
                pass

        # Try to find by exact name AND expected URL without modifying anything
        existing = Stream.objects.filter(name=name, url=stream_url).first()
        if existing:
            return existing

        # Create new with ffmpeg profile and our URL
        stream = Stream.objects.create(
            name=name,
            url=stream_url,
            logo_url=None,
            tvg_id=None,
            stream_profile_id=self._get_stream_profile_id(),
        )
        return stream

    def _get_or_create_channel(
        self,
        name: str,
        stream: Stream,
        channel_id: Optional[int],
        preferred_channel_number: Optional[int],
    ) -> Channel:
        # If ID provided and exists, return as-is (no mutation)
        if channel_id:
            try:
                return Channel.objects.get(id=channel_id)
            except Channel.DoesNotExist:
                pass

        # If a channel number is provided and exists in the Weather group, reuse only if it looks like ours.
        if preferred_channel_number:
            match = Channel.objects.filter(
                channel_number=preferred_channel_number,
                channel_group__name=self._channel_group_name,
            ).first()
            if match:
                if match.name.startswith(self._channel_title):
                    return match
                raise RuntimeError(
                    f"Channel number {preferred_channel_number} already exists in the Weather group."
                )

        # Otherwise create a new one in the Weather group with ffmpeg profile
        group, _ = ChannelGroup.objects.get_or_create(name=self._channel_group_name)
        channel_number = preferred_channel_number or Channel.get_next_available_channel_number(starting_from=1000)
        channel = Channel.objects.create(
            name=name,
            channel_number=channel_number,
            channel_group=group,
            stream_profile_id=self._get_stream_profile_id(),
        )
        return channel

    # --- process management -------------------------------------------------
    def _python_interpreter(self) -> str:
        """Resolve a real Python interpreter even when running under uWSGI."""
        exe = Path(sys.executable or "")
        candidates = []

        if exe.name and exe.name.lower().startswith("uwsgi"):
            candidates.extend([exe.with_name("python"), exe.with_name("python3")])

        venv = os.environ.get("VIRTUAL_ENV")
        if venv:
            venv_path = Path(venv)
            candidates.extend([venv_path / "bin" / "python", venv_path / "bin" / "python3"])

        candidates.append(exe)
        candidates.append(Path("python"))
        candidates.append(Path("python3"))

        for candidate in candidates:
            if not candidate:
                continue
            if isinstance(candidate, Path) and candidate.is_absolute() and candidate.exists():
                return str(candidate)
            if not isinstance(candidate, Path) or not candidate.is_absolute():
                resolved = shutil.which(str(candidate))
                if resolved:
                    return resolved

        raise RuntimeError("Unable to locate a Python interpreter for WeatherStream")

    def _launch_process(
        self,
        zip_code: str,
        location_label: Optional[str],
        encoding: Dict[str, Any],
        logger: Any,
        settings: Dict[str, Any],
        run_token: str,
        stream_url: str,
        station_index: int,
    ) -> int:
        python_exec = self._python_interpreter()
        cmd = [
            python_exec,
            "-m",
            "weatherstream.main",
            "--zip",
            zip_code,
            "--output-fps",
            str(encoding["fps"]),
            "--w",
            str(encoding.get("width", self._output_defaults["width"])),
            "--h",
            str(encoding.get("height", self._output_defaults["height"])),
            "--video-kbps",
            str(encoding.get("video_kbps", self._output_defaults["video_kbps"])),
            "--out",
            stream_url,
        ]

        if location_label:
            cmd += ["--location-name", location_label]

        # Timezone (optional)
        tz = (settings.get("timezone") or "").strip()
        if not tz:
            try:
                tz = CoreSettings.get_system_time_zone()
            except Exception:
                tz = ""
        if tz:
            cmd += ["--timezone", tz]

        # --- RSS flags (optional) ---
        raw_urls = (settings.get("rss_urls") or "").strip()
        urls = self._sanitize_rss_urls(raw_urls)
        for url in urls:
            cmd += ["--rss-url", url]

        try:
            rss_refresh = int(settings.get("rss_refresh_sec") or 300)
        except (TypeError, ValueError):
            rss_refresh = 300
        rss_refresh = max(60, min(3600, rss_refresh))
        if rss_refresh > 0:
            cmd += ["--rss-refresh-sec", str(rss_refresh)]

        try:
            rss_max = int(settings.get("rss_max_items") or 3)
        except (TypeError, ValueError):
            rss_max = 3
        rss_max = max(1, min(50, rss_max))
        if rss_max > 0:
            cmd += ["--rss-max-items", str(rss_max)]

        env = os.environ.copy()
        extra_path = str(self._base_dir)
        existing_path = env.get("PYTHONPATH")
        if existing_path:
            if extra_path not in existing_path.split(os.pathsep):
                env["PYTHONPATH"] = os.pathsep.join([extra_path, existing_path])
        else:
            env["PYTHONPATH"] = extra_path
        env["WEATHARR_PLUGIN_KEY"] = self._plugin_key
        env["WEATHARR_STATION_ID"] = str(station_index)
        env["WEATHARR_RUN_TOKEN"] = run_token
        env.setdefault("DJANGO_SETTINGS_MODULE", "dispatcharr.settings")

        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._rotate_log_if_needed()
        log_entry = f"\n--- [{datetime.now().isoformat()}] Starting WeatherStream for ZIP {zip_code} ---\n"
        with open(self._log_path, "ab") as log_file:
            log_file.write(log_entry.encode("utf-8"))
        log_handle = open(self._log_path, "ab", buffering=0)

        popen_kwargs: Dict[str, Any] = {
            "cwd": str(self._base_dir),
            "stdout": log_handle,
            "stderr": subprocess.STDOUT,
            "env": env,
        }

        if os.name != "nt":
            popen_kwargs["preexec_fn"] = os.setsid
        else:  # pragma: no cover - Windows
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]

        try:
            proc = subprocess.Popen(cmd, **popen_kwargs)
        except Exception:
            log_handle.close()
            raise

        log_handle.close()
        if logger:
            logger.info("WeatherStream started with PID %s", proc.pid)
        return proc.pid

    def _terminate_process(self, pid: int, logger: Any, expected_token: Optional[str] = None) -> bool:
        if not self._is_process_running(pid, expected_token):
            return False

        kill = os.kill
        if os.name != "nt" and hasattr(os, "killpg"):
            def kill_group(target_pid: int, sig: int) -> None:
                os.killpg(target_pid, sig)
            kill = kill_group  # type: ignore[assignment]

        try:
            kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return False
        except Exception:
            if logger:
                logger.exception("Failed to terminate WeatherStream PID %s", pid)
            raise

        deadline = time.time() + 10
        while time.time() < deadline:
            if not self._is_process_running(pid):
                break
            time.sleep(0.5)
        else:
            try:
                kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

        self._reap_process(pid)

        if logger:
            logger.info("WeatherStream PID %s terminated", pid)
        return True

    def _is_process_running(self, pid: Optional[int], expected_token: Optional[str] = None) -> bool:
        if not pid:
            return False
        try:
            pid_int = int(pid)
        except (TypeError, ValueError):
            return False

        if expected_token and not self._pid_matches_token(pid_int, expected_token):
            return False

        # If the child has already exited, reap it to avoid zombies and report not running
        try:
            finished_pid, _ = os.waitpid(pid_int, os.WNOHANG)
            if finished_pid == pid_int:
                return False
        except ChildProcessError:
            pass
        except OSError:
            return False

        try:
            os.kill(pid_int, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False
        return True

    def _reap_process(self, pid: int) -> None:
        try:
            while True:
                finished_pid, _ = os.waitpid(pid, os.WNOHANG)
                if finished_pid == 0:
                    break
                if finished_pid == pid:
                    break
        except ChildProcessError:
            pass
        except OSError:
            pass

    # --- pruning helpers ----------------------------------------------------
    def _allowed_setting_keys(self) -> set[str]:
        field_ids = {f["id"] for f in self.fields}
        runtime: set[str] = set()
        for idx in self._station_indices():
            for name in _RUNTIME_FIELDS:
                runtime.add(self._station_runtime_key(idx, name))
        return field_ids | runtime

    def _prune_unknown_keys(self, stored: Dict[str, Any]) -> Dict[str, Any]:
        allowed = self._allowed_setting_keys()
        return {k: v for k, v in (stored or {}).items() if k in allowed}

    def _persist_settings(self, updates: Dict[str, Any], clear: Optional[list[str]] = None) -> Dict[str, Any]:
        clear = clear or []
        with transaction.atomic():
            cfg = PluginConfig.objects.select_for_update().get(key=self._plugin_key)
            stored = dict(cfg.settings or {})
            stored.update(updates)
            for key in clear:
                stored.pop(key, None)
            stored = self._prune_unknown_keys(stored)
            cfg.settings = stored
            cfg.save(update_fields=["settings", "updated_at"])
            return stored
