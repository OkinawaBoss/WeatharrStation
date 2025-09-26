import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import shutil

from django.db import transaction
from django.utils import timezone

from apps.plugins.models import PluginConfig
from apps.channels.models import Channel, ChannelGroup, ChannelStream, Stream
from core.models import StreamProfile

try:
    from weatherstream.data.zipcodes import resolve_zip
except Exception:  # pragma: no cover - fallback when weatherstream assets missing
    resolve_zip = None


class Plugin:
    name = "Weatharr Station"
    version = "1.0"
    description = "Start a local WeatherStream broadcast and publish it as a channel."

    fields = [
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
            "help_text": "Optional channel number to reuse an existing channel.",
        },
        {
            "id": "fps",
            "label": "Frames Per Second",
            "type": "number",
            "default": 24,
            "help_text": "Output frame rate (1–60). Default 24.",
        },
        # --- RSS settings (string input; supports multiple URLs separated by comma/semicolon/newlines)
        {
            "id": "rss_urls",
            "label": "RSS/Atom Feeds",
            "type": "string",
            "default": "",
            "help_text": "Enter one or more feed URLs, separated by commas (you can also paste with newlines/semicolons).",
        },
        {
            "id": "rss_refresh_sec",
            "label": "RSS Refresh (seconds)",
            "type": "number",
            "default": 300,
            "help_text": "How often to refresh RSS feeds. Default 300.",
        },
        {
            "id": "rss_max_items",
            "label": "Max Items Per Feed",
            "type": "number",
            "default": 3,
            "help_text": "Limit titles per feed. Default 3.",
        },
    ]

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
        {"id": "start", "label": "Start", "description": "Launch WeatherStream with the saved ZIP code"},
        {"id": "stop", "label": "Stop", "description": "Terminate the WeatherStream process", "confirm": _STOP_CONFIRM_TEMPLATE},
        {
            "id": "reset_defaults",
            "label": "Reset to Defaults",
            "description": "Restore Weatharr Station settings to their default values and stop any running backend.",
            "confirm": _RESET_CONFIRM_TEMPLATE,
        },
    ]

    def __init__(self) -> None:
        self._base_dir = Path(__file__).resolve().parent
        self._plugin_key = self._base_dir.name.replace(" ", "_").lower()
        self._log_path = self._base_dir / "weatharrstation.log"
        self._http_port = 5950
        self._stream_url = f"http://127.0.0.1:{self._http_port}/weatharr.ts"
        self._channel_group_name = "Weather"
        self._channel_title = "Weatharr Station"
        self._stream_title = "Weatharr Station Feed"

        # cache for stream profile id lookup
        self._stream_profile_id: Optional[int] = None

        # defaults (fps only; no UI field)
        self._encoding_defaults = {"fps": 24}

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

    # --- action handlers ----------------------------------------------------
    def _handle_start(self, context: Dict[str, Any]) -> Dict[str, Any]:
        logger = context.get("logger")
        settings = dict(context.get("settings") or {})

        zip_code = (settings.get("zip_code") or "").strip()
        if not zip_code:
            return {"status": "error", "message": "Set the ZIP code in plugin settings before starting."}
        if not zip_code.isdigit() or len(zip_code) != 5:
            return {"status": "error", "message": "ZIP code must be a 5-digit number."}

        stored_pid = settings.get("pid")
        if stored_pid and self._is_process_running(stored_pid):
            location_name = settings.get("location_label") or self._channel_title
            return {
                "status": "running",
                "message": f"Weather stream already running for {location_name}.",
                "pid": stored_pid,
                "channel_id": settings.get("channel_id"),
                "stream_id": settings.get("stream_id"),
            }

        if stored_pid and not self._is_process_running(stored_pid):
            self._persist_settings({}, clear=["pid", "running"])
            stored_pid = None

        # resolve location label: prefer explicit field, else ZIP lookup
        location_label = (settings.get("location_name") or "").strip() or self._resolve_location(zip_code)

        encoding = self._resolve_encoding_settings(settings)

        try:
            stream, channel = self._ensure_stream_and_channel(settings, location_label)
        except Exception as exc:
            if logger:
                logger.exception("Failed to prepare WeatherStream channel resources")
            return {"status": "error", "message": f"Failed to prepare channel: {exc}"}

        try:
            pid = self._launch_process(zip_code, location_label, encoding, logger, settings)
        except Exception as exc:
            if logger:
                logger.exception("Failed to launch WeatherStream process")
            return {"status": "error", "message": f"Failed to start WeatherStream: {exc}"}

        now_iso = timezone.now().isoformat()
        persisted = self._persist_settings(
            {
                "pid": pid,
                "running": True,
                "last_started_at": now_iso,
                "stream_id": stream.id,
                "channel_id": channel.id,
                "channel_number": channel.channel_number,
                "location_label": location_label,
                "output_url": self._stream_url,
                "encoding": encoding,
            }
        )

        message = "Weather stream started."
        if location_label:
            message = f"Weather stream started for {location_label}."

        return {
            "status": "running",
            "message": message,
            "pid": pid,
            "channel_id": channel.id,
            "stream_id": stream.id,
            "channel_number": channel.channel_number,
            "settings": persisted,
        }

    def _handle_stop(self, context: Dict[str, Any]) -> Dict[str, Any]:
        logger = context.get("logger")
        settings = dict(context.get("settings") or {})
        pid = settings.get("pid")

        if not pid:
            self._persist_settings({"running": False}, clear=["pid"])
            return {"status": "stopped", "message": "Weather stream is not currently running."}

        was_running = self._terminate_process(pid, logger)
        now_iso = timezone.now().isoformat()
        persisted = self._persist_settings({"running": False, "last_stopped_at": now_iso}, clear=["pid"])

        if was_running:
            return {"status": "stopped", "message": "Weather stream stopped.", "settings": persisted}
        return {"status": "stopped", "message": "No active WeatherStream process found; state reset.", "settings": persisted}

    def _handle_status(self, context: Dict[str, Any]) -> Dict[str, Any]:
        settings = dict(context.get("settings") or {})
        running, settings = self._refresh_running_state(settings)
        message = "Weather stream is running." if running else "Weather stream is stopped."
        return {
            "status": "running" if running else "stopped",
            "message": message,
            "settings": settings,
            "pid": settings.get("pid"),
            "channel_id": settings.get("channel_id"),
            "stream_id": settings.get("stream_id"),
            "channel_number": settings.get("channel_number"),
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
        default_values["running"] = False

        clear_keys = [
            "pid",
            "stream_id",
            "channel_id",
            "location_label",
            "output_url",
            "encoding",
            "last_started_at",
        ]

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

        if current_settings.get("output_url") != self._stream_url:
            current_settings = self._persist_settings({"output_url": self._stream_url})

        pid = current_settings.get("pid")
        is_running = self._is_process_running(pid)

        if is_running:
            if not current_settings.get("running"):
                current_settings = self._persist_settings({"running": True})
            return True, current_settings

        updates: Dict[str, Any] = {}
        clear_keys: list[str] = []
        if current_settings.get("running"):
            updates["running"] = False
        if pid:
            clear_keys.append("pid")

        if updates or clear_keys:
            current_settings = self._persist_settings(updates, clear=clear_keys)

        return False, current_settings

    # --- helpers ------------------------------------------------------------
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

    def _ensure_stream_and_channel(self, settings: Dict[str, Any], location_label: Optional[str]) -> tuple[Stream, Channel]:
        # Names for creation only (we will not change existing)
        stream_name = self._stream_title if not location_label else f"{self._stream_title} ({location_label})"
        channel_name = self._channel_title if not location_label else f"{self._channel_title} - {location_label}"

        stream = self._get_or_create_stream(stream_name, settings.get("stream_id"))
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

    def _resolve_encoding_settings(self, settings: Dict[str, Any]) -> Dict[str, Any]:
        # Only FPS is used, default 24
        fps = self._encoding_defaults["fps"]
        try:
            # allow override if someone set it in DB manually; otherwise default
            val = settings.get("fps")
            if val not in (None, ""):
                fps = max(1, min(60, int(val)))
        except (TypeError, ValueError):
            fps = self._encoding_defaults["fps"]
        return {"fps": fps}

    # --- stream profile lookup (ffmpeg) ------------------------------------
    def _get_stream_profile_id(self) -> int:
        if self._stream_profile_id is not None:
            return self._stream_profile_id

        # Find a profile named "ffmpeg" (case-insensitive). Raise if not found.
        profile = (
            StreamProfile.objects.filter(name__iexact="proxy").first()
            or StreamProfile.objects.filter(name__icontains="proxy").first()
        )
        if not profile:
            raise RuntimeError("Required 'proxy' stream profile not found. You fucked up bad.")
        self._stream_profile_id = profile.id
        return self._stream_profile_id

    # --- create-only semantics ---------------------------------------------
    def _get_or_create_stream(self, name: str, stream_id: Optional[int]) -> Stream:
        if stream_id:
            try:
                return Stream.objects.get(id=stream_id)
            except Stream.DoesNotExist:
                pass

        # Try to find by exact name first without modifying anything
        existing = Stream.objects.filter(name=name).first()
        if existing:
            return existing

        # Create new with ffmpeg profile and our URL
        stream = Stream.objects.create(
            name=name,
            url=self._stream_url,
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

        # If a channel number is provided and exists, reuse as-is
        if preferred_channel_number:
            match = Channel.objects.filter(channel_number=preferred_channel_number).first()
            if match:
                return match

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
            "--out",
            self._stream_url,
        ]

        if location_label:
            cmd += ["--location-name", location_label]

        # Timezone (optional)
        tz = (settings.get("timezone") or "").strip()
        if tz:
            cmd += ["--timezone", tz]

        # --- RSS flags (optional) ---
        raw_urls = (settings.get("rss_urls") or "").strip()
        if raw_urls:
            # Accept comma, semicolon, or newline separators (works with single-line UI field)
            normalized = raw_urls.replace("\r", "\n").replace(";", ",").replace("\n", ",")
            urls = [u.strip() for u in normalized.split(",") if u.strip()]
            for url in urls:
                cmd += ["--rss-url", f'{url}']

        try:
            rss_refresh = int(settings.get("rss_refresh_sec") or 300)
        except (TypeError, ValueError):
            rss_refresh = 300
        if rss_refresh > 0:
            cmd += ["--rss-refresh-sec", str(rss_refresh)]

        try:
            rss_max = int(settings.get("rss_max_items") or 3)
        except (TypeError, ValueError):
            rss_max = 3
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

        self._log_path.parent.mkdir(parents=True, exist_ok=True)
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

    def _terminate_process(self, pid: int, logger: Any) -> bool:
        if not self._is_process_running(pid):
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

    def _is_process_running(self, pid: Optional[int]) -> bool:
        if not pid:
            return False
        try:
            pid_int = int(pid)
        except (TypeError, ValueError):
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
        runtime = {
            "pid", "running", "last_started_at", "last_stopped_at",
            "stream_id", "channel_id", "channel_number",
            "location_label", "output_url", "encoding",
        }
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
