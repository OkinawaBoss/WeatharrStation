# weatherstream/output/stream_ffmpeg.py
from __future__ import annotations

import errno
import os
import platform
import queue
import shutil
import subprocess
import threading
from pathlib import Path
from functools import lru_cache
from urllib.parse import urlparse
from ctypes.util import find_library


class FFMPEGStreamer:
    def __init__(
        self,
        width: int,
        height: int,
        fps: int,
        out_url: str,
        *,
        out_width: int | None = None,
        out_height: int | None = None,
        voice_fifo: str | None = None,
        music_fifo: str | None = None,
        music_playlist: str | None = None,
        vb_kbps: int = 3500,
        ab_kbps: int = 128,
        muxrate_kbps: int | None = None,
        muxrate_factor: float = 1.08,
        gop_seconds: float = 1.0,
        force_cfr: bool = False,
        use_wallclock_ts: bool = False,
        audio_sample_rate: int = 48000,
        inject_silence_if_missing: bool = True,
        video_encoder: str = "auto",        # "auto"|"h264_videotoolbox"|"h264_nvenc"|"h264_qsv"|"h264_amf"|"libx264"
        encoder_preset: str = "veryfast",
        threads: int = 2,
        srt_latency_ms: int = 120,
        srt_mode: str = "listener",
        udp_pkt_size: int = 1316,
        tcp_listen: bool = True,
        tcp_nodelay: bool = True,
        pat_period: float = 0.5,
        pcr_period_ms: int = 40,
        flush_packets: bool = False,
        print_cmd: bool = False,
        drop_when_behind: bool = True,
        max_queue: int = 2,
    ):
        self.width = int(width)
        self.height = int(height)
        self.fps = int(fps)
        self.out_url = str(out_url)
        self.out_width = int(out_width) if out_width else None
        self.out_height = int(out_height) if out_height else None

        self.voice_fifo = voice_fifo
        self.music_fifo = music_fifo
        self.music_playlist = music_playlist

        self.vb_kbps = int(vb_kbps)
        self.ab_kbps = int(ab_kbps)
        self.muxrate_kbps = int(muxrate_kbps) if muxrate_kbps else None
        self.muxrate_factor = float(muxrate_factor)

        self.gop_seconds = float(gop_seconds)
        self.force_cfr = bool(force_cfr)
        self.use_wallclock_ts = bool(use_wallclock_ts)

        self.audio_sample_rate = int(audio_sample_rate)
        self.inject_silence_if_missing = bool(inject_silence_if_missing)

        self.video_encoder = video_encoder.lower()
        self.encoder_preset = encoder_preset
        self.threads = int(threads)

        self.srt_latency_ms = int(srt_latency_ms)
        self.srt_mode = str(srt_mode)
        self.udp_pkt_size = int(udp_pkt_size)
        self.tcp_listen = bool(tcp_listen)
        self.tcp_nodelay = bool(tcp_nodelay)

        self.pat_period = float(pat_period)
        self.pcr_period_ms = int(pcr_period_ms)
        self.flush_packets = bool(flush_packets)

        self.print_cmd = bool(print_cmd)
        self.proc: subprocess.Popen | None = None
        self.drop_when_behind = bool(drop_when_behind)
        self.max_queue = max(1, int(max_queue))
        self._queue: queue.Queue[bytes] | None = None
        self._writer_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._proc_dead = False
        self._proc_lock = threading.Lock()

    # ------------------------- helpers -------------------------

    def _choose_encoder(self) -> tuple[str, list[str]]:
        g = max(1, int(round(self.fps * self.gop_seconds)))
        # Common H.264 rate control; keep universally supported flags only.
        common = [
            "-g", str(g),
            "-keyint_min", str(g),
            "-bf", "0",
            # (No -sc_threshold here; some encoders ignore it and warn)
            "-pix_fmt", "yuv420p",
            "-b:v", f"{self.vb_kbps}k",
            "-maxrate", f"{self.vb_kbps}k",
            "-bufsize", f"{self.vb_kbps * 2}k",
        ]

        enc = self.video_encoder
        if enc == "auto":
            sys = platform.system().lower()
            if sys == "darwin":
                order = ["h264_videotoolbox", "libx264"]
            elif sys == "windows":
                order = ["h264_nvenc", "h264_qsv", "h264_amf", "libx264"]
            else:
                order = ["h264_nvenc", "h264_qsv", "libx264"]
            return self._enc_args_try(order, common)

        if enc in {"h264_videotoolbox", "h264_nvenc", "h264_qsv", "h264_amf", "libx264"}:
            if not self._encoder_supported(enc):
                raise RuntimeError(f"Requested encoder '{enc}' is not available on this host.")
            return self._enc_args(enc, common)

        return self._enc_args("libx264", common)

    def _enc_args_try(self, order: list[str], base: list[str]) -> tuple[str, list[str]]:
        for candidate in order:
            if self._encoder_supported(candidate):
                return self._enc_args(candidate, base)
        return self._enc_args("libx264", base)

    def _enc_args(self, enc: str, base: list[str]) -> tuple[str, list[str]]:
        args: list[str] = []
        if enc == "h264_videotoolbox":
            args = [
                "-c:v", "h264_videotoolbox",
                "-profile:v", "high",
                "-realtime", "1",
            ]
        elif enc == "h264_nvenc":
            preset_map = {
                "placebo": "p1", "veryslow": "p2", "slower": "p3", "slow": "p4",
                "medium": "p5", "fast": "p6", "faster": "p6", "veryfast": "p7", "ultrafast": "p7"
            }
            p = preset_map.get(self.encoder_preset, "p5")
            args = [
                "-c:v", "h264_nvenc",
                "-preset", p,
                "-tune", "ull",
                "-rc", "cbr",
                "-zerolatency", "1",
                "-delay", "0",
            ]
        elif enc == "h264_qsv":
            args = [
                "-c:v", "h264_qsv",
                "-global_quality", "0",
                "-look_ahead", "0",
                "-bf", "0",
                "-qsv_device", "auto",
            ]
        elif enc == "h264_amf":
            args = [
                "-c:v", "h264_amf",
                "-usage", "lowlatency",
                "-rc", "cbr",
                "-bf", "0",
            ]
        else:  # libx264
            args = [
                "-c:v", "libx264",
                "-tune", "zerolatency",
                "-preset", self.encoder_preset,
                "-threads", str(max(0, self.threads)),
                "-x264-params", "nal-hrd=cbr:force-cfr=1:repeat-headers=1:scenecut=0",
            ]
        return enc, args + base

    def _encoder_supported(self, enc: str) -> bool:
        enc = (enc or "").lower()
        sys = platform.system().lower()
        if enc == "h264_videotoolbox":
            return sys == "darwin"
        if enc == "h264_nvenc":
            return self._nvenc_available()
        if enc == "h264_qsv":
            if sys == "windows":
                return True
            if sys == "linux":
                return os.path.exists("/dev/dri/renderD128") or os.path.exists("/dev/dri/card0")
            return False
        if enc == "h264_amf":
            return sys == "windows"
        if enc == "libx264":
            return True
        return False

    @staticmethod
    @lru_cache(maxsize=None)
    def _nvenc_available() -> bool:
        lib = find_library("cuda")
        if lib:
            return True
        known_paths = [
            "/usr/lib64/nvidia/libcuda.so.1",
            "/usr/lib/x86_64-linux-gnu/libcuda.so.1",
            "/usr/lib/wsl/lib/libcuda.so",
            "/usr/local/cuda/lib64/libcuda.so.1",
        ]
        for path in known_paths:
            if os.path.exists(path):
                return True
        return shutil.which("nvidia-smi") is not None

    def _dest(self) -> tuple[str, str, list[str], bool, bool]:
        """
        Returns: (format, url, extra_args, is_ts_like, is_http_ts)
        """
        parsed = urlparse(self.out_url)
        scheme = (parsed.scheme or "").lower()
        url = self.out_url
        extra: list[str] = []

        if scheme == "udp":
            if "pkt_size=" not in url:
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}pkt_size={self.udp_pkt_size}"
            return "mpegts", url, extra, True, False

        if scheme == "tcp":
            params = []
            if self.tcp_listen and "listen=" not in url:
                params.append("listen=1")
            if self.tcp_nodelay and "tcp_nodelay=" not in url:
                params.append("tcp_nodelay=1")
            if params:
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}{'&'.join(params)}"
            return "mpegts", url, extra, True, False

        if scheme == "srt":
            params = []
            if "mode=" not in url:
                params.append(f"mode={self.srt_mode}")
            if "transtype=" not in url:
                params.append("transtype=live")
            if "latency=" not in url:
                params.append(f"latency={self.srt_latency_ms}")
            if "linger=" not in url:
                params.append("linger=0")
            if params:
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}{'&'.join(params)}"
            return "mpegts", url, extra, True, False

        if scheme in {"http", "https"}:
            extra = ["-listen", "1"]
            return "mpegts", url, extra, True, True

        # File / unknown
        path = parsed.path or ""
        if path.lower().endswith((".mp4", ".mov", ".m4v")):
            return "mp4", path or "out.mp4", extra, False, False

        return "mpegts", (path or "out.ts"), extra, True, False

    # ------------------------- lifecycle -------------------------

    def start(self):
        if self.proc and self.proc.poll() is None:
            return
        if shutil.which("ffmpeg") is None:
            raise RuntimeError("ffmpeg executable not found in PATH")

        out_fmt, out_url, extra_args, is_ts, is_http_ts = self._dest()

        v_k = self.vb_kbps
        a_k = self.ab_kbps
        mux_k = self.muxrate_kbps if self.muxrate_kbps else None

        enc_name, v_args = self._choose_encoder()

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-fflags", "+genpts",
            "-thread_queue_size", "8192",
            "-f", "rawvideo",
            "-pix_fmt", "rgba",
            "-s", f"{self.width}x{self.height}",
            "-r", str(self.fps),
        ]
        if self.use_wallclock_ts:
            cmd += ["-use_wallclock_as_timestamps", "1"]
        cmd += ["-i", "-"]

        if self.out_width and self.out_height and (
            self.out_width != self.width or self.out_height != self.height
        ):
            cmd += ["-vf", f"scale={self.out_width}:{self.out_height}"]

        # Optional audio inputs
        idx = 1
        voice_idx = None
        music_idx = None

        if self.voice_fifo:
            cmd += [
                "-thread_queue_size", "4096",
                "-f", "s16le", "-ar", str(self.audio_sample_rate), "-ac", "2",
                "-i", self.voice_fifo,
            ]
            voice_idx = idx; idx += 1

        if self.music_fifo:
            cmd += [
                "-thread_queue_size", "4096",
                "-f", "s16le", "-ar", str(self.audio_sample_rate), "-ac", "2",
                "-i", self.music_fifo,
            ]
            music_idx = idx; idx += 1
        elif self.music_playlist and Path(self.music_playlist).exists():
            cmd += [
                "-thread_queue_size", "4096",
                "-stream_loop", "-1",
                "-f", "concat",
                "-safe", "0",
                "-i", self.music_playlist,
            ]
            music_idx = idx; idx += 1

        if voice_idx is None and music_idx is None and self.inject_silence_if_missing:
            cmd += ["-f", "lavfi", "-i", f"anullsrc=channel_layout=stereo:sample_rate={self.audio_sample_rate}"]
            voice_idx = idx; idx += 1

        # Audio mixing / ducking
        filter_args = []
        if voice_idx is not None and music_idx is not None:
            af = (
                f"[{music_idx}:a][{voice_idx}:a]"
                "sidechaincompress=threshold=0.035:ratio=10:attack=5:release=250:makeup=4[duck];"
                f"[duck][{voice_idx}:a]"
                "amix=inputs=2:normalize=0:duration=longest:dropout_transition=0[aout]"
            )
            filter_args = ["-filter_complex", af]
            audio_map = "[aout]"
        else:
            only = voice_idx if voice_idx is not None else music_idx
            audio_map = f"{only}:a" if only is not None else None

        # TS mux shaping
        mpegts_mux_opts: list[str] = []
        if is_ts:
            # Always resend headers/mark discontinuity
            mpegts_mux_opts += ["-mpegts_flags", "+resend_headers+initial_discontinuity"]

            if is_http_ts:
                # Relaxed for HTTP to avoid DTS<PCR & client resets
                mpegts_mux_opts += [
                    "-flush_packets", "1" if self.flush_packets else "0",
                    "-muxpreload", "0.5",
                    "-muxdelay", "0.7",
                    # no -muxrate on HTTP
                    # (leave PCR/INTERLEAVE at ffmpeg defaults)
                ]
            else:
                # Tight CBR for UDP/SRT/TCP
                mpegts_mux_opts += [
                    "-flush_packets", "1" if self.flush_packets else "0",
                    "-max_interleave_delta", "0",
                    "-muxpreload", "0",
                    "-muxdelay", "0",
                    "-pat_period", str(self.pat_period),
                    "-pcr_period", str(self.pcr_period_ms),
                ]
                if mux_k:
                    mpegts_mux_opts += ["-muxrate", f"{mux_k}k"]

        # Build final command
        cmd += filter_args
        cmd += ["-map", "0:v:0"]
        if audio_map:
            cmd += ["-map", audio_map]

        cmd += v_args
        if enc_name == "h264_videotoolbox":
            cmd += ["-bsf:v", "dump_extra"]

        if self.force_cfr:
            cmd += ["-vsync", "cfr", "-fps_mode", "cfr"]

        cmd += [
            "-c:a", "aac",
            "-b:a", f"{self.ab_kbps}k",
            "-ar", str(self.audio_sample_rate),
        ]

        cmd += mpegts_mux_opts
        cmd += extra_args
        cmd += ["-f", out_fmt, out_url]

        if self.print_cmd:
            print("FFmpeg CMD:\n", " ".join(cmd), flush=True)

        try:
            with self._proc_lock:
                self.proc = subprocess.Popen(
                    cmd,
                    stdin=subprocess.PIPE,
                    bufsize=0,
                )
                self._proc_dead = False
        except FileNotFoundError as exc:
            raise RuntimeError("ffmpeg executable not found; ensure ffmpeg is installed") from exc

        if self.drop_when_behind:
            if self._queue is None or self._queue.maxsize != self.max_queue:
                self._queue = queue.Queue(maxsize=self.max_queue)
            if not self._writer_thread or not self._writer_thread.is_alive():
                self._stop_event.clear()
                self._writer_thread = threading.Thread(
                    target=self._writer_loop,
                    name="ffmpeg-writer",
                    daemon=True,
                )
                self._writer_thread.start()

    def _restart(self) -> None:
        try:
            self.stop()
        except Exception:
            pass
        self.start()

    def _write_all(self, payload: bytes) -> None:
        proc = self.proc
        if proc is None or proc.stdin is None:
            raise RuntimeError("FFmpeg process not available")
        view = memoryview(payload)
        while view:
            written = os.write(proc.stdin.fileno(), view)
            view = view[written:]

    def _writer_loop(self) -> None:
        while not self._stop_event.is_set():
            if self._queue is None:
                break
            try:
                payload = self._queue.get(timeout=0.25)
            except queue.Empty:
                continue
            if payload is None:
                break
            try:
                with self._proc_lock:
                    proc = self.proc
                    if proc is None or proc.poll() is not None or proc.stdin is None:
                        self._proc_dead = True
                        continue
                    self._write_all(payload)
            except (BrokenPipeError, ConnectionResetError, OSError):
                self._proc_dead = True
                continue

    def send(self, frame) -> bool:
        if self.proc is None or self.proc.poll() is not None or self._proc_dead:
            self.start()
        if self.proc is None:
            raise RuntimeError("FFmpeg process not available")

        if isinstance(frame, (bytes, bytearray, memoryview)):
            payload = frame
        elif hasattr(frame, "tobytes"):
            payload = frame.tobytes()
        else:
            raise TypeError(f"Unsupported frame type: {type(frame)!r}")

        if self.drop_when_behind:
            if self._queue is None:
                self._queue = queue.Queue(maxsize=self.max_queue)
            try:
                self._queue.put_nowait(payload)
                return True
            except queue.Full:
                # Drop frame if writer is backed up.
                return False

        try:
            self._write_all(payload)
            return True
        except (BrokenPipeError, ConnectionResetError):
            print("[FFMPEGStreamer] Output connection closed; restarting ffmpeg…", flush=True)
            self._restart()
            return False
        except OSError as exc:
            if exc.errno in {errno.EPIPE, errno.ECONNRESET}:
                print("[FFMPEGStreamer] Output write failed; restarting ffmpeg…", flush=True)
                self._restart()
                return False
            raise

    def stop(self):
        if self.drop_when_behind:
            self._stop_event.set()
            if self._queue is not None:
                try:
                    self._queue.put_nowait(None)
                except queue.Full:
                    pass
            if self._writer_thread and self._writer_thread.is_alive():
                if threading.current_thread() is not self._writer_thread:
                    self._writer_thread.join(timeout=2.0)
            self._writer_thread = None
        if self.proc:
            try:
                if self.proc.stdin:
                    self.proc.stdin.flush()
                    self.proc.stdin.close()
            except Exception:
                pass
            try:
                self.proc.wait(timeout=3)
            except Exception:
                pass
        self.proc = None
