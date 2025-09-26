from __future__ import annotations
import os
import subprocess
import tempfile
import time
from threading import Thread
from pathlib import Path
from typing import List

AUDIO_EXTS = {".mp3", ".m4a", ".aac", ".wav", ".flac", ".ogg", ".oga"}


class MusicPipe:
    """Streams a repeating playlist to a PCM FIFO using a single ffmpeg process."""

    def __init__(self, music_dir: str, fifo_path: str):
        self.music_dir = Path(music_dir)
        self.fifo_path = Path(fifo_path)
        self._thread: Thread | None = None
        self._stop = False
        self._proc: subprocess.Popen | None = None
        self._playlist_file: Path | None = None

    def ensure_fifo(self) -> None:
        if self.fifo_path.exists():
            try:
                st = os.stat(self.fifo_path)
                if not ((st.st_mode & 0o170000) == 0o010000):
                    self.fifo_path.unlink(missing_ok=True)
            except Exception:
                pass
        if not self.fifo_path.exists():
            os.mkfifo(self.fifo_path)

    def start(self) -> None:
        self._stop = False
        if self._thread and self._thread.is_alive():
            return
        self._thread = Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop = True
        try:
            if self._proc:
                self._proc.terminate()
                self._proc.wait(timeout=2)
        except Exception:
            pass
        self._proc = None
        if self._thread:
            try:
                self._thread.join(timeout=1.0)
            except Exception:
                pass
            self._thread = None
        if self._playlist_file and self._playlist_file.exists():
            try:
                self._playlist_file.unlink()
            except Exception:
                pass
        self._playlist_file = None

    # ---------- internals ----------

    def _collect_tracks(self) -> List[Path]:
        if not self.music_dir.exists():
            return []
        return [
            p for p in sorted(self.music_dir.iterdir())
            if p.is_file() and p.suffix.lower() in AUDIO_EXTS
        ]

    def _build_playlist(self, tracks: List[Path]) -> Path | None:
        if not tracks:
            return None
        playlist = tempfile.NamedTemporaryFile("w", delete=False, suffix=".txt")
        try:
            for track in tracks:
                playlist.write(f"file '{track.as_posix()}'\n")
        finally:
            playlist.flush()
            playlist.close()
        return Path(playlist.name)

    def _run(self) -> None:
        while not self._stop:
            tracks = self._collect_tracks()
            if not tracks:
                # Wait a bit and retry if the folder is empty
                if self._stop:
                    break
                time.sleep(0.5)
                continue

            playlist_path = self._build_playlist(tracks)
            if playlist_path is None:
                time.sleep(0.5)
                continue
            self._playlist_file = playlist_path

            cmd = [
                "ffmpeg",
                "-hide_banner", "-loglevel", "error",
                "-re",
                "-stream_loop", "-1",
                "-f", "concat",
                "-safe", "0",
                "-i", str(playlist_path),
                "-ar", "48000", "-ac", "2",
                "-f", "s16le",
                "-y",
                str(self.fifo_path),
            ]
            try:
                self._proc = subprocess.Popen(cmd)
                self._proc.wait()
            except Exception:
                self._proc = None
                time.sleep(0.5)
            finally:
                try:
                    if playlist_path.exists():
                        playlist_path.unlink()
                except Exception:
                    pass
                self._playlist_file = None

            if self._stop:
                break
