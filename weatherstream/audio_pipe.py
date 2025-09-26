from __future__ import annotations
import os, time, queue, tempfile, subprocess, threading
from threading import Thread
from pathlib import Path

RATE = 48000
CHANNELS = 2
SAMPLE_BYTES = 2  # s16le
BYTES_PER_SEC = RATE * CHANNELS * SAMPLE_BYTES
CHUNK_BYTES = 8192  # smaller chunks give better pacing accuracy


class AudioPipe:
    """
    Writes continuous PCM s16le stereo 44.1k audio to a FIFO.
    Uses cross-platform pyttsx3 (offline) for narration.
    Now with an async TTS worker so main loop never blocks.
    """
    def __init__(self, fifo_path: str):
        self.fifo_path = Path(fifo_path)
        self.q: "queue.Queue[bytes]" = queue.Queue()        # PCM segments to write
        self.tts_q: "queue.Queue[tuple[str, str | None, int]]" = queue.Queue()  # (text, voice, rate)
        self._stop = False
        self._writer: Thread | None = None
        self._tts: Thread | None = None
        self._fp = None  # FIFO handle

    # ---------- public API ----------
    def ensure_fifo(self):
        if self.fifo_path.exists():
            try:
                st = os.stat(self.fifo_path)
                # If not a FIFO, replace it
                if not ((st.st_mode & 0o170000) == 0o010000):  # S_IFIFO
                    self.fifo_path.unlink(missing_ok=True)
            except Exception:
                pass
        if not self.fifo_path.exists():
            os.mkfifo(self.fifo_path)

    def start(self):
        """
        Open FIFO RDWR so it never blocks even if the other end isn't opened yet.
        Then start the writer thread and the TTS worker.
        """
        self._stop = False
        fd = os.open(str(self.fifo_path), os.O_RDWR)
        self._fp = os.fdopen(fd, "wb", buffering=0)
        self._writer = Thread(target=self._writer_loop, daemon=True)
        self._writer.start()
        self._tts = Thread(target=self._tts_worker, daemon=True)
        self._tts.start()

    def stop(self):
        self._stop = True
        try:
            if self._tts:
                self._tts.join(timeout=1.0)
        except Exception:
            pass
        try:
            if self._writer:
                self._writer.join(timeout=1.0)
        except Exception:
            pass
        try:
            if self._fp:
                self._fp.close()
        except Exception:
            pass

    def speak(self, text: str, voice: str | None = None, rate_wpm: int = 190):
        """
        Queue text for async synthesis. Non-blocking.
        """
        if (text or "").strip():
            self.tts_q.put((text, voice, rate_wpm))

    # ---------- internal ----------
    def _writer_loop(self):
        silence = b"\x00" * CHUNK_BYTES
        next_ts = time.perf_counter()

        while not self._stop:
            try:
                segment = self.q.get_nowait()
            except queue.Empty:
                segment = silence

            mv = memoryview(segment)
            pos = 0
            n = len(segment)
            while pos < n and not self._stop:
                end = min(pos + CHUNK_BYTES, n)
                chunk = mv[pos:end]
                self._fp.write(chunk)
                pos = end

                next_ts += len(chunk) / BYTES_PER_SEC
                sleep_for = next_ts - time.perf_counter()
                if sleep_for > 0:
                    time.sleep(min(sleep_for, 0.05))
                else:
                    # If we fell behind, realign clock to avoid drift.
                    next_ts = time.perf_counter()

    def _tts_worker(self):
        while not self._stop:
            try:
                text, voice, rate_wpm = self.tts_q.get(timeout=0.25)
            except queue.Empty:
                continue
            pcm = tts_to_pcm_bytes(text, voice=voice, rate_wpm=rate_wpm)
            if pcm:
                self.q.put(pcm)


def tts_to_pcm_bytes(text: str, voice: str | None = None, rate_wpm: int = 190) -> bytes | None:
    """
    Cross-platform TTS using pyttsx3 only.
    - Windows: SAPI5
    - macOS: NSSpeechSynthesizer
    - Linux: eSpeak/eSpeak-NG (install 'espeak-ng')
    Converts to s16le 44.1k stereo with ffmpeg and returns raw PCM bytes.
    """
    text = (text or "").strip()
    if not text:
        return None

    try:
        import pyttsx3
    except Exception:
        print("[AudioPipe] pyttsx3 not installed; skipping speech.")
        return None

    with tempfile.TemporaryDirectory() as td:
        from pathlib import Path
        td_path = Path(td)
        wav = td_path / "tts.wav"

        eng = pyttsx3.init()
        if voice:
            try:
                for v in eng.getProperty("voices") or []:
                    name = (getattr(v, "name", "") or "").lower()
                    if voice.lower() in name:
                        eng.setProperty("voice", v.id)
                        break
            except Exception:
                pass
        try:
            eng.setProperty("rate", int(rate_wpm))
        except Exception:
            pass

        eng.save_to_file(text, str(wav))
        eng.runAndWait()

        try:
            pcm = subprocess.run(
                [
                    "ffmpeg", "-hide_banner", "-loglevel", "error",
                    "-i", str(wav),
                    "-ar", "48000", "-ac", "2", "-f", "s16le", "pipe:1",
                ],
                check=True,
                capture_output=True,
            ).stdout
            return pcm
        except Exception as e:
            print(f"[AudioPipe] ffmpeg convert failed: {e}")
            return None
