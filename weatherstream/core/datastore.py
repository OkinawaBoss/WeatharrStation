from __future__ import annotations
import threading
import time

class DataStore:
    """Background data refresh; thread-safe read()."""
    def __init__(self, fetcher, interval_sec: float = 60.0):
        self.fetcher = fetcher
        self.interval = float(interval_sec)
        self._data = {}
        self._lock = threading.Lock()
        self._stop = False
        self._t: threading.Thread | None = None

    def start(self):
        def loop():
            while not self._stop:
                try:
                    new_data = self.fetcher() or {}
                    with self._lock:
                        self._data = new_data
                except Exception:
                    # keep running
                    pass
                time.sleep(self.interval)
        self._t = threading.Thread(target=loop, daemon=True)
        self._t.start()

    def stop(self):
        self._stop = True
        if self._t:
            self._t.join(timeout=1.0)

    def read(self) -> dict:
        with self._lock:
            return dict(self._data)
