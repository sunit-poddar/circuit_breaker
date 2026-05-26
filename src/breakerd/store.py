import threading
from datetime import datetime


class Store:
    MEMBER_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"

    SUCCESS = "success"
    FAILED = "failed"
    TOTAL = "total"
    WINDOW_START = "window_start"
    PAST_WINDOW = "past_window"
    PAST_WINDOW_END = "end"
    WINDOW_KEY = "window"


class CircuitStoreSingleton:
    """
    In-memory buffer for per-breaker request counters.

    Holds success/failure counts locally until the distributed strategy
    flushes them to Redis.  Implemented as a singleton so all strategy
    instances on the same pod share one buffer.
    """
    __instance = None

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    def __init__(self):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self.__circuits = {}
            self._lock = threading.Lock()

    def _initialize_if_absent(self, breaker_name: str) -> None:
        if breaker_name not in self.__circuits:
            self.__circuits[breaker_name] = {
                Store.TOTAL: 0,
                Store.SUCCESS: 0,
                Store.FAILED: 0,
                Store.WINDOW_START: datetime.now().strftime(Store.MEMBER_TIMESTAMP_FORMAT),
                Store.PAST_WINDOW: {},
            }

    @property
    def circuits(self):
        return self.__circuits

    @circuits.setter
    def circuits(self, breaker_name):
        self._initialize_if_absent(breaker_name)

    def get_past_window(self, breaker_name):
        with self._lock:
            return self.__circuits.get(breaker_name, {}).get(Store.PAST_WINDOW)

    def update_past_window(self, breaker_name, past_window):
        with self._lock:
            if breaker_name in self.__circuits:
                self.__circuits[breaker_name][Store.PAST_WINDOW] = {
                    **self.__circuits[breaker_name][Store.PAST_WINDOW],
                    **past_window,
                }

    def get_breaker(self, breaker_name):
        with self._lock:
            self._initialize_if_absent(breaker_name)
            return dict(self.__circuits[breaker_name])

    def record_success(self, breaker_name, increment=1):
        with self._lock:
            self._initialize_if_absent(breaker_name)
            self.__circuits[breaker_name][Store.SUCCESS] += increment
            self.__circuits[breaker_name][Store.TOTAL] += increment
            return dict(self.__circuits[breaker_name])

    def record_failure(self, breaker_name, increment=1):
        with self._lock:
            self._initialize_if_absent(breaker_name)
            self.__circuits[breaker_name][Store.FAILED] += increment
            self.__circuits[breaker_name][Store.TOTAL] += increment
            return dict(self.__circuits[breaker_name])

    def reset_breaker(self, breaker_name):
        with self._lock:
            self.__circuits.pop(breaker_name, None)
            self._initialize_if_absent(breaker_name)
