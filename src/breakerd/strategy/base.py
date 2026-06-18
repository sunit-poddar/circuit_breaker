import abc
import logging
import math
from dataclasses import dataclass
from enum import Enum
from time import monotonic

from breakerd.store import CircuitStoreSingleton


logger = logging.getLogger(__name__)


class Strategy(Enum):
    Distributed = "distributed_pods"
    Memory = "memory"


class BreakerStates(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass
class BreakerBaseStrategyConfig:
    name: str
    recovery_timeout: int
    error_threshold_open: float
    error_threshold_close: float
    min_requests: int
    window: int


class BreakerBaseStrategy(abc.ABC):
    def __init__(self, config: BreakerBaseStrategyConfig, fallback_function, **kwargs):
        self.config = config
        self._name = config.name
        self._state = BreakerStates.CLOSED
        self._failure_count = 0
        self._last_failure = None
        self._opened = self._get_monotonic()
        self._fallback_function = fallback_function
        self._recovery_timeout = config.recovery_timeout
        self.store = CircuitStoreSingleton()
        self._strategy = "base"

    # ── Abstract interface ────────────────────────────────────────────────

    @abc.abstractmethod
    def handle_error(self, exception) -> BreakerStates:
        pass

    @abc.abstractmethod
    def handle_success(self) -> BreakerStates:
        pass

    @property
    @abc.abstractmethod
    def included_errors(self) -> list:
        pass

    @property
    @abc.abstractmethod
    def excluded_errors(self) -> list:
        pass

    @abc.abstractmethod
    def sync(self, sync_dt):
        pass

    @abc.abstractmethod
    def feature_flag_enabled(self) -> bool:
        pass

    # ── State machine ─────────────────────────────────────────────────────

    @property
    def state(self) -> BreakerStates:
        """Pure read — no side effects."""
        return self._state

    @state.setter
    def state(self, value: BreakerStates):
        self._state = value

    def _open_circuit(self):
        if self._state in (BreakerStates.CLOSED, BreakerStates.HALF_OPEN):
            self.log(f"OPENING CIRCUIT - {self.name}")
            self._state = BreakerStates.OPEN
            self._opened = self._get_monotonic()

    def _close_circuit(self):
        self.log(f"CLOSING CIRCUIT - {self.name}")
        self._state = BreakerStates.CLOSED
        self._failure_count = 0
        self.store.reset_breaker(self.name)

    def _transition_to_half_open(self):
        self.log(f"TRANSITIONING TO HALF_OPEN - {self.name}")
        self._state = BreakerStates.HALF_OPEN

    def maybe_recover(self) -> bool:
        """
        Check if the recovery timeout has elapsed and transition OPEN → HALF_OPEN.
        Returns True if a transition happened.
        """
        if self._state == BreakerStates.OPEN and self.seconds_remaining_until_circuit_is_open <= 0:
            self._transition_to_half_open()
            return True
        return False

    # ── Properties ────────────────────────────────────────────────────────

    @property
    def closed(self) -> bool:
        return self._state == BreakerStates.CLOSED

    @property
    def opened(self) -> bool:
        return self._state == BreakerStates.OPEN

    @property
    def half_open(self) -> bool:
        return self._state == BreakerStates.HALF_OPEN

    @property
    def name(self) -> str:
        return self._name

    @property
    def failure_count(self) -> int:
        return self._failure_count

    @property
    def last_failure(self):
        return self._last_failure

    @property
    def fallback_function(self):
        return self._fallback_function

    @property
    def seconds_remaining_until_circuit_is_open(self) -> int:
        remain = (self._opened + self._recovery_timeout) - self._get_monotonic()
        return math.ceil(remain) if remain > 0 else math.floor(remain)

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _get_monotonic() -> float:
        return monotonic()

    def log(self, msg, level=logging.INFO, extra=None):
        formatted = f"[CIRCUIT_BREAKER - {self.name} - {self._strategy}] {msg}"
        logger.log(level=level, msg=formatted, extra=extra or {})
