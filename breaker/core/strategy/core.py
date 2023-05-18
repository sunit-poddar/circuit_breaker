import abc
import math
from enum import Enum
from time import monotonic
import logging

from dataclasses import dataclass

from core.store import CircuitStoreSingleton


logger = logging.getLogger(__name__)


class Strategy(Enum):
    Distributed = "distributed_pods"


class BreakerStates(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"


@dataclass
class BreakerBaseStrategyConfig:
    """class for storing breaker strategy configuration"""
    name: str
    recovery_timeout: int  # in seconds  ## open the circuit for this much time
    error_threshold_open: float  # error should be more than this to open the circuit
    error_threshold_close: float  # error should be less than this to close the circuit
    min_requests: int  # minimum number of requests before circuit open decision can be taken
    window: int  # in seconds


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
        self._strategy = None

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

    @property
    def state(self) -> BreakerStates:
        if self._state == BreakerStates.OPEN and self.seconds_remaining_until_circuit_is_open <= 0:
            self._state = BreakerStates.CLOSED
            self._opened = self._get_monotonic()
            self._failure_count = 0
            self.store.reset_breaker(self.name)
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    def _open_circuit(self):
        if self.state == BreakerStates.CLOSED:
            logger.info(f"[CIRCUIT_BREAKER] OPENING CIRCUIT - {self.name}")
            self.state = BreakerStates.OPEN
            self._opened = self._get_monotonic()

    def _close_circuit(self):
        if self.state == BreakerStates.OPEN:
            logger.info(f"[CIRCUIT_BREAKER] CLOSING CIRCUIT - {self.name}")
            self.state = BreakerStates.CLOSED

    @property
    def failure_count(self):
        return self._failure_count

    @property
    def closed(self):
        return self.state == BreakerStates.CLOSED

    @property
    def opened(self):
        return self.state == BreakerStates.OPEN

    @property
    def name(self):
        return self._name

    @property
    def last_failure(self):
        return self._last_failure

    @property
    def seconds_remaining_until_circuit_is_open(self):
        """
        Number of seconds remaining, the circuit breaker stays in OPEN state
        :return: int
        """
        remain = (self._opened + self._recovery_timeout) - self._get_monotonic()
        return math.ceil(remain) if remain > 0 else math.floor(remain)

    @property
    def fallback_function(self):
        return self._fallback_function

    @staticmethod
    def _get_monotonic():
        return monotonic()

    def log(self, msg, level=logging.INFO, extra={}):
        msg = f"[CIRCUIT_BREAKER - {self.name} - {self._strategy}] {msg}"
        logger.log(level=level, msg=msg, extra=extra)
