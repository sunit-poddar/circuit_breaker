import threading
from functools import wraps
from inspect import isgeneratorfunction

from breakerd.exceptions import CircuitBreakerError
from breakerd.helpers import build_failure_predicate
from breakerd.store import Store
from breakerd.strategy.base import Strategy, BreakerBaseStrategyConfig
from breakerd.strategy.distributed import DistributedPods


class BreakerInstancesSingleton:
    _instance = None
    _lock: threading.Lock = threading.Lock()

    def __init__(self):
        self._breakers = {}

    @classmethod
    def get_instance(cls) -> "BreakerInstancesSingleton":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @property
    def breakers(self):
        return self._breakers

    @breakers.setter
    def breakers(self, instance_map):
        self._breakers[instance_map["name"]] = instance_map["instance"]


class BreakerStrategiesSingleton:
    _instance = None
    _lock: threading.Lock = threading.Lock()

    def __init__(self):
        self._strategies = {}

    @classmethod
    def get_instance(cls) -> "BreakerStrategiesSingleton":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @property
    def strategies(self):
        return self._strategies

    @strategies.setter
    def strategies(self, strategy_name_map: dict):
        self._strategies[strategy_name_map["name"]] = strategy_name_map["strategy"]

    def get_strategy(
        self,
        strategy,
        name,
        recovery_timeout,
        failure_threshold_open,
        failure_threshold_close,
        fallback_function,
        redis_client=None,
    ):
        if name in self.strategies:
            return self.strategies[name]

        if strategy == Strategy.Distributed:
            distributed_config = {
                name: {"window": 60, "min_requests": 10, "read_delay": 1}
            }
            per_breaker_cfg = distributed_config.get(name, {})
            breaker_config = BreakerBaseStrategyConfig(
                name=name,
                recovery_timeout=recovery_timeout,
                error_threshold_open=failure_threshold_open,
                error_threshold_close=failure_threshold_close,
                min_requests=per_breaker_cfg.get("min_requests", DistributedPods.DEFAULT_MIN_REQUESTS),
                window=per_breaker_cfg.get("window", DistributedPods.DEFAULT_WINDOW),
            )
            instance = DistributedPods(
                breaker_config,
                fallback_function,
                redis_client=redis_client,
                read_delay=per_breaker_cfg.get("read_delay", DistributedPods.DEFAULT_WINDOW_READ_DELAY),
            )
            self.strategies = {"strategy": instance, "name": name}
            return instance

        return None

    @property
    def all_closed(self) -> bool:
        return len(list(self.get_open)) == 0

    @property
    def get_strategy_names(self):
        return self.strategies.keys()

    @property
    def get_strategies(self):
        return self.strategies.values()

    def get(self, name):
        return self.strategies.get(name)

    @property
    def get_open(self):
        for s in self.get_strategies:
            if s.opened:
                yield s.name

    @property
    def get_closed(self):
        for s in self.get_strategies:
            if s.closed:
                yield s.name


class BreakerService:
    DEFAULT_FAILURE_THRESHOLD = 0.5
    DEFAULT_RECOVERY_TIMEOUT = 30
    DEFAULT_EXPECTED_EXCEPTION = Exception
    DEFAULT_FALLBACK_FUNCTION = None

    def __init__(
        self,
        failure_threshold_open=None,
        failure_threshold_close=None,
        recovery_timeout=None,
        name=None,
        fallback_function=None,
        strategy: Strategy = None,
        redis_client=None,
    ):
        self._failure_threshold_open = failure_threshold_open or self.DEFAULT_FAILURE_THRESHOLD
        self._failure_threshold_close = failure_threshold_close or self.DEFAULT_FAILURE_THRESHOLD
        self._recovery_timeout = recovery_timeout or self.DEFAULT_RECOVERY_TIMEOUT
        self._fallback_function = fallback_function or self.DEFAULT_FALLBACK_FUNCTION
        self._name = name
        self._strategy = strategy
        self._redis_client = redis_client
        # Cache the strategy instance at decoration time so every call does not
        # traverse the singleton chain.
        self._strategy_instance = None

    def get_strategy(self):
        if self._strategy_instance is None:
            self._strategy_instance = BreakerStrategiesSingleton.get_instance().get_strategy(
                self._strategy,
                self._name,
                self._recovery_timeout,
                self._failure_threshold_open,
                self._failure_threshold_close,
                self._fallback_function,
                redis_client=self._redis_client,
            )
        return self._strategy_instance

    def is_failure(self, exc_type, exc_value):
        return build_failure_predicate(
            self.get_strategy().included_errors or self.DEFAULT_EXPECTED_EXCEPTION
        )(exc_type, exc_value)

    def __call__(self, wrapped):
        return self.decorate(wrapped)

    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc_value, _traceback):
        if not self.get_strategy().feature_flag_enabled():
            return False

        if exc_type and self.is_failure(exc_type, exc_value):
            self.__call_failed(exc_value)
        else:
            self.__call_succeeded()

        # Re-raise the original exception; callers decide whether to swallow it.
        return False

    def decorate(self, function):
        if isgeneratorfunction(function):
            call = self.call_generator
        else:
            call = self.call

        @wraps(function)
        def wrapper(*args, **kwargs):
            strategy = self.get_strategy()

            if not strategy.feature_flag_enabled():
                strategy.log("strategy not enabled")
                return call(function, *args, **kwargs)

            strategy.log(f"CURRENT STATE -- {strategy.state} | OPENED={strategy.opened}")

            # Check if a timed-out OPEN circuit should move to HALF_OPEN.
            strategy.maybe_recover()

            if strategy.opened:
                # Still within recovery cooldown — reject immediately.
                if strategy.fallback_function:
                    return strategy.fallback_function(*args, **kwargs)
                raise CircuitBreakerError(strategy)

            if strategy.half_open:
                # Allow exactly one probe request through.  Success/failure is
                # handled via __exit__ in the context manager below.
                return call(function, *args, **kwargs)

            # CLOSED — check whether to open before forwarding the call.
            buffered_data = strategy.store.get_breaker(strategy.name) or {}
            if strategy._should_open(
                buffered_data.get(Store.SUCCESS), buffered_data.get(Store.FAILED), sync=False
            ):
                strategy._open_circuit()

            if strategy.opened:
                if strategy.fallback_function:
                    return strategy.fallback_function(*args, **kwargs)
                raise CircuitBreakerError(strategy)

            return call(function, *args, **kwargs)

        return wrapper

    def call(self, func, *args, **kwargs):
        with self:
            return func(*args, **kwargs)

    def call_generator(self, func, *args, **kwargs):
        with self:
            for el in func(*args, **kwargs):
                yield el

    def __call_succeeded(self):
        self.get_strategy().handle_success()

    def __call_failed(self, exception):
        self.get_strategy().handle_error(exception)

    def __str__(self):
        return self.get_strategy().name


def circuit_breaker(
    name,
    strategy: Strategy = Strategy.Distributed,
    failure_threshold=None,
    failure_threshold_close=None,
    recovery_timeout=None,
    fallback_function=None,
    redis_client=None,
):
    """
    Circuit breaker decorator.

    Usage:
        @circuit_breaker(name="feed_api", failure_threshold=0.5, recovery_timeout=30)
        def call_feed_service():
            ...
    """
    if callable(name):
        # Used as @circuit_breaker without arguments — wrap directly.
        return BreakerService().decorate(name)

    if not name:
        raise ValueError(
            "circuit_breaker requires a name. Example: @circuit_breaker(name='feed_api')"
        )

    breaker_instances = BreakerInstancesSingleton.get_instance()
    if name in breaker_instances.breakers:
        return breaker_instances.breakers[name]

    breaker_instance = BreakerService(
        failure_threshold_open=failure_threshold,
        failure_threshold_close=failure_threshold_close,
        recovery_timeout=recovery_timeout,
        name=name,
        fallback_function=fallback_function,
        strategy=strategy,
        redis_client=redis_client,
    )
    breaker_instances.breakers = {"name": name, "instance": breaker_instance}
    return breaker_instance


# Keep the original name as an alias so old imports still work during migration.
circuit = circuit_breaker
