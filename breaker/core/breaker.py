from functools import wraps
from inspect import isgeneratorfunction

from core.helpers import build_failure_predicate
from core.store import Store
from core.strategy.DistributedPodsStrategy import DistributedPods
from core.strategy.core import Strategy, BreakerBaseStrategyConfig
from core.utils import CircuitBreakerError


class BreakerInstancesSingleton:
    _instance = None

    def __init__(self):
        self._breakers = {}

    @staticmethod
    def get_instance():
        if BreakerInstancesSingleton._instance is None:
            BreakerInstancesSingleton._instance = BreakerInstancesSingleton()
        return BreakerInstancesSingleton._instance

    @property
    def breakers(self):
        return self._breakers

    @breakers.setter
    def breakers(self, instance_map):
        self._breakers[instance_map['name']] = instance_map['instance']


class BreakerStrategiesSingleton:
    _instance = None

    def __init__(self):
        self._strategies = {}

    @staticmethod
    def get_instance():
        if BreakerStrategiesSingleton._instance is None:
            BreakerStrategiesSingleton._instance = BreakerStrategiesSingleton()
        return BreakerStrategiesSingleton._instance

    @property
    def strategies(self):
        return self._strategies

    @strategies.setter
    def strategies(self, strategy_name_map: dict):
        strategy_instance = strategy_name_map['strategy']
        name = strategy_name_map['name']
        self._strategies[name] = strategy_instance

    def get_strategy(self, strategy, name, recovery_timeout, failure_threshold_open, failure_threshold_close, fallback_function):
        if name in self.strategies:
            return self.strategies.get(name)

        instance = None
        # fetch remote config from remote for hot reloads

        if strategy == Strategy.Distributed:
            distributed_config = {"breaker_name": {"window": 60, "min_requests": 10, "read_delay": 1}}
            breaker_config = BreakerBaseStrategyConfig(
                name=name,
                recovery_timeout=recovery_timeout,
                error_threshold_open=failure_threshold_open,
                error_threshold_close=failure_threshold_close,
                min_requests=distributed_config.get("min_requests", DistributedPods.DEFAULT_MIN_REQUESTS),
                window=distributed_config.get("window", DistributedPods.DEFAULT_WINDOW)
            )
            instance = DistributedPods(breaker_config, fallback_function,
                                       read_delay=distributed_config.get("read_delay", DistributedPods.DEFAULT_WINDOW_READ_DELAY))
            self.strategies = {
                "strategy": instance,
                "name":  name
            }

        return instance

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
        for __strategy in self.get_strategies:
            if __strategy.opened:
                yield __strategy.name

    @property
    def get_closed(self):
        for __strategy in self.get_strategies:
            if __strategy.closed:
                yield __strategy.name


class BreakerService(object):
    DEFAULT_FAILURE_THRESHOLD = 0.5  # 50% failure rate
    DEFAULT_RECOVERY_TIMEOUT = 30
    DEFAULT_EXPECTED_EXCEPTION = Exception
    DEFAULT_FALLBACK_FUNCTION = None

    def __init__(self, failure_threshold_open=None,
                 failure_threshold_close=None,
                 recovery_timeout=None,
                 name=None,
                 fallback_function=None,
                 strategy: Strategy = None,
                 ):
        """
        Construct a circuit breaker.
        :param failure_threshold: break open after this many failures
        :param recovery_timeout: close after this many seconds
        :param name: name for this circuitbreaker
        :param fallback_function: called when the circuit is opened
           :return: Circuitbreaker instance
           :rtype: Circuitbreaker
        """
        self._failure_threshold_open = failure_threshold_open or self.DEFAULT_FAILURE_THRESHOLD
        self._failure_threshold_close = failure_threshold_close or self.DEFAULT_FAILURE_THRESHOLD
        self._recovery_timeout = recovery_timeout or self.DEFAULT_RECOVERY_TIMEOUT
        self._fallback_function = fallback_function or self.DEFAULT_FALLBACK_FUNCTION
        self._name = name
        self._strategy = strategy

    def get_strategy(self):
        return BreakerStrategiesSingleton.get_instance().get_strategy(self._strategy, self._name,
                                                                      self._recovery_timeout,
                                                                      self._failure_threshold_open,
                                                                      self._failure_threshold_close,
                                                                      self._fallback_function)

    def is_failure(self, exc_type, exc_value):
        return build_failure_predicate(self.get_strategy().included_errors or BreakerService.DEFAULT_EXPECTED_EXCEPTION)(exc_type, exc_value)

    def __call__(self, wrapped):
        return self.decorate(wrapped)

    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc_value, _traceback):
        if not self.get_strategy().feature_flag_enabled():
            return False

        if exc_type and self.is_failure(exc_type, exc_value):
            # exception was raised and is our concern
            self.__call_failed(exc_value)
        else:
            self.__call_succeeded()
        return False  # return False to raise exception if any

    def decorate(self, function):
        """
        Applies the circuit breaker to a function
        """

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

            opened = strategy.opened
            strategy.log(f"CURRENT STATE -- {strategy._state} | OPENED - {opened}")

            buffered_data = strategy.store.get_breaker(strategy.name) or {}
            if strategy.closed:
                if strategy._should_open(buffered_data.get(Store.SUCCESS), buffered_data.get(Store.FAILED), sync=False):
                    strategy._open_circuit()

            if strategy.opened:
                if strategy.fallback_function:
                    return strategy.fallback_function(*args, **kwargs)
                raise CircuitBreakerError(strategy)

            return call(function, *args, **kwargs)

        return wrapper

    def call(self, func, *args, **kwargs):
        """
        Calls the decorated function and applies the circuit breaker
        rules on success or failure
        :param func: Decorated function
        """
        with self:
            return func(*args, **kwargs)

    def call_generator(self, func, *args, **kwargs):
        """
        Calls the decorated generator function and applies the circuit breaker
        rules on success or failure
        :param func: Decorated generator function
        """
        with self:
            for el in func(*args, **kwargs):
                yield el

    def __call_succeeded(self):
        """
        Handle success in respective strategy
        Make sure to close the circuit if failure count or percentage has gone below threshold
        and reset failure count/percentage
        """
        self.get_strategy().handle_success()

    def __call_failed(self, exception):
        """
        Handle failure in respective strategy based on count failure/percentage open circuit if threshold has reached
        """
        self.get_strategy().handle_error(exception)

    def __str__(self, *args, **kwargs):
        return self.get_strategy().name


"""
SAMPLE USAGE 


@circuit(name="feed_api_call", failure_threshold=0.5, recovery_timeout=20)
def get_unified_feed(*args):
    pass

This means a breaker instance will be created, if not already exists, 
with a failure threshold of 50% and recovery timeout of 20 seconds after opening circuit
"""


def circuit(name,
            strategy: Strategy = Strategy.Distributed,
            failure_threshold=None,
            failure_threshold_close=None,
            recovery_timeout=None,
            fallback_function=None):
    # if the decorator is used without parameters, the
    # wrapped function is provided as first argument
    if callable(name):
        return BreakerService().decorate(name)
    else:
        if not name:
            raise Exception("invalid circuit breaker configuration. name is mandatory. "
                            "ex: @circuit(name='<service_name>')")

        breaker_instances = BreakerInstancesSingleton.get_instance()

        if name in breaker_instances.breakers:
            return breaker_instances.breakers[name]

        breaker_instance = BreakerService(
            failure_threshold_open=failure_threshold,
            failure_threshold_close=failure_threshold_close,
            recovery_timeout=recovery_timeout,
            name=name,
            fallback_function=fallback_function,
            strategy=strategy)
        breaker_instances.breakers = {"name": name, "instance": breaker_instance}
        return breaker_instance
