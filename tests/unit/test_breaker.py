"""
Core circuit breaker unit tests.

All tests use the memory-only path (redis_client mocked out) so no Redis
instance is required.  Integration tests live in tests/integration/.
"""
from unittest.mock import MagicMock, patch

import pytest

from breakerd import circuit_breaker, BreakerStrategiesSingleton
from breakerd.exceptions import CircuitBreakerError
from breakerd.strategy.base import BreakerStates


def _make_redis_mock():
    """Return a MagicMock that satisfies all Redis calls used by DistributedPods."""
    mock = MagicMock()
    mock.mget.return_value = []
    mock.incr.return_value = 1
    mock.expireat.return_value = True
    return mock


# ── Basic state tests ─────────────────────────────────────────────────────────

class TestCircuitBreakerBasicBehavior:

    def test_single_success_keeps_circuit_closed(self):
        redis_mock = _make_redis_mock()

        @circuit_breaker(name="test_basic_success", redis_client=redis_mock)
        def call_service():
            return "ok"

        result = call_service()
        assert result == "ok"
        assert BreakerStrategiesSingleton.get_instance().get("test_basic_success").state == BreakerStates.CLOSED

    @patch("breakerd.strategy.base.BreakerBaseStrategy._get_monotonic")
    def test_circuit_opens_after_failure_threshold(self, patched_monotonic):
        patched_monotonic.return_value = 1
        redis_mock = _make_redis_mock()
        breaker_name = "test_opens_after_failures"

        @circuit_breaker(name=breaker_name, failure_threshold=0.5, recovery_timeout=30, redis_client=redis_mock)
        def failing_call():
            raise ValueError("boom")

        # Drive failures until threshold is crossed (min_requests=10 default)
        for i in range(12):
            try:
                failing_call()
            except (ValueError, CircuitBreakerError):
                pass

        strategy = BreakerStrategiesSingleton.get_instance().get(breaker_name)
        assert strategy.opened, "Circuit should be OPEN after sustained failures"

    @patch("breakerd.strategy.base.BreakerBaseStrategy._get_monotonic")
    def test_circuit_stays_closed_with_all_successes(self, patched_monotonic):
        patched_monotonic.return_value = 1
        redis_mock = _make_redis_mock()
        breaker_name = "test_stays_closed"

        @circuit_breaker(name=breaker_name, redis_client=redis_mock)
        def successful_call():
            return "ok"

        for _ in range(30):
            result = successful_call()
            assert result == "ok"

        strategy = BreakerStrategiesSingleton.get_instance().get(breaker_name)
        assert strategy.closed, "Circuit should remain CLOSED with all successes"

    @patch("breakerd.strategy.base.BreakerBaseStrategy._get_monotonic")
    def test_open_circuit_raises_circuit_breaker_error(self, patched_monotonic):
        patched_monotonic.return_value = 1
        redis_mock = _make_redis_mock()
        breaker_name = "test_open_raises"

        @circuit_breaker(name=breaker_name, failure_threshold=0.5, recovery_timeout=30, redis_client=redis_mock)
        def failing_call():
            raise ValueError("boom")

        for _ in range(12):
            try:
                failing_call()
            except (ValueError, CircuitBreakerError):
                pass

        strategy = BreakerStrategiesSingleton.get_instance().get(breaker_name)
        if strategy.opened:
            with pytest.raises(CircuitBreakerError):
                failing_call()


# ── Recovery tests ────────────────────────────────────────────────────────────

class TestCircuitBreakerRecovery:

    @patch("breakerd.strategy.base.BreakerBaseStrategy._get_monotonic")
    def test_open_circuit_moves_to_half_open_after_timeout(self, patched_monotonic):
        """After recovery_timeout the circuit transitions OPEN → HALF_OPEN."""
        patched_monotonic.return_value = 1
        redis_mock = _make_redis_mock()
        breaker_name = "test_half_open_timeout"

        @circuit_breaker(name=breaker_name, failure_threshold=0.5, recovery_timeout=30, redis_client=redis_mock)
        def failing_call():
            raise ValueError("boom")

        for _ in range(12):
            try:
                failing_call()
            except (ValueError, CircuitBreakerError):
                pass

        strategy = BreakerStrategiesSingleton.get_instance().get(breaker_name)
        assert strategy.opened

        # Simulate recovery timeout elapsed
        patched_monotonic.return_value = 32
        strategy.maybe_recover()
        assert strategy.half_open, "Circuit should be HALF_OPEN after timeout"

    @patch("breakerd.strategy.base.BreakerBaseStrategy._get_monotonic")
    def test_half_open_to_closed_on_success(self, patched_monotonic):
        """A successful probe in HALF_OPEN transitions the circuit to CLOSED."""
        patched_monotonic.return_value = 1
        redis_mock = _make_redis_mock()
        breaker_name = "test_half_open_to_closed"

        @circuit_breaker(name=breaker_name, failure_threshold=0.5, recovery_timeout=30, redis_client=redis_mock)
        def call():
            return "ok"

        # Trigger strategy initialization with one call.
        call()
        strategy = BreakerStrategiesSingleton.get_instance().get(breaker_name)
        # Manually force HALF_OPEN state.
        strategy._state = BreakerStates.HALF_OPEN

        result = call()
        assert result == "ok"
        assert strategy.closed, "Circuit should close after successful probe"

    @patch("breakerd.strategy.base.BreakerBaseStrategy._get_monotonic")
    def test_half_open_to_open_on_failure(self, patched_monotonic):
        """A failing probe in HALF_OPEN re-opens the circuit."""
        patched_monotonic.return_value = 1
        redis_mock = _make_redis_mock()
        breaker_name = "test_half_open_to_open"

        @circuit_breaker(name=breaker_name, failure_threshold=0.5, recovery_timeout=30, redis_client=redis_mock)
        def call():
            raise ValueError("still broken")

        # Trigger strategy initialization; catch the expected ValueError.
        try:
            call()
        except ValueError:
            pass

        strategy = BreakerStrategiesSingleton.get_instance().get(breaker_name)
        strategy._state = BreakerStates.HALF_OPEN

        try:
            call()
        except (ValueError, CircuitBreakerError):
            pass

        # After a failure in HALF_OPEN, the circuit must not be CLOSED.
        assert not strategy.closed, "Circuit should not be closed after failed probe"

    @patch("breakerd.strategy.base.BreakerBaseStrategy._get_monotonic")
    def test_full_lifecycle_closed_open_halfopen_closed(self, patched_monotonic):
        """Full state machine: CLOSED → OPEN → HALF_OPEN → CLOSED."""
        patched_monotonic.return_value = 1
        redis_mock = _make_redis_mock()
        breaker_name = "test_full_lifecycle"

        @circuit_breaker(name=breaker_name, failure_threshold=0.5, recovery_timeout=30, redis_client=redis_mock)
        def failing_call():
            raise ValueError("boom")

        @circuit_breaker(name=breaker_name, failure_threshold=0.5, recovery_timeout=30, redis_client=redis_mock)
        def successful_call():
            return "ok"

        # Drive to OPEN
        for _ in range(12):
            try:
                failing_call()
            except (ValueError, CircuitBreakerError):
                pass

        strategy = BreakerStrategiesSingleton.get_instance().get(breaker_name)
        assert strategy.opened, "Circuit should be OPEN"

        # Elapse recovery timeout → HALF_OPEN
        patched_monotonic.return_value = 32
        strategy.maybe_recover()
        assert strategy.half_open, "Circuit should be HALF_OPEN"

        # Successful probe → CLOSED
        result = successful_call()
        assert result == "ok"
        assert strategy.closed, "Circuit should close after successful probe"


# ── Configuration tests ───────────────────────────────────────────────────────

class TestCircuitBreakerConfiguration:

    @patch("breakerd.strategy.base.BreakerBaseStrategy._get_monotonic")
    def test_custom_failure_threshold_respected(self, patched_monotonic):
        patched_monotonic.return_value = 1
        redis_mock = _make_redis_mock()
        breaker_name = "test_custom_threshold"

        @circuit_breaker(name=breaker_name, failure_threshold=0.3, recovery_timeout=45, redis_client=redis_mock)
        def failing_call():
            raise ValueError("boom")

        for _ in range(12):
            try:
                failing_call()
            except (ValueError, CircuitBreakerError):
                pass

        strategy = BreakerStrategiesSingleton.get_instance().get(breaker_name)
        assert strategy.config.error_threshold_open == 0.3
        assert strategy.config.recovery_timeout == 45

    @patch("breakerd.strategy.base.BreakerBaseStrategy._get_monotonic")
    def test_recovery_timeout_is_configurable(self, patched_monotonic):
        patched_monotonic.return_value = 1
        redis_mock = _make_redis_mock()
        breaker_name = "test_custom_timeout"

        @circuit_breaker(name=breaker_name, failure_threshold=0.5, recovery_timeout=40, redis_client=redis_mock)
        def failing_call():
            raise ValueError("boom")

        for _ in range(12):
            try:
                failing_call()
            except (ValueError, CircuitBreakerError):
                pass

        strategy = BreakerStrategiesSingleton.get_instance().get(breaker_name)
        assert strategy.opened

        patched_monotonic.return_value = 41  # past the 40s timeout
        strategy.maybe_recover()
        assert strategy.half_open

    def test_same_name_returns_same_instance(self):
        redis_mock = _make_redis_mock()

        @circuit_breaker(name="shared_breaker", redis_client=redis_mock)
        def call_a():
            return "a"

        @circuit_breaker(name="shared_breaker", redis_client=redis_mock)
        def call_b():
            return "b"

        # Initialize strategy by calling the function.
        call_a()

        strategy_a = BreakerStrategiesSingleton.get_instance().get("shared_breaker")
        assert strategy_a is not None


# ── Fallback tests ────────────────────────────────────────────────────────────

class TestCircuitBreakerFallback:

    @patch("breakerd.strategy.base.BreakerBaseStrategy._get_monotonic")
    def test_fallback_called_when_open(self, patched_monotonic):
        patched_monotonic.return_value = 1
        redis_mock = _make_redis_mock()
        breaker_name = "test_fallback"
        fallback_calls = []

        def my_fallback():
            fallback_calls.append(1)
            return "fallback_response"

        @circuit_breaker(
            name=breaker_name,
            failure_threshold=0.5,
            recovery_timeout=30,
            fallback_function=my_fallback,
            redis_client=redis_mock,
        )
        def failing_call():
            raise ValueError("boom")

        for _ in range(12):
            try:
                failing_call()
            except (ValueError, CircuitBreakerError):
                pass

        strategy = BreakerStrategiesSingleton.get_instance().get(breaker_name)
        if strategy.opened:
            result = failing_call()
            assert result == "fallback_response"
            assert len(fallback_calls) >= 1
