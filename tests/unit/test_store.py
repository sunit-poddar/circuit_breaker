"""Tests for the in-memory CircuitStoreSingleton."""
from breakerd.store import CircuitStoreSingleton, Store


class TestCircuitStoreSingleton:

    def test_singleton_returns_same_instance(self):
        a = CircuitStoreSingleton()
        b = CircuitStoreSingleton()
        assert a is b

    def test_record_success_increments_counters(self):
        store = CircuitStoreSingleton()
        store.record_success("svc")
        store.record_success("svc")
        data = store.get_breaker("svc")
        assert data[Store.SUCCESS] == 2
        assert data[Store.TOTAL] == 2
        assert data[Store.FAILED] == 0

    def test_record_failure_increments_counters(self):
        store = CircuitStoreSingleton()
        store.record_failure("svc", increment=3)
        data = store.get_breaker("svc")
        assert data[Store.FAILED] == 3
        assert data[Store.TOTAL] == 3
        assert data[Store.SUCCESS] == 0

    def test_record_initializes_absent_breaker(self):
        store = CircuitStoreSingleton()
        data = store.record_success("new_svc")
        assert data[Store.SUCCESS] == 1

    def test_reset_breaker_clears_counters(self):
        store = CircuitStoreSingleton()
        store.record_failure("svc", increment=5)
        store.reset_breaker("svc")
        data = store.get_breaker("svc")
        assert data[Store.FAILED] == 0
        assert data[Store.SUCCESS] == 0
        assert data[Store.TOTAL] == 0

    def test_update_past_window(self):
        store = CircuitStoreSingleton()
        store.get_breaker("svc")  # initialize
        store.update_past_window("svc", {"end": "2024-01-01T00:00:00", "window": {}})
        pw = store.get_past_window("svc")
        assert pw["end"] == "2024-01-01T00:00:00"

    def test_multiple_breakers_are_independent(self):
        store = CircuitStoreSingleton()
        store.record_failure("svc_a", increment=5)
        store.record_success("svc_b", increment=3)
        assert store.get_breaker("svc_a")[Store.FAILED] == 5
        assert store.get_breaker("svc_b")[Store.SUCCESS] == 3
        assert store.get_breaker("svc_a")[Store.SUCCESS] == 0
