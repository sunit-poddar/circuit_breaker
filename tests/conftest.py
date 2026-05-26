"""
Shared fixtures for the breakerd test suite.

Key design decision: every test gets a fresh singleton state so tests are
fully isolated — no shared breaker instances across test functions.
"""
import pytest

from breakerd.decorator import BreakerInstancesSingleton, BreakerStrategiesSingleton
from breakerd.store import CircuitStoreSingleton


@pytest.fixture(autouse=True)
def reset_singletons():
    """Reset all singletons before each test for full isolation."""
    # Clear BreakerInstancesSingleton
    BreakerInstancesSingleton._instance = None

    # Clear BreakerStrategiesSingleton
    BreakerStrategiesSingleton._instance = None

    # Clear CircuitStoreSingleton
    store = CircuitStoreSingleton()
    # pylint: disable=protected-access
    store._CircuitStoreSingleton__circuits.clear()

    yield

    # Teardown — same cleanup
    BreakerInstancesSingleton._instance = None
    BreakerStrategiesSingleton._instance = None
    store._CircuitStoreSingleton__circuits.clear()
