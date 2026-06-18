"""
Thread-safety tests for CircuitStoreSingleton.

These tests verify that concurrent counter increments do not lose updates —
the core correctness invariant for any multi-threaded production service.
"""
import threading

import pytest

from breakerd.store import CircuitStoreSingleton, Store


class TestConcurrentCounterSafety:

    def test_concurrent_success_increments_no_loss(self):
        """100 threads × 100 increments = exactly 10,000 success records."""
        store = CircuitStoreSingleton()
        threads = 100
        increments_per_thread = 100
        barrier = threading.Barrier(threads)

        def worker():
            barrier.wait()  # all threads start simultaneously
            for _ in range(increments_per_thread):
                store.record_success("concurrent_success")

        t_list = [threading.Thread(target=worker) for _ in range(threads)]
        for t in t_list:
            t.start()
        for t in t_list:
            t.join()

        data = store.get_breaker("concurrent_success")
        assert data[Store.SUCCESS] == threads * increments_per_thread

    def test_concurrent_failure_increments_no_loss(self):
        """100 threads × 100 increments = exactly 10,000 failure records."""
        store = CircuitStoreSingleton()
        threads = 100
        increments_per_thread = 100
        barrier = threading.Barrier(threads)

        def worker():
            barrier.wait()
            for _ in range(increments_per_thread):
                store.record_failure("concurrent_failure")

        t_list = [threading.Thread(target=worker) for _ in range(threads)]
        for t in t_list:
            t.start()
        for t in t_list:
            t.join()

        data = store.get_breaker("concurrent_failure")
        assert data[Store.FAILED] == threads * increments_per_thread

    def test_mixed_concurrent_operations_no_loss(self):
        """50 success threads + 50 failure threads, totals must be exact."""
        store = CircuitStoreSingleton()
        n = 50
        increments = 100
        barrier = threading.Barrier(n * 2)

        def success_worker():
            barrier.wait()
            for _ in range(increments):
                store.record_success("mixed")

        def failure_worker():
            barrier.wait()
            for _ in range(increments):
                store.record_failure("mixed")

        threads = (
            [threading.Thread(target=success_worker) for _ in range(n)]
            + [threading.Thread(target=failure_worker) for _ in range(n)]
        )
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        data = store.get_breaker("mixed")
        assert data[Store.SUCCESS] == n * increments
        assert data[Store.FAILED] == n * increments
        assert data[Store.TOTAL] == n * increments * 2
