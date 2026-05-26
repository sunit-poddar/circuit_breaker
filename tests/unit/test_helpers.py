"""Tests for the exception predicate builder."""
import pytest

from breakerd.helpers import build_failure_predicate, in_exception_list


class TestInExceptionList:

    def test_matches_exact_type(self):
        pred = in_exception_list(ValueError)
        assert pred(ValueError, ValueError("x"))

    def test_matches_subclass(self):
        pred = in_exception_list(Exception)
        assert pred(ValueError, ValueError("x"))

    def test_does_not_match_unrelated(self):
        pred = in_exception_list(ValueError)
        assert not pred(TypeError, TypeError("x"))


class TestBuildFailurePredicate:

    def test_single_exception_class(self):
        pred = build_failure_predicate(ValueError)
        assert pred(ValueError, ValueError("x"))
        assert not pred(TypeError, TypeError("x"))

    def test_iterable_of_exception_classes(self):
        pred = build_failure_predicate([ValueError, KeyError])
        assert pred(ValueError, ValueError("x"))
        assert pred(KeyError, KeyError("x"))
        assert not pred(TypeError, TypeError("x"))

    def test_callable_predicate_passthrough(self):
        custom = lambda t, v: t is RuntimeError
        pred = build_failure_predicate(custom)
        assert pred(RuntimeError, RuntimeError("x"))
        assert not pred(ValueError, ValueError("x"))

    def test_raises_on_string_input(self):
        with pytest.raises(ValueError, match="cannot be a string"):
            build_failure_predicate("ValueError")

    def test_raises_on_non_callable(self):
        with pytest.raises(ValueError, match="does not look like a predicate"):
            build_failure_predicate(42)
