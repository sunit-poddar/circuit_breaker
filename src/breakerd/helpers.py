from inspect import isclass


def in_exception_list(*exc_types):
    """Build a predicate that checks whether an exception is a subtype of any listed type."""

    def matches_types(thrown_type, exc_value):
        return issubclass(thrown_type, exc_types)

    return matches_types


def build_failure_predicate(expected_exception):
    """
    Build a failure predicate with signature (Type[Exception], Exception) -> bool.

    Accepts:
    - A single Exception subclass
    - An iterable of Exception subclasses
    - A callable predicate (passed through as-is)
    """
    if isclass(expected_exception) and issubclass(expected_exception, Exception):
        return in_exception_list(expected_exception)

    if isinstance(expected_exception, (bytes, str)):
        raise ValueError("expected_exception cannot be a string. Did you mean name?")

    try:
        iter(expected_exception)
        return in_exception_list(*expected_exception)
    except TypeError:
        pass

    if not callable(expected_exception) or isclass(expected_exception):
        raise ValueError("expected_exception does not look like a predicate")

    return expected_exception
