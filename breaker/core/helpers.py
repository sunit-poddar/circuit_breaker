from inspect import isclass


def in_exception_list(*exc_types):
    """Build a predicate function that checks if an exception is a subtype from a list"""

    def matches_types(thrown_type, exc_value):
        return issubclass(thrown_type, exc_types)

    return matches_types


def build_failure_predicate(expected_exception):
    """ Build a failure predicate_function.
          The returned function has the signature (Type[Exception], Exception) -> bool.
          Return value True indicates a failure in the underlying function.
        :param expected_exception: either an type of Exception, iterable of Exception types, or a predicate function.
          If an Exception type or iterable of Exception types, the failure predicate will return True when a thrown
          exception type matches one of the provided types.
          If a predicate function, it will just be returned as is.
         :return: callable (Type[Exception], Exception) -> bool
    """

    if isclass(expected_exception) and issubclass(expected_exception, Exception):
        failure_predicate = in_exception_list(expected_exception)
    else:
        try:
            # Check for an iterable of Exception types
            iter(expected_exception)

            # guard against a surprise later
            if isinstance(expected_exception, (bytes, str)):
                raise ValueError("expected_exception cannot be a string. Did you mean name?")
            failure_predicate = in_exception_list(*expected_exception)
        except TypeError:
            # not iterable. guess that it's a predicate function
            if not callable(expected_exception) or isclass(expected_exception):
                raise ValueError("expected_exception does not look like a predicate")
            failure_predicate = expected_exception
    return failure_predicate
