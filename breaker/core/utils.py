import threading


def ticker(interval, *, daemonize=True, name="", debug=True):
    """
    Runs the given function every 'interval' seconds
    interval - float: frequency in seconds
    daemonize - bool: True flag sets the thread in daemon mode. Daemon threads are abruptly stopped at shutdown.
                    Their resources (such as open files, database transactions, etc.) may not be released properly
    name - str: thread name

    EXAMPLE USAGE
    Prints 'hi' every 2 seconds

    @ticker(2)
    def say(message):
        print(message)

    thread_event = say("hi")
    thread_event.set() -> stop execution
    """

    def decorator(function):
        def wrapper(*args, **kwargs):
            stopped = threading.Event()

            def loop():
                # executed in another thread
                while not stopped.wait(interval):  # until stopped
                    function(*args, **kwargs)

            t = threading.Thread(target=loop)
            t.daemon = daemonize  # stop if the program exits
            if name:
                t.name = name
            t.start()

            return stopped
        return wrapper
    return decorator


def tick_repeatedly(interval, func, *args):
    stopped = threading.Event()

    def loop():
        while not stopped.wait(interval):  # the first call is in `interval` secs
            func(*args)

    threading.Thread(target=loop).start()
    return stopped.set


class CircuitBreakerError(Exception):
    def __init__(self, circuit_breaker, *args, **kwargs):
        """
        :param circuit_breaker:
        :param args:
        :param kwargs:
        :return:
        """
        super(CircuitBreakerError, self).__init__(*args, **kwargs)
        self._circuit_breaker = circuit_breaker

    def __str__(self, *args, **kwargs):
        return 'Circuit "%s" OPEN (%d failures, %d sec remaining) (last_failure: %r)' % (
            self._circuit_breaker.name,
            self._circuit_breaker.failure_count,
            round(self._circuit_breaker.seconds_remaining_until_circuit_is_open),
            self._circuit_breaker.last_failure,
        )
