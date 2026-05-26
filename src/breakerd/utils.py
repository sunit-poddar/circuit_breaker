import logging
import threading
import traceback

logger = logging.getLogger(__name__)


def ticker(interval, *, daemonize=True, name=""):
    """
    Decorator that runs the wrapped function every `interval` seconds in a daemon thread.

    Returns a threading.Event; call .set() to stop the loop.

    Example:
        @ticker(2)
        def heartbeat():
            ping_redis()

        stop = heartbeat()
        stop.set()  # stop the loop
    """

    def decorator(function):
        def wrapper(*args, **kwargs):
            stopped = threading.Event()

            def loop():
                while not stopped.wait(interval):
                    try:
                        function(*args, **kwargs)
                    except Exception:
                        logger.error(
                            "Unhandled exception in ticker thread '%s':\n%s",
                            name or function.__name__,
                            traceback.format_exc(),
                        )

            t = threading.Thread(target=loop, daemon=daemonize)
            if name:
                t.name = name
            t.start()
            return stopped

        return wrapper

    return decorator


def tick_repeatedly(interval, func, *args):
    """Run `func(*args)` every `interval` seconds. Returns the stop Event."""
    stopped = threading.Event()

    def loop():
        while not stopped.wait(interval):
            try:
                func(*args)
            except Exception:
                logger.error(
                    "Unhandled exception in tick_repeatedly thread:\n%s",
                    traceback.format_exc(),
                )

    threading.Thread(target=loop, daemon=True).start()
    return stopped
