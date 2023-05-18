from datetime import datetime


class Store:
    # constants
    MEMBER_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"

    # store enum keys
    SUCCESS = "success"
    FAILED = "failed"
    TOTAL = "total"
    WINDOW_START = "window_start"
    PAST_WINDOW = "past_window"
    PAST_WINDOW_END = "end"
    WINDOW_KEY = "window"


class CircuitStoreSingleton:
    """
    Circuit Local Database Class: Singleton
    This class holds temporary values for a circuit breaker with its total and failed counts until it is
    synced with redis
    """
    __instance = None

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super(CircuitStoreSingleton, cls).__new__(cls)
        return cls.__instance

    def __init__(self):
        self.__circuits = {}

    @property
    def circuits(self):
        return self.__circuits

    @circuits.setter
    def circuits(self, breaker_name):
        """
            store.circuits = "feed_api_call"

            "feed_api_call": {
                "total": 0,
                "success": 0,
                "failed": 0,
                "window_start": datetime.now().strftime(Store.MEMBER_TIMESTAMP_FORMAT),
                "past_window": {
                    "end": "",
                    "window": {
                        "<ts>": {
                            "success": ,
                            "failure":
                        }
                    }
                }
            }
        """
        if breaker_name not in self.__circuits:
            self.__circuits[breaker_name] = {
                Store.TOTAL: 0,
                Store.SUCCESS: 0,
                Store.FAILED: 0,
                Store.WINDOW_START: datetime.now().strftime(Store.MEMBER_TIMESTAMP_FORMAT),
                Store.PAST_WINDOW: {}
            }

    def get_past_window(self, breaker_name):
        return self.__circuits.get(breaker_name, {}).get(Store.PAST_WINDOW)

    def update_past_window(self, breaker_name, past_window):
        if breaker_name in self.__circuits:
            self.__circuits[breaker_name][Store.PAST_WINDOW] = {**self.__circuits[breaker_name][Store.PAST_WINDOW],
                                                                **past_window}

    def get_breaker(self, breaker_name):
        breaker = self.__circuits.get(breaker_name)
        if not breaker:
            self.circuits = breaker_name
        return self.__circuits.get(breaker_name, None)

    def record_success(self, breaker_name, increment=1):
        if breaker_name in self.__circuits:
            self.__circuits[breaker_name][Store.SUCCESS] += increment
            self.__circuits[breaker_name][Store.TOTAL] += increment
        else:
            self.circuits = breaker_name
            self.record_success(breaker_name)
        return self.__circuits[breaker_name]

    def record_failure(self, breaker_name, increment=1):
        if breaker_name in self.__circuits:
            self.__circuits[breaker_name][Store.FAILED] += increment
            self.__circuits[breaker_name][Store.TOTAL] += increment
        else:
            self.circuits = breaker_name
            self.record_failure(breaker_name)

        return self.__circuits[breaker_name]

    def reset_breaker(self, breaker_name):
        # remove breaker from circuit store if exists
        self.__circuits.pop(breaker_name, None)

        # create a fresh circuit breaker store instance
        self.circuits = breaker_name

