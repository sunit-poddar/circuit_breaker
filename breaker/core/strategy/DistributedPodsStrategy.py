from datetime import datetime, timedelta
from dateutil.rrule import rrule, SECONDLY
from django_redis import get_redis_connection

from community.personalisation.configuration import PersonalisationConfig
from core.strategy.core import BreakerBaseStrategy, BreakerStates, BreakerBaseStrategyConfig
from core.store import Store


class DistributedPods(BreakerBaseStrategy):
    DEFAULT_MIN_REQUESTS = 30
    DEFAULT_WINDOW_READ_DELAY = 1  # in seconds
    DEFAULT_WINDOW = 60  # in seconds

    def __init__(self, config: BreakerBaseStrategyConfig, fallback_function, **kwargs):
        super().__init__(config, fallback_function, **kwargs)

        self._strategy = "DistributedPods"
        # custom class variables
        self._read_delay = kwargs.get("read_delay") or self.DEFAULT_WINDOW_READ_DELAY
        self._redis = get_redis_connection("default")
        self._redis_key_success = f"{self.name}-success"
        self._redis_key_failure = f"{self.name}-failure"
        self.log(f"Initializing breaker with config {config}")

    def __filter_redis_data_in_threshold(self, redis_data):
        """
        redis_data:
        list of (member, score) tuples
        [(b'22-13-35T20:35:06', 3.0), (b'22-13-35T20:35:05', 11.0)]
        """
        filtered = {}  # {timestamp: score}
        for data in redis_data:
            member = data[0]
            score = data[1]
            try:
                member_timestamp = datetime.strptime(member, Store.MEMBER_TIMESTAMP_FORMAT)
            except ValueError:
                # invalid member timestamp
                self.log(f"Invalid timestamp stored", extra={"timestamp": member, "breaker": self.name})
                continue

            if (datetime.now() - member_timestamp).total_seconds() <= self.config.window:
                filtered[member_timestamp] = score
        return filtered

    def __format_success_cache_key(self, dt):
        if isinstance(dt, datetime):
            dt = dt.strftime(Store.MEMBER_TIMESTAMP_FORMAT)
        return f"breaker:{self.name}:success:-{dt}"

    def __format_failure_cache_key(self, dt):
        if isinstance(dt, datetime):
            dt = dt.strftime(Store.MEMBER_TIMESTAMP_FORMAT)
        return f"breaker:{self.name}:failure:-{dt}"

    def __fetch_past_window_from_store(self, sync=True):
        self.log(f"fetching past window from store - {self.name}")
        past_window = self.store.get_past_window(self.name)
        is_past_window_updated = False
        dt_now = None

        def fetch_window_data_from_redis(instance, past_window_end):
            dt_now = datetime.now()
            self.log(f"fetching past window from redis - {instance.name} | past window end - {past_window_end}")

            previous_window_end = datetime.now()
            if not past_window_end:
                previous_window_start = previous_window_end - timedelta(seconds=self.config.window + self._read_delay)
            else:
                previous_window_start = past_window_end - timedelta(seconds=self._read_delay)

            success_keys, failure_keys = [], []
            formatted_times = []
            for dt in rrule(SECONDLY, dtstart=previous_window_start, until=previous_window_end):
                formatted_time = dt.strftime(Store.MEMBER_TIMESTAMP_FORMAT)
                formatted_times.append(formatted_time)
                success_keys.append(instance.__format_success_cache_key(formatted_time))
                failure_keys.append(instance.__format_failure_cache_key(formatted_time))

            success_redis_data = [int(val.decode()) if val else 0 for val in self._redis.mget(success_keys)]
            failure_redis_data = [int(val.decode()) if val else 0 for val in self._redis.mget(failure_keys)]

            success_map = dict(zip(success_keys, success_redis_data))
            failure_map = dict(zip(failure_keys, failure_redis_data))

            window = {}
            for formatted_time in formatted_times:
                window[formatted_time] = {
                    Store.SUCCESS: success_map.get(instance.__format_success_cache_key(formatted_time), 0),
                    Store.FAILED: failure_map.get(instance.__format_failure_cache_key(formatted_time), 0)
                }

            return {
                Store.PAST_WINDOW_END: dt_now.strftime(Store.MEMBER_TIMESTAMP_FORMAT),
                Store.WINDOW_KEY: window
            }, dt_now

        if not past_window:
            # fetch from redis and save
            self.log(f"fetching past window from store - past window not available in store")
            past_window, dt_now = fetch_window_data_from_redis(self, None)
            is_past_window_updated = True

        elif past_window.get(Store.PAST_WINDOW_END):
            past_window_end = datetime.strptime(past_window.get(Store.PAST_WINDOW_END), Store.MEMBER_TIMESTAMP_FORMAT)
            if (datetime.now() - past_window_end).total_seconds() > self._read_delay:
                self.log(f"fetching past window from store - past window expired in store")
                # fetch from redis and save
                past_window, dt_now = fetch_window_data_from_redis(self, past_window_end)
                is_past_window_updated = True

        if is_past_window_updated:
            past_window[Store.WINDOW_KEY][dt_now.strftime(Store.MEMBER_TIMESTAMP_FORMAT)] = {
                Store.SUCCESS: self.store.get_breaker(self.name).get(Store.SUCCESS),
                Store.FAILED: self.store.get_breaker(self.name).get(Store.FAILED)
            }

            if sync:
                self.sync(dt_now)
                self.store.update_past_window(self.name, past_window)

        return past_window, is_past_window_updated

    def _should_open(self, buffered_success, buffered_failure, sync=True):
        past_window, is_past_window_updated = self.__fetch_past_window_from_store(sync)
        if is_past_window_updated:
            buffered_data = self.store.get_breaker(self.name)
            buffered_success, buffered_failure = buffered_data.get(Store.SUCCESS), buffered_data.get(Store.FAILED)

        self.log(f"_should_open : deciding whether to open the circuit - past_window - {past_window} | "
                 f"buffered_success - {buffered_success} | buffered_failure - {buffered_failure}")
        past_failures = 0
        past_successes = 0
        for _, window_details in past_window.get(Store.WINDOW_KEY).items():
            past_successes += window_details.get(Store.SUCCESS, 0) or 0
            past_failures += window_details.get(Store.FAILED, 0) or 0

        total_failures = past_failures + buffered_failure
        total_events = total_failures + past_successes + buffered_success

        self.log(f"_should_open : total_failures - {total_failures} | "
                 f"total_events - {total_events} | error threshold - {self.config.error_threshold_open}")

        if total_events >= self.config.min_requests and total_failures / total_events >= self.config.error_threshold_open:
            self.log(f"_should_open : circuit open conditions met ")
            return True

        return False

    def _should_close(self, buffered_success, buffered_failure):
        past_window, is_past_window_updated = self.__fetch_past_window_from_store()
        if is_past_window_updated:
            buffered_data = self.store.get_breaker(self.name)
            buffered_success, buffered_failure = buffered_data.get(Store.SUCCESS), buffered_data.get(Store.FAILED)

        self.log(f"_should_close : deciding whether to close the circuit - past_window - {past_window} | "
                 f"buffered_success - {buffered_success} | buffered_failure - {buffered_failure}")
        past_failures = 0
        past_successes = 0
        for _, window_details in past_window.get(Store.WINDOW_KEY).items():
            past_successes += window_details.get(Store.SUCCESS, 0) or 0
            past_failures += window_details.get(Store.FAILED, 0) or 0

        total_failures = past_failures + buffered_failure
        total_events = total_failures + past_successes + buffered_success

        self.log(f"_should_close : total_failures - {total_failures} | total_events - {total_events} | "
                 f"error threshold - {self.config.error_threshold_close}")

        if total_events and total_failures / total_events <= self.config.error_threshold_close:
            self.log(f"_should_close : circuit close conditions met")
            return True

        return False

    def handle_error(self, exception) -> BreakerStates:
        self._last_failure = exception
        self._failure_count += 1
        buffered_data = self.store.record_failure(self.name)

        if self._should_open(buffered_data.get(Store.SUCCESS), buffered_data.get(Store.FAILED)):
            self._open_circuit()

        return self._state

    def handle_success(self) -> BreakerStates:
        buffered_data = self.store.record_success(self.name)

        if self._state == BreakerStates.OPEN:
            if self._should_close(buffered_data.get(Store.SUCCESS), buffered_data.get(Store.FAILED)):
                self._close_circuit()

        return self._state

    def sync(self, sync_dt):
        self.log(f"syncing buffer to redis - {self.name}")
        breaker_buffer = self.store.get_breaker(self.name)
        success_count = breaker_buffer.get(Store.SUCCESS, 0)
        failure_count = breaker_buffer.get(Store.FAILED, 0)

        dt = sync_dt.strftime(Store.MEMBER_TIMESTAMP_FORMAT)
        success_key = self.__format_success_cache_key(dt)
        failure_key = self.__format_failure_cache_key(dt)

        if success_count:
            self._redis.incr(success_key, success_count)
            # TTL of twice window size
            self._redis.expireat(success_key, sync_dt + timedelta(seconds=self.config.window * 2))

        if failure_count:
            self._redis.incr(failure_key, failure_count)
            # TTL of twice window size
            self._redis.expireat(failure_key, sync_dt + timedelta(seconds=self.config.window * 2))

        self.store.reset_breaker(self.name)

    def feature_flag_enabled(self) -> bool:
        # fetch from remote in production
        return True

    @property
    def included_errors(self) -> list:
        return [
            Exception
        ]

    @property
    def excluded_errors(self) -> list:
        return []
