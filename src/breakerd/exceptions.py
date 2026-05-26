class CircuitBreakerError(Exception):
    def __init__(self, circuit_breaker, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._circuit_breaker = circuit_breaker

    def __str__(self, *args, **kwargs):
        return 'Circuit "%s" OPEN (%d failures, %d sec remaining) (last_failure: %r)' % (
            self._circuit_breaker.name,
            self._circuit_breaker.failure_count,
            round(self._circuit_breaker.seconds_remaining_until_circuit_is_open),
            self._circuit_breaker.last_failure,
        )
