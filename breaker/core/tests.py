from django.core.cache import cache
from django.test import TestCase
from unittest.mock import patch

from rest_framework.exceptions import APIException
from rest_framework.response import Response

from core.breaker import circuit, BreakerStrategiesSingleton
from core.strategy.core import BreakerStates
from core.utils import CircuitBreakerError


class BreakerTest(TestCase):
    def setUp(self):
        cache.clear()

    @patch("core.circuit_breaker.strategy.core.BreakerBaseStrategy._get_monotonic")
    @patch("utils.remote_config_download.LocalRemoteConfig.get_breaker_config_for")
    @patch("community.personalisation.configuration.PersonalisationConfig.get_breaker_config_flags")
    def test_strategy_flag_turned_off(self, breaker_config_flags, strategy_config, patched_monotonic):
        breaker_name = "test_circuit_close_single_call_with_flag_turned_off"
        breaker_config_flags.return_value = {breaker_name: False}
        strategy_config.return_value = {"min_requests": 10, "window": 60}
        patched_monotonic.return_value = 1

        @circuit(breaker_name)
        def test_circuit():
            return Response(status=200)

        @circuit(breaker_name)
        def test_circuit_open():
            raise APIException(f"testing circuit open for {breaker_name}")

        response = test_circuit()
        self.assertEqual(response.status_code, 200)

        strategy_config_from_monitor = BreakerStrategiesSingleton.get_instance().get(breaker_name).config
        self.assertEqual(strategy_config_from_monitor.name, breaker_name)
        self.assertEqual(strategy_config_from_monitor.min_requests, 10)
        self.assertEqual(strategy_config_from_monitor.window, 60)

        for i in range(15):
            try:
                test_circuit_open()
            except Exception as e:  # noqa
                self.assertTrue(type(e), APIException)
                self.assertNotEqual(type(e), CircuitBreakerError)

    @patch("community.personalisation.configuration.PersonalisationConfig.get_breaker_config_flags")
    def test_strategy_flag_turned_off_circuit_close_single_call_returning_200(self, breaker_config_flags):
        breaker_name = "strategy_flag_turned_off_circuit_close_single_call_returning_200"
        breaker_config_flags.return_value = {breaker_name: True}

        @circuit(breaker_name)
        def test_circuit():
            return Response(status=200)

        response = test_circuit()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(BreakerStrategiesSingleton.get_instance().get(breaker_name).state, BreakerStates.CLOSED)

    @patch("community.personalisation.configuration.PersonalisationConfig.get_breaker_config_flags")
    def test_strategy_flag_turned_off_circuit_close_single_call_returning_500(self, breaker_config_flags):
        breaker_name = "strategy_flag_turned_off_circuit_close_single_call_returning_500"
        breaker_config_flags.return_value = {breaker_name: True}

        @circuit(breaker_name)
        def test_circuit():
            return Response(status=500)

        response = test_circuit()
        self.assertEqual(response.status_code, 500)
        self.assertEqual(BreakerStrategiesSingleton.get_instance().get(breaker_name).state, BreakerStates.CLOSED)

    @patch("community.personalisation.configuration.PersonalisationConfig.get_breaker_config_flags")
    def test_strategy_flag_turned_on_circuit_close_multiple_calls_200(self, breaker_config_flags):
        breaker_name = "strategy_flag_turned_on_circuit_close_multiple_calls_200"
        breaker_config_flags.return_value = {breaker_name: True}

        @circuit(breaker_name)
        def test_circuit():
            return Response(status=200)

        for i in range(30):
            response = test_circuit()
            self.assertEqual(response.status_code, 200)
            self.assertEqual(BreakerStrategiesSingleton.get_instance().get(breaker_name).state, BreakerStates.CLOSED)

    @patch("utils.remote_config_download.LocalRemoteConfig.get_breaker_config_for")
    @patch("community.personalisation.configuration.PersonalisationConfig.get_breaker_config_flags")
    def test_strategy_flag_turned_on_circuit_close_multiple_calls_500(self, breaker_config_flags, strategy_config):
        breaker_name = "strategy_flag_turned_on_circuit_close_multiple_calls_500"

        breaker_config_flags.return_value = {breaker_name: True}
        strategy_config.return_value = {"min_requests": 10, "window": 60}

        @circuit(breaker_name)
        def test_circuit():
            raise APIException("returning 500")

        for i in range(15):
            try:
                response = test_circuit()
                self.assertEqual(response.status_code, 500)
            except Exception:  # noqa
                if i < 9:
                    self.assertTrue(BreakerStrategiesSingleton.get_instance().get(breaker_name).closed)
                else:
                    self.assertTrue(BreakerStrategiesSingleton.get_instance().get(breaker_name).opened)

        strategy_config_from_monitor = BreakerStrategiesSingleton.get_instance().get(breaker_name).config
        self.assertEqual(strategy_config_from_monitor.name, breaker_name)
        self.assertEqual(strategy_config_from_monitor.min_requests, 10)
        self.assertEqual(strategy_config_from_monitor.window, 60)
        self.assertTrue(BreakerStrategiesSingleton.get_instance().get(breaker_name).opened)

    @patch("core.circuit_breaker.strategy.core.BreakerBaseStrategy._get_monotonic")
    @patch("utils.remote_config_download.LocalRemoteConfig.get_breaker_config_for")
    @patch("community.personalisation.configuration.PersonalisationConfig.get_breaker_config_flags")
    def test_strategy_flag_turned_on_circuit_close_multiple_calls_500_with_circuit_open(self, breaker_config_flags, strategy_config, patched_monotonic):
        breaker_name = "strategy_flag_turned_on_circuit_close_multiple_calls_500_with_circuit_open"
        breaker_config_flags.return_value = {breaker_name: True}
        strategy_config.return_value = {"min_requests": 10, "window": 60}
        patched_monotonic.return_value = 1

        @circuit(breaker_name, recovery_timeout=40)
        def test_circuit():
            raise APIException("returning 500")

        for i in range(12):
            try:
                response = test_circuit()
                self.assertEqual(response.status_code, 500)
            except Exception:  # noqa
                if i < 9:
                    self.assertTrue(BreakerStrategiesSingleton.get_instance().get(breaker_name).closed)
                else:
                    self.assertTrue(BreakerStrategiesSingleton.get_instance().get(breaker_name).opened)

        self.assertTrue(BreakerStrategiesSingleton.get_instance().get(breaker_name).opened)
        strategy_config_from_monitor = BreakerStrategiesSingleton.get_instance().get(breaker_name).config
        self.assertEqual(strategy_config_from_monitor.name, breaker_name)
        self.assertEqual(strategy_config_from_monitor.min_requests, 10)
        self.assertEqual(strategy_config_from_monitor.window, 60)
        self.assertEqual(strategy_config_from_monitor.recovery_timeout, 40)

        patched_monotonic.return_value = 41
        self.assertTrue(BreakerStrategiesSingleton.get_instance().get(breaker_name).closed)

    @patch("core.circuit_breaker.strategy.core.BreakerBaseStrategy._get_monotonic")
    @patch("utils.remote_config_download.LocalRemoteConfig.get_breaker_config_for")
    @patch("community.personalisation.configuration.PersonalisationConfig.get_breaker_config_flags")
    def test_strategy_flag_turned_on_circuit_close_multiple_calls_500_with_circuit_open_and_close(self, breaker_config_flags, strategy_config, patched_monotonic):
        breaker_name = "strategy_flag_turned_on_circuit_close_multiple_calls_500_with_circuit_open_and_close"
        breaker_config_flags.return_value = {breaker_name: True}
        strategy_config.return_value = {"min_requests": 10, "window": 60}

        patched_monotonic.return_value = 1

        @circuit(breaker_name)
        def test_circuit():
            raise APIException("returning 500")

        @circuit(breaker_name)
        def test_circuit_close():
            return Response(status=200)

        for i in range(12):
            try:
                response = test_circuit()
                self.assertEqual(response.status_code, 500)
            except Exception:  # noqa
                print("CURRENT- --- - [", i, "]  - - - - ", BreakerStrategiesSingleton.get_instance().get(breaker_name).state)

                if i < 9:
                    self.assertTrue(BreakerStrategiesSingleton.get_instance().get(breaker_name).closed)
                else:
                    self.assertTrue(BreakerStrategiesSingleton.get_instance().get(breaker_name).opened)

        self.assertTrue(BreakerStrategiesSingleton.get_instance().get(breaker_name).opened)

        strategy_config_from_monitor = BreakerStrategiesSingleton.get_instance().get(breaker_name).config
        self.assertEqual(strategy_config_from_monitor.name, breaker_name)
        self.assertEqual(strategy_config_from_monitor.min_requests, 10)
        self.assertEqual(strategy_config_from_monitor.window, 60)
        self.assertEqual(strategy_config_from_monitor.recovery_timeout, 30)

        patched_monotonic.return_value = 31
        print("PATCHED MONOLOTINC =====", BreakerStrategiesSingleton.get_instance().get(breaker_name).state)
        response = test_circuit_close()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(BreakerStrategiesSingleton.get_instance().get(breaker_name).closed)

    @patch("core.circuit_breaker.strategy.core.BreakerBaseStrategy._get_monotonic")
    @patch("utils.remote_config_download.LocalRemoteConfig.get_breaker_config_for")
    @patch("community.personalisation.configuration.PersonalisationConfig.get_breaker_config_flags")
    def test_strategy_flag_turned_on_circuit_close_multiple_calls_500_with_circuit_open_and_close_with_params(self,
                                                                                                              breaker_config_flags,
                                                                                                              strategy_config,
                                                                                                              patched_monotonic):
        breaker_name = "strategy_flag_turned_on_circuit_close_multiple_calls_500_with_circuit_open_and_close_v2"
        breaker_config_flags.return_value = {breaker_name: True}
        strategy_config.return_value = {"min_requests": 10, "window": 60}

        patched_monotonic.return_value = 1

        @circuit(breaker_name, failure_threshold=0.3, recovery_timeout=45)
        def test_circuit():
            raise APIException("returning 500")

        @circuit(breaker_name, failure_threshold=0.3, recovery_timeout=45)
        def test_circuit_close():
            return Response(status=200)

        for i in range(12):
            try:
                response = test_circuit()
                self.assertEqual(response.status_code, 500)
            except Exception:  # noqa
                if i < 9:
                    self.assertTrue(BreakerStrategiesSingleton.get_instance().get(breaker_name).closed)
                else:
                    self.assertTrue(BreakerStrategiesSingleton.get_instance().get(breaker_name).opened)

        self.assertTrue(BreakerStrategiesSingleton.get_instance().get(breaker_name).opened)
        strategy_config_from_monitor = BreakerStrategiesSingleton.get_instance().get(breaker_name).config
        self.assertEqual(strategy_config_from_monitor.name, breaker_name)
        self.assertEqual(strategy_config_from_monitor.min_requests, 10)
        self.assertEqual(strategy_config_from_monitor.window, 60)
        self.assertEqual(strategy_config_from_monitor.error_threshold_open, 0.3)
        self.assertEqual(strategy_config_from_monitor.recovery_timeout, 45)

        patched_monotonic.return_value = 46
        response = test_circuit_close()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(BreakerStrategiesSingleton.get_instance().get(breaker_name).closed)
