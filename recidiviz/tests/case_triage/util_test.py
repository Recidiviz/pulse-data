# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Tests for the Redis connection options in recidiviz/case_triage/util.py."""
import unittest

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry

from recidiviz.case_triage.util import (
    REDIS_RETRY_MAX_RETRIES,
    get_redis_connection_options,
)
from recidiviz.utils.types import assert_type


class TestGetRedisConnectionOptions(unittest.TestCase):
    """Tests for get_redis_connection_options."""

    def _call_with_retry_after_failures(
        self, *, error: Exception, num_failures: int
    ) -> tuple[str, int]:
        """Returns the result of a command that raises error num_failures times
        before succeeding, run through the configured Retry, and how many times
        the command was attempted."""
        retry = assert_type(get_redis_connection_options()["retry"], Retry)
        attempts = 0

        def command() -> str:
            nonlocal attempts
            attempts += 1
            if attempts <= num_failures:
                raise error
            return "OK"

        return retry.call_with_retry(command, lambda _: None), attempts

    def test_retries_redis_connection_error(self) -> None:
        # redis-py wraps a socket reset in its own ConnectionError, which is not
        # a subclass of the builtin ConnectionError.
        result, attempts = self._call_with_retry_after_failures(
            error=RedisConnectionError(
                "Error while reading from 10.0.0.1:6379 : (104, 'Connection reset by peer')"
            ),
            num_failures=1,
        )
        self.assertEqual("OK", result)
        self.assertEqual(2, attempts)

    def test_retries_redis_timeout_error(self) -> None:
        result, attempts = self._call_with_retry_after_failures(
            error=RedisTimeoutError("Timeout reading from socket"),
            num_failures=1,
        )
        self.assertEqual("OK", result)
        self.assertEqual(2, attempts)

    def test_raises_after_max_retries(self) -> None:
        with self.assertRaises(RedisConnectionError):
            self._call_with_retry_after_failures(
                error=RedisConnectionError("Connection reset by peer"),
                num_failures=REDIS_RETRY_MAX_RETRIES + 1,
            )
