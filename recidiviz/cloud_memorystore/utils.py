# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
""" Utils for working with Redis """
import time
from datetime import datetime, timedelta
from typing import List, Iterator, Set

import redis


class RedisKeyTimeoutError(TimeoutError):
    def __init__(self, missing_keys: List[str]) -> None:
        self.message = "Timed out waiting for Redis keys"
        self.missing_keys = missing_keys
        super().__init__(self.message)


def await_redis_keys(
    cache: redis.Redis,
    required_keys: List[str],
    timeout_timedelta: timedelta = timedelta(minutes=2),
) -> Iterator[Set[str]]:
    """Waits for a list of Redis keys to exist before returning
    Yields the list of remaining keys so the caller can track progress
    """
    remaining_keys = set(required_keys)
    timeout = datetime.now() + timeout_timedelta

    while remaining_keys and datetime.now() <= timeout:
        for key in cache.scan_iter():
            remaining_keys.discard(key.decode("utf-8"))

        yield remaining_keys

        time.sleep(1)

    if remaining_keys:
        raise RedisKeyTimeoutError(missing_keys=list(remaining_keys))
