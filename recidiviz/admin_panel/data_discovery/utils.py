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
""" Data discovery utilities """
import uuid

import redis

from recidiviz.cloud_memorystore.redis_communicator import RedisCommunicator
from recidiviz.utils.environment import in_gcp
from recidiviz.utils.secrets import get_secret


def get_data_discovery_cache() -> redis.Redis:
    """
    Returns a client for the data discovery Redis instance.
    Redis commands can be issued directly to this client and all connection handling is done under inside `redis.Redis`.
    Idle connections will be closed by `redis.Redis` automatically.
    To get query cached data discovery information from the cache, you may want to provide this `Redis` instance
    to a `RedisCommunicator`, `DataDiscoveryArgsFactory`, or `SingleIngestFileParquetCache` class.
    """
    if not in_gcp():
        return redis.Redis()

    redis_host = get_secret("data_discovery_redis_host")
    redis_port = get_secret("data_discovery_redis_port")

    if redis_host and redis_port:
        return redis.Redis(
            host=redis_host,
            port=int(redis_port),
        )

    raise ValueError("Cannot find data discovery redis secrets")


def get_data_discovery_communicator(channel_uuid: uuid.UUID) -> RedisCommunicator:
    return RedisCommunicator(get_data_discovery_cache(), channel_uuid)
