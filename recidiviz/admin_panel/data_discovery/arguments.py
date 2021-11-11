# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021  Recidiviz, Inc.
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
""" Arguments data class for the data discovery tool """
import json
import os
import uuid
from datetime import date
from typing import List, Any, TypedDict, Literal

import attr

from recidiviz.admin_panel.data_discovery.types import DataDiscoveryTTL
from recidiviz.admin_panel.data_discovery.utils import get_data_discovery_cache
from recidiviz.cloud_memorystore.redis_communicator import RedisCommunicator
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.types.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
    gcsfs_direct_ingest_storage_directory_path_for_region,
)
from recidiviz.utils.environment import in_gcp


class Condition(TypedDict):
    column: str
    operator: Literal["in", "not in"]
    values: List[str]


class ConditionGroup(TypedDict):
    conditions: List[Condition]


@attr.s
class DataDiscoveryArgs:
    """ Arguments data class for the data discovery tool """

    # Direct ingest region code
    region_code: str = attr.ib()

    # ID of this search
    id: str = attr.ib()

    # Date time selector. Searches for files in the given months
    start_date: date = attr.ib()
    end_date: date = attr.ib()

    # Data will be filtered to rows that match any permutation of these conditions (columns/values)
    condition_groups: List[ConditionGroup] = attr.ib(factory=list)

    raw_files: List[str] = attr.ib(factory=list)
    ingest_views: List[str] = attr.ib(factory=list)

    @property
    def communication_channel_uuid(self) -> uuid.UUID:
        return uuid.UUID(self.id)

    @property
    def direct_ingest_storage_directory(self) -> GcsfsDirectoryPath:
        if in_gcp():
            return gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code=self.region_code,
                system_level=SystemLevel.STATE,
                ingest_instance=DirectIngestInstance.PRIMARY,
            )

        # Local override
        return GcsfsDirectoryPath.from_absolute_path(
            f"recidiviz-staging-direct-ingest-state-storage/{self.region_code.lower()}"
        )

    @property
    def search_paths(self) -> List[str]:
        """ Returns the search paths to use when listing GCS file entries """
        return [
            os.path.join(
                self.region_code.lower(), str(GcsfsDirectIngestFileType.RAW_DATA.value)
            ),
            os.path.join(
                self.region_code.lower(),
                str(GcsfsDirectIngestFileType.INGEST_VIEW.value),
            ),
        ]

    @property
    def state_files_cache_key(self) -> str:
        """ Returns the cache key for a state's GCS files """
        return f"files-list-{self.region_code}"


class DataDiscoveryArgsFactory:
    """ Factory for creating/reading DataDiscoveryArgs to/from Redis"""

    @classmethod
    def create(cls, **kwargs: Any) -> DataDiscoveryArgs:
        """ Factory method for DataDiscoveryArgs. Persists JSON serialized DataDiscoveryArgs to redis """
        cache = get_data_discovery_cache()
        communicator = RedisCommunicator.create(cache, max_messages=25)

        data_discovery_args = DataDiscoveryArgs(
            id=str(communicator.channel_uuid), **kwargs
        )
        cache.setex(
            data_discovery_args.id,
            DataDiscoveryTTL.DATA_DISCOVERY_ARGS,
            json.dumps(attr.asdict(data_discovery_args)),
        )

        return data_discovery_args

    @classmethod
    def fetch(cls, discovery_id: str) -> DataDiscoveryArgs:
        """ Factory method for retrieving existing DataDiscoveryArgs"""
        cache = get_data_discovery_cache()
        serialized_arguments = cache.get(discovery_id)

        if not serialized_arguments:
            raise ValueError(f"Could not find discovery with ID {discovery_id}")

        args = json.loads(serialized_arguments.decode("utf-8"))
        args["start_date"] = date.fromisoformat(args.pop("start_date"))
        args["end_date"] = date.fromisoformat(args.pop("end_date"))

        return DataDiscoveryArgs(**args)
