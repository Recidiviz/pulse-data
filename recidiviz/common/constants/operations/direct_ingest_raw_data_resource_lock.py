# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Constants related to the direct_ingest_raw_data_resource_lock operations table"""
from enum import unique
from typing import Dict

import recidiviz.common.constants.operations.enum_canonical_strings as operations_enum_strings
from recidiviz.common.constants.operations.operations_enum import OperationsEnum


@unique
class DirectIngestRawDataResourceLockResource(OperationsEnum):
    """The raw data resource locked by the direct ingest raw data resource lock."""

    BUCKET = operations_enum_strings.direct_ingest_lock_resource_bucket
    OPERATIONS_DATABASE = (
        operations_enum_strings.direct_ingest_lock_resource_operations_database
    )
    BIG_QUERY_RAW_DATA_DATASET = (
        operations_enum_strings.direct_ingest_lock_resource_big_query_raw_data_dataset
    )

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The raw data resource locked by the direct ingest raw data resource lock"
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["OperationsEnum", str]:
        return _DIRECT_INGEST_LOCK_RESOURCE_VALUE_DESCRIPTIONS


@unique
class DirectIngestRawDataLockActor(OperationsEnum):
    """The actor acquiring a direct ingest raw data resource lock."""

    ADHOC = operations_enum_strings.direct_ingest_lock_actor_adhoc
    PROCESS = operations_enum_strings.direct_ingest_lock_actor_process

    @classmethod
    def get_enum_description(cls) -> str:
        return "The actor acquiring a direct ingest raw data resource lock"

    @classmethod
    def get_value_descriptions(cls) -> Dict["OperationsEnum", str]:
        return _DIRECT_INGEST_LOCK_ACTOR_VALUE_DESCRIPTIONS


_DIRECT_INGEST_LOCK_RESOURCE_VALUE_DESCRIPTIONS: Dict[OperationsEnum, str] = {
    DirectIngestRawDataResourceLockResource.BUCKET: (
        "The BUCKET resource refers to the raw data GCS bucket associated with the "
        "region_code and ingest_instance of the lock."
        "n.b.: while data platform infrasturcture should acquire this "
        "resource lock before accessing it, states will transfer data directly to this "
        "bucket without acquiring the lock so this lock does not necessarily guarentee "
        "the bucket state will remain constant throughout the duration of the lock."
    ),
    DirectIngestRawDataResourceLockResource.OPERATIONS_DATABASE: (
        "The OPERATIONS_DATABASE resource refers to the CloudSQL operations database "
        "rows associated with a particular region_code and ingest_instance of the "
        "lock, most notably in the DirectIngestRawFileMetadataV2 table."
    ),
    DirectIngestRawDataResourceLockResource.BIG_QUERY_RAW_DATA_DATASET: (
        "The BIG_QUERY_RAW_DATA_DATASET resource refers to all tables in the BigQuery "
        "dataset associated with the region_code and ingest_instance of the lock, "
        "usually in the form of us_xx_raw_data (with an optional _secondary suffix)."
    ),
}

_DIRECT_INGEST_LOCK_ACTOR_VALUE_DESCRIPTIONS: Dict[OperationsEnum, str] = {
    DirectIngestRawDataLockActor.ADHOC: (
        "An ad hoc acquisition of a raw data resource lock. Typically, this will be "
        "from the admin panel and no lock_ttl is required to be set for this actor type."
    ),
    DirectIngestRawDataLockActor.PROCESS: (
        "A programatic acquisiton of a raw data resource lock. A lock_ttl is required "
        "to be set for this actor type."
    ),
}
