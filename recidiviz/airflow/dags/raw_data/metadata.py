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
"""Metadata and constants used in the raw data import DAG"""
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

RESOURCE_LOCKS_NEEDED = [
    DirectIngestRawDataResourceLockResource.BIG_QUERY_RAW_DATA_DATASET,
    DirectIngestRawDataResourceLockResource.BUCKET,
    DirectIngestRawDataResourceLockResource.OPERATIONS_DATABASE,
]
RESOURCE_LOCK_ACQUISITION_DESCRIPTION = (
    "Lock held for the duration of the raw data import dag"
)
RESOURCE_LOCK_TTL_SECONDS_PRIMARY = 3 * 60 * 60  # 3 hours
RESOURCE_LOCK_TTL_SECONDS_SECONDARY = 3 * 60 * 60  # 6 hours


SKIPPED_FILE_ERRORS: str = "skipped_file_errors"
APPEND_READY_FILE_BATCHES: str = "append_ready_file_batches"
IMPORT_READY_FILES: str = "import_ready_files"
REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_METADATA: str = (
    "requires_pre_import_normalization_files_big_query_metadata"
)
REQUIRES_PRE_IMPORT_NORMALIZATION_FILES: str = "requires_pre_import_normalization_files"


def get_resource_lock_ttl(raw_data_instance: DirectIngestInstance) -> int:
    """Returns the ttl in seconds for the resources listed in RESOURCE_LOCKS_NEEDED. We
    bifurcate between primary and secondary as we expect secondary runs to take longer
    and don't want to the ttl to permit other processes to acquire the resource lock
    without the raw data import DAG actually being finished with it.
    """
    match raw_data_instance:
        case DirectIngestInstance.PRIMARY:
            return RESOURCE_LOCK_TTL_SECONDS_PRIMARY
        case DirectIngestInstance.SECONDARY:
            return RESOURCE_LOCK_TTL_SECONDS_SECONDARY
        case _:
            raise ValueError(f"Unexpected value: {raw_data_instance.value}")
