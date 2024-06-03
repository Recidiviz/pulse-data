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
from functools import cached_property
from typing import Optional, Tuple

import attr

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.ingest.direct.gcs.filename_parts import (
    DirectIngestRawFilenameParts,
    filename_parts_from_path,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

RESOURCE_LOCKS_NEEDED = [
    DirectIngestRawDataResourceLockResource.BIG_QUERY_RAW_DATA_DATASET,
    DirectIngestRawDataResourceLockResource.BUCKET,
    DirectIngestRawDataResourceLockResource.OPERATIONS_DATABASE,
]
RESOURCE_LOCK_AQUISITION_DESCRIPTION = (
    "Lock held for the duration of the raw data import dag"
)
RESOURCE_LOCK_TTL_SECONDS_PRIMARY = 3 * 60 * 60  # 3 hours
RESOURCE_LOCK_TTL_SECONDS_SECONDARY = 3 * 60 * 60  # 6 hours


def get_resource_lock_ttl(raw_data_instance: DirectIngestInstance) -> int:
    """Returns the ttl in seconds for the resources listed in RESOURCE_LOCKS_NEEDED. We
    bifurcate bewteen primary and secondary as we expect secondary runs to take longer
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


@attr.define
class RawGCSFileMetadataSummary:
    """This represents metadata about a single literal file (e.g. a CSV) that is stored
    in GCS

    Attributes:
        gcs_file_id (int): An id that corresponds to the literal file in Google Cloud
            Storage in direct_ingest_raw_gcs_file_metadata. A single file will always
            have a single gcs_file_id.
        file_id (int | None): A "conceptual" file id that corresponds to a single,
            conceptual file sent to us by the state. For raw files states send us in
            chunks (such as ContactNoteComment), each literal CSV that makes up the
            whole file will have a different gcs_file_id, but all of those entries will
            have the same file_id.
        path (GcsfsFilePath): The path in Google Cloud Storage of the file.
    """

    gcs_file_id: int
    file_id: Optional[int]
    path: GcsfsFilePath

    @cached_property
    def parts(self) -> DirectIngestRawFilenameParts:
        return filename_parts_from_path(self.path)

    def to_xcom(self) -> Tuple[int, Optional[int], str]:
        return self.gcs_file_id, self.file_id, self.path.abs_path()

    @classmethod
    def from_gcs_metadata_table_row(
        cls, db_record: Tuple[int, str], abs_path: GcsfsFilePath
    ) -> "RawGCSFileMetadataSummary":
        return RawGCSFileMetadataSummary(
            gcs_file_id=db_record[0], file_id=None, path=abs_path
        )
