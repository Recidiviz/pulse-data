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
from typing import Any, List, Optional, Tuple

import attr

from recidiviz.airflow.dags.raw_data.types import GCSMetadataRow
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.ingest.direct.gcs.filename_parts import (
    DirectIngestRawFilenameParts,
    filename_parts_from_path,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.types import assert_type

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
        cls, metadata_row: GCSMetadataRow, abs_path: GcsfsFilePath
    ) -> "RawGCSFileMetadataSummary":
        return RawGCSFileMetadataSummary(
            gcs_file_id=metadata_row.gcs_file_id,
            file_id=metadata_row.file_id,
            path=abs_path,
        )

    @classmethod
    def from_xcom(cls, xcom_input: List[Any]) -> "RawGCSFileMetadataSummary":
        if len(xcom_input) != 3:
            raise ValueError(
                f"Expected to retrieve 3 items to build {cls.__name__} but found {len(xcom_input)}"
            )

        return RawGCSFileMetadataSummary(
            gcs_file_id=assert_type(xcom_input[0], int),
            file_id=xcom_input[1],
            path=GcsfsFilePath.from_absolute_path(assert_type(xcom_input[2], str)),
        )


@attr.define
class RawBigQueryFileMetadataSummary:
    """This represents metadata about a "conceptual" file_id that exists in BigQuery,
    made up of at least one literal csv file |gcs_files|.

    Attributes:
        gcs_files (list[RawGCSFileMetadataSummary]): a list of RawGCSFileMetadataSummary
            objects that correspond to the literal csv files that comprise this single,
            conceptual file.
        file_tag (str): The shared file_tag from |gcs_files| cached on this object.
        file_id (int | None): A "conceptual" file id that corresponds to a single,
            conceptual file sent to us by the state. For raw files states send us in
            chunks (such as ContactNoteComment), each literal CSV that makes up the
            whole file will have a different gcs_file_id, but all of those entries will
            have the same file_id.
    """

    gcs_files: List[RawGCSFileMetadataSummary]
    file_tag: str
    file_id: Optional[int] = attr.field(default=None)

    def to_xcom(self) -> Tuple[int, List[str]]:
        return (
            assert_type(self.file_id, int),
            [gcs_file.path.abs_path() for gcs_file in self.gcs_files],
        )

    @classmethod
    def from_gcs_files(
        cls, gcs_files: List[RawGCSFileMetadataSummary]
    ) -> "RawBigQueryFileMetadataSummary":
        return RawBigQueryFileMetadataSummary(
            file_id=gcs_files[0].file_id,
            file_tag=gcs_files[0].parts.file_tag,
            gcs_files=gcs_files,
        )
