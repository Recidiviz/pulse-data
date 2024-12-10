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
"""Constants related to the direct_ingest_raw_file_import operations table"""
from enum import Enum, unique
from typing import Dict

import recidiviz.common.constants.operations.enum_canonical_strings as operations_enum_strings
from recidiviz.common.constants.operations.operations_enum import OperationsEnum


@unique
class DirectIngestRawFileImportStatus(OperationsEnum):
    """The status of a raw file import"""

    STARTED = operations_enum_strings.direct_ingest_raw_file_import_status_started
    SUCCEEDED = operations_enum_strings.direct_ingest_raw_file_import_status_succeeded
    DEFERRED = operations_enum_strings.direct_ingest_raw_file_import_status_deferred
    FAILED_UNKNOWN = (
        operations_enum_strings.direct_ingest_raw_file_import_status_failed_unknown
    )
    FAILED_LOAD_STEP = (
        operations_enum_strings.direct_ingest_raw_file_import_status_failed_load_step
    )
    FAILED_PRE_IMPORT_NORMALIZATION_STEP = (
        operations_enum_strings.direct_ingest_raw_file_import_status_failed_pre_import_normalization_step
    )
    FAILED_VALIDATION_STEP = (
        operations_enum_strings.direct_ingest_raw_file_import_status_failed_validation_step
    )
    FAILED_IMPORT_BLOCKED = (
        operations_enum_strings.direct_ingest_raw_file_import_status_failed_import_blocked
    )

    @classmethod
    def get_enum_description(cls) -> str:
        return "The status of a raw file import"

    @classmethod
    def get_value_descriptions(cls) -> Dict["OperationsEnum", str]:
        return _DIRECT_INGEST_RAW_FILE_IMPORT_STATUS_VALUE_DESCRIPTIONS


_DIRECT_INGEST_RAW_FILE_IMPORT_STATUS_VALUE_DESCRIPTIONS: Dict[OperationsEnum, str] = {
    DirectIngestRawFileImportStatus.STARTED: (
        "The STARTED status means that we know that an import has started for a "
        "file_id; likely, the import is still in progress. However, there is a chance "
        "that the import crashed and failed to set an updated status."
    ),
    DirectIngestRawFileImportStatus.SUCCEEDED: (
        "The SUCCEEDED status means that the an import has completed successfully. This "
        "means that new raw data is in the relevant BigQuery table, the import "
        "session table accurately reflects both the number of rows in the raw file as "
        "well as the rows added to the updated table and the raw data files associated "
        "with the file_id have been moved to storage."
    ),
    DirectIngestRawFileImportStatus.DEFERRED: (
        "The DEFERRED status means that the raw data import DAG found this file in the"
        "ingest bucket, but decided to not import it during this run of the DAG. This"
        "is expected to happen during secondary reimport to limit the length of any "
        "single DAG run."
    ),
    DirectIngestRawFileImportStatus.FAILED_UNKNOWN: (
        "The FAILED_UNKNOWN status is a catch-all for an import failing without the "
        "import DAG identifying what the specific issue is."
    ),
    DirectIngestRawFileImportStatus.FAILED_LOAD_STEP: (
        "The FAILED_LOAD_STEP status means that the import failed during the load step, "
        "or the step when we use BigQuery's load job to insert new data into a temp "
        "table and then insert these rows into the existing raw data table."
    ),
    DirectIngestRawFileImportStatus.FAILED_PRE_IMPORT_NORMALIZATION_STEP: (
        "The FAILED_PRE_IMPORT_NORMALIZATION_STEP status means that the import failed "
        "during the pre-import normalization step, or the step when we prepare the  "
        "raw file for the BigQuery load job by standardizing the CSV Dialect."
    ),
    DirectIngestRawFileImportStatus.FAILED_VALIDATION_STEP: (
        "The FAILED_VALIDATION_STEP status means that the import failed during the "
        "validation step, or the step when we run validation queries against the temp "
        "raw table in BigQuery and return any errors."
    ),
    DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED: (
        "The FAILED_IMPORT_BLOCKED status means that the import of this file was blocked "
        "by a failure for a file with the same file_tag for an earlier update_datetime, "
        "not necessarily due to an issue with the file itself. This error occurs in order "
        "to ensure data is always guaranteed to be added to the raw data table in ascending "
        "update_datetime order."
    ),
}


class DirectIngestRawFileImportStatusBucket(Enum):
    """Higher-level status buckets for DirectIngestRawFileImportStatus"""

    IN_PROGRESS: str = "IN_PROGRESS"
    SUCCEEDED: str = "SUCCEEDED"
    FAILED: str = "FAILED"

    __IN_PROGRESS_STATUSES: frozenset[DirectIngestRawFileImportStatus] = frozenset(
        [
            DirectIngestRawFileImportStatus.STARTED,
            DirectIngestRawFileImportStatus.DEFERRED,
        ]
    )
    __SUCCEEDED_STATUSES: frozenset[DirectIngestRawFileImportStatus] = frozenset(
        [DirectIngestRawFileImportStatus.SUCCEEDED]
    )
    __FAILED_STATUSES: frozenset[DirectIngestRawFileImportStatus] = frozenset(
        [
            DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED,
            DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
            DirectIngestRawFileImportStatus.FAILED_VALIDATION_STEP,
            DirectIngestRawFileImportStatus.FAILED_PRE_IMPORT_NORMALIZATION_STEP,
            DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
        ]
    )

    @classmethod
    def in_progress_statuses(cls) -> frozenset[DirectIngestRawFileImportStatus]:
        return cls.__IN_PROGRESS_STATUSES

    @classmethod
    def succeeded_statuses(cls) -> frozenset[DirectIngestRawFileImportStatus]:
        return cls.__SUCCEEDED_STATUSES

    @classmethod
    def failed_statuses(cls) -> frozenset[DirectIngestRawFileImportStatus]:
        return cls.__FAILED_STATUSES

    @classmethod
    def from_import_status(
        cls, status: DirectIngestRawFileImportStatus
    ) -> "DirectIngestRawFileImportStatusBucket":

        if status in cls.__IN_PROGRESS_STATUSES:
            return cls.IN_PROGRESS

        if status in cls.__SUCCEEDED_STATUSES:
            return cls.SUCCEEDED

        if status in cls.__FAILED_STATUSES:
            return cls.FAILED

        raise ValueError(
            f"Unrecognized import status: {status}; please add it to the list of values in {__name__}"
        )

    def for_api(self) -> str:
        """Transforms a value for the AdminPanel."""
        return self.value.replace("_", " ")
