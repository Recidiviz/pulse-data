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
"""Constants related to the direct_ingest_raw_data_import_session operations table"""
from enum import unique
from typing import Dict

import recidiviz.common.constants.operations.enum_canonical_strings as operations_enum_strings
from recidiviz.common.constants.operations.operations_enum import OperationsEnum


@unique
class DirectIngestRawDataImportSessionStatus(OperationsEnum):
    """The status of a raw data import session"""

    STARTED = operations_enum_strings.direct_ingest_import_session_status_started
    SUCCEEDED = operations_enum_strings.direct_ingest_import_session_status_succeeded
    FAILED_UNKNWON = (
        operations_enum_strings.direct_ingest_import_session_status_failed_unknown
    )
    FAILED_LOAD_STEP = (
        operations_enum_strings.direct_ingest_import_session_status_failed_load_step
    )
    FAILED_PRE_IMPORT_NORMALIZATION_STEP = (
        operations_enum_strings.direct_ingest_import_session_status_failed_pre_import_normalization_step
    )

    @classmethod
    def get_enum_description(cls) -> str:
        return "The status of a raw data import session"

    @classmethod
    def get_value_descriptions(cls) -> Dict["OperationsEnum", str]:
        return _DIRECT_INGEST_IMPORT_SESSION_STATUS_VALUE_DESCRIPTIONS


_DIRECT_INGEST_IMPORT_SESSION_STATUS_VALUE_DESCRIPTIONS: Dict[OperationsEnum, str] = {
    DirectIngestRawDataImportSessionStatus.STARTED: (
        "The STARTED status means that we know that an import has started for a "
        "file_id; likely, the import is still in progress. However, there is a chance "
        "that the import crashed and failed to set an updated status."
    ),
    DirectIngestRawDataImportSessionStatus.SUCCEEDED: (
        "The SUCCEEDED status means that the an import has completed successfully. This "
        "means that new raw data is in the relevant BigQuery table, the import "
        "session table accurately reflects both the number of rows in the raw file as "
        "well as the rows added to the updated table and the raw data files associated "
        "with the file_id have been moved to storage."
    ),
    DirectIngestRawDataImportSessionStatus.FAILED_UNKNWON: (
        "The FAILED_UNKNWON status is a catch-all for an import failing without the "
        "import DAG identifying what the specfic issue is."
    ),
    DirectIngestRawDataImportSessionStatus.FAILED_LOAD_STEP: (
        "The FAILED_LOAD_STEP status means that the import failed during the load step, "
        "or the step when we use BigQuery's load job to insert new data into a temp "
        "table and then insert these rows into the existing raw data table."
    ),
    DirectIngestRawDataImportSessionStatus.FAILED_PRE_IMPORT_NORMALIZATION_STEP: (
        "The FAILED_PRE_IMPORT_NORMALIZATION_STEP status means that the import failed "
        "during the pre-import normalization step, or the step when we prepare the  "
        "raw file for the BigQuery load job by standardizing the CSV Dialect."
    ),
}
