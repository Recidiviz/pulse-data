#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Constants related to the `direct_ingest_instance_status` table."""

from enum import unique
from typing import Dict

import recidiviz.common.constants.operations.enum_canonical_strings as operations_enum_strings
from recidiviz.common.constants.operations.operations_enum import OperationsEnum


@unique
class DirectIngestStatus(OperationsEnum):
    """The status of a direct ingest instance."""

    RAW_DATA_REIMPORT_STARTED = (
        operations_enum_strings.direct_ingest_status_raw_data_reimport_started
    )
    INITIAL_STATE = operations_enum_strings.direct_ingest_status_initial_state
    RAW_DATA_IMPORT_IN_PROGRESS = (
        operations_enum_strings.direct_ingest_status_raw_data_import_in_progress
    )
    READY_TO_FLASH = operations_enum_strings.direct_ingest_status_ready_to_flash
    FLASH_IN_PROGRESS = operations_enum_strings.direct_ingest_status_flash_in_progress
    FLASH_COMPLETED = operations_enum_strings.direct_ingest_status_flash_completed
    RAW_DATA_REIMPORT_CANCELED = (
        operations_enum_strings.direct_ingest_status_raw_data_reimport_canceled
    )
    RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS = (
        operations_enum_strings.direct_ingest_status_raw_data_reimport_cancellation_in_progress
    )
    RAW_DATA_UP_TO_DATE = (
        operations_enum_strings.direct_ingest_status_raw_data_up_to_date
    )
    STALE_RAW_DATA = operations_enum_strings.direct_ingest_status_stale_raw_data
    NO_RAW_DATA_REIMPORT_IN_PROGRESS = (
        operations_enum_strings.direct_ingest_status_no_raw_data_reimport_in_progress
    )

    @classmethod
    def get_enum_description(cls) -> str:
        return "The status of an ingest instance."

    @classmethod
    def get_value_descriptions(cls) -> Dict["OperationsEnum", str]:
        return _DIRECT_INGEST_INSTANCE_STATUS_VALUE_DESCRIPTIONS


_DIRECT_INGEST_INSTANCE_STATUS_VALUE_DESCRIPTIONS: Dict[OperationsEnum, str] = {
    DirectIngestStatus.RAW_DATA_REIMPORT_STARTED: (
        "Rows are added with this status (in SECONDARY "
        "only) when a SECONDARY rerun is triggered from the Admin Panel that involves "
        "importing a new (or modified) set of raw data before generating ingest view "
        "results and committing that data to Postgres."
    ),
    DirectIngestStatus.INITIAL_STATE: (
        "A single row is added with this status for the "
        "PRIMARY instance when a region is first added."
    ),
    DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS: (
        "Rows are added with this status when raw data import to BQ is in progress."
    ),
    DirectIngestStatus.READY_TO_FLASH: (
        "Rows are added with this status (for the SECONDARY instance only) when the "
        "scheduler finds no more work to do. If doing a rerun with new raw data, this "
        "also means that new raw data has not been added to the PRIMARY instance "
        "since the start of this rerun that has not yet been processed in this "
        "instance."
    ),
    DirectIngestStatus.FLASH_IN_PROGRESS: (
        "Rows are added with this status to both instances when a flash from "
        "SECONDARY to PRIMARY is in progress."
    ),
    DirectIngestStatus.FLASH_COMPLETED: (
        "Rows are added with this status to both instances when a flash from "
        "SECONDARY to PRIMARY is completed. In SECONDARY, no statuses will be added "
        "after this status until a new rerun is started. In PRIMARY, the scheduler "
        "will transition this status to the next appropriate status."
    ),
    DirectIngestStatus.RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS: (
        "Rows are added with this status in SECONDARY "
        "instances when a cancellation of a raw data reimport is in progress."
    ),
    DirectIngestStatus.RAW_DATA_REIMPORT_CANCELED: (
        "Rows are added with this status (for the "
        "SECONDARY instance only) when a raw data reimport is canceled before results "
        "are flashed from SECONDARY to PRIMARY (this can be due to validations "
        "indicating issues with a rerun, for example)."
    ),
    DirectIngestStatus.RAW_DATA_UP_TO_DATE: (
        "Rows are added with this status (in PRIMARY "
        "instances only) when the scheduler finds no raw data import work to do in the "
        "PRIMARY instance."
    ),
    DirectIngestStatus.STALE_RAW_DATA: (
        "Rows are added with this status (in SECONDARY instances only) if a) we are "
        "doing a rerun using SECONDARY raw data and b) the scheduler has found no more "
        "work to do and c) the PRIMARY raw data is more up to date than the SECONDARY "
        "raw data."
    ),
    DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS: (
        "Rows are added with this status (in SECONDARY "
        "instances only) if no raw data reimport is in progress."
    ),
}
