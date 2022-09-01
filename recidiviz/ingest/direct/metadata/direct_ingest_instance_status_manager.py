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
"""An interface for reading and updating DirectIngestInstanceStatuses. For a visualization of valid instance statuses
transitions, please refer to http://go/ingest-instance-status-flow.
"""
import abc
from typing import Dict, List, Optional

from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

# Invalid statuses, by instance.
INVALID_STATUSES: Dict[DirectIngestInstance, List[DirectIngestStatus]] = {
    DirectIngestInstance.PRIMARY: [
        DirectIngestStatus.READY_TO_FLASH,
        DirectIngestStatus.STALE_RAW_DATA,
        DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
        DirectIngestStatus.NO_RERUN_IN_PROGRESS,
    ],
    DirectIngestInstance.SECONDARY: [DirectIngestStatus.UP_TO_DATE],
}

# Valid initial statuses, by instance.
VALID_INITIAL_STATUSES: Dict[DirectIngestInstance, List[DirectIngestStatus]] = {
    DirectIngestInstance.PRIMARY: [
        # UP_TO_DATE can be a default initial state for PRIMARY, when the primary instance
        # has no work done and no work to do yet.
        DirectIngestStatus.UP_TO_DATE,
        DirectIngestStatus.STANDARD_RERUN_STARTED,
    ],
    DirectIngestInstance.SECONDARY: [
        DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
        DirectIngestStatus.STANDARD_RERUN_STARTED,
        # NO_RERUN_IN_PROGRESS can be a default initial state for SECONDARY, when the secondary instance
        # has no work done and no work to do yet.
        DirectIngestStatus.NO_RERUN_IN_PROGRESS,
    ],
}

# Valid status transitions that are applicable to both PRIMARY and SECONDARY.
SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS: Dict[
    DirectIngestStatus, List[DirectIngestStatus]
] = {
    DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS: [
        DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
        DirectIngestStatus.STANDARD_RERUN_STARTED,
        DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
        DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
    ],
    DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS: [
        DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
        DirectIngestStatus.STANDARD_RERUN_STARTED,
        DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
        DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
        DirectIngestStatus.UP_TO_DATE,
    ],
    DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS: [
        DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
        DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
        DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
    ],
    DirectIngestStatus.FLASH_COMPLETED: [DirectIngestStatus.FLASH_IN_PROGRESS],
}

# Valid status transitions, per instance.
VALID_CURRENT_STATUS_TRANSITIONS: Dict[
    DirectIngestInstance, Dict[DirectIngestStatus, List[DirectIngestStatus]]
] = {
    DirectIngestInstance.PRIMARY: {
        DirectIngestStatus.STANDARD_RERUN_STARTED: [
            DirectIngestStatus.UP_TO_DATE,
            DirectIngestStatus.FLASH_COMPLETED,
        ],
        DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS: [
            DirectIngestStatus.FLASH_COMPLETED,
            DirectIngestStatus.UP_TO_DATE,
        ]
        + SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS
        ],
        DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS: SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
            DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS
        ],
        DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS: SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
            DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS
        ],
        DirectIngestStatus.FLASH_COMPLETED: SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
            DirectIngestStatus.FLASH_COMPLETED
        ],
        # PRIMARY ONLY status.
        DirectIngestStatus.UP_TO_DATE: [
            DirectIngestStatus.UP_TO_DATE,
            DirectIngestStatus.FLASH_COMPLETED,
            DirectIngestStatus.STANDARD_RERUN_STARTED,
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
            DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
        ],
        # PRIMARY specific check for FLASH_IN_PROGRESS. Flashing could start when ingest
        # is at any point in PRIMARY - it doesn't matter because everything in PRIMARY
        # will be overwritten.
        DirectIngestStatus.FLASH_IN_PROGRESS: [
            s
            for s in list(DirectIngestStatus)
            if s not in INVALID_STATUSES[DirectIngestInstance.PRIMARY]
        ],
    },
    DirectIngestInstance.SECONDARY: {
        DirectIngestStatus.STANDARD_RERUN_STARTED: [
            DirectIngestStatus.NO_RERUN_IN_PROGRESS
        ],
        DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED: [
            DirectIngestStatus.NO_RERUN_IN_PROGRESS,
        ],
        # SECONDARY specific check for FLASH_IN_PROGRESS.
        DirectIngestStatus.FLASH_IN_PROGRESS: [DirectIngestStatus.READY_TO_FLASH],
        DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS: [
            DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
            DirectIngestStatus.STALE_RAW_DATA,
            # We would transition from this status if we're doing new raw data import in
            # PRIMARY.
            DirectIngestStatus.READY_TO_FLASH,
        ]
        + SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS
        ],
        DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS: SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
            DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS
        ]
        + [DirectIngestStatus.READY_TO_FLASH],
        DirectIngestStatus.READY_TO_FLASH: [
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
            DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
        ],
        DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS: SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
            DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS
        ],
        DirectIngestStatus.STALE_RAW_DATA: [
            DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
            DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
            DirectIngestStatus.READY_TO_FLASH,
        ],
        DirectIngestStatus.FLASH_COMPLETED: SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
            DirectIngestStatus.FLASH_COMPLETED
        ],
        DirectIngestStatus.NO_RERUN_IN_PROGRESS: [DirectIngestStatus.FLASH_COMPLETED],
    },
}


class DirectIngestInstanceStatusManager:
    """An interface for reading and updating DirectIngestInstanceStatuses. For a
    visualization of valid instance statuses transitions, please refer to
    http://go/ingest-instance-status-flow.
    """

    def __init__(self, region_code: str, ingest_instance: DirectIngestInstance):
        self.region_code = region_code.upper()
        self.ingest_instance = ingest_instance

    @abc.abstractmethod
    def get_raw_data_source_instance(self) -> DirectIngestInstance:
        """Returns the current raw data source of the ingest instance associated with
        this status manager.
        """

    @abc.abstractmethod
    def change_status_to(self, new_status: DirectIngestStatus) -> None:
        """Change status to the passed in status."""

    @abc.abstractmethod
    def get_current_status(self) -> Optional[DirectIngestStatus]:
        """Get current status."""

    def validate_transition(
        self,
        ingest_instance: DirectIngestInstance,
        current_status: Optional[DirectIngestStatus],
        new_status: DirectIngestStatus,
    ) -> None:
        """Validates that the transition from the current status to the new status is
        valid and throws a ValueError if it is not.
        """
        if new_status in INVALID_STATUSES[ingest_instance]:
            raise ValueError(
                f"The status={new_status.value} is an invalid status to transition to "
                f"in instance={ingest_instance.value}"
            )

        if (
            current_status is None
            and new_status not in VALID_INITIAL_STATUSES[ingest_instance]
        ):
            raise ValueError(
                f"Invalid initial status [{new_status}] for instance "
                f"[{ingest_instance}]. Initial statuses can only be one of "
                f"the following: "
                f"{VALID_INITIAL_STATUSES[ingest_instance]}"
            )

        if current_status is None:
            # The new_status is a valid start of rerun status.
            return

        valid_current_statuses = VALID_CURRENT_STATUS_TRANSITIONS[ingest_instance][
            new_status
        ]

        if current_status not in valid_current_statuses:
            current_status_str = current_status.value if current_status else None
            raise ValueError(
                f"[{self.region_code}][{self.ingest_instance}] Can only transition from the following statuses to "
                f"{new_status.value}: {[status.value for status in valid_current_statuses]}. Current status is "
                f"{current_status_str}."
            )
