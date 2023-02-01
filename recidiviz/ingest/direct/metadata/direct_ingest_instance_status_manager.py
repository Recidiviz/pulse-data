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
import datetime
from typing import Dict, List, Optional

from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.operations.entities import DirectIngestInstanceStatus
from recidiviz.utils import environment

# Invalid statuses, by instance.
INVALID_STATUSES: Dict[DirectIngestInstance, List[DirectIngestStatus]] = {
    DirectIngestInstance.PRIMARY: [
        DirectIngestStatus.READY_TO_FLASH,
        DirectIngestStatus.FLASH_CANCELLATION_IN_PROGRESS,
        DirectIngestStatus.FLASH_CANCELED,
        DirectIngestStatus.STALE_RAW_DATA,
        DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
        DirectIngestStatus.NO_RERUN_IN_PROGRESS,
        DirectIngestStatus.BLOCKED_ON_PRIMARY_RAW_DATA_IMPORT,
    ],
    DirectIngestInstance.SECONDARY: [DirectIngestStatus.UP_TO_DATE],
}

# Statuses that an instance can only transition to via some human intervention.
HUMAN_INTERVENTION_STATUSES: Dict[DirectIngestInstance, List[DirectIngestStatus]] = {
    DirectIngestInstance.PRIMARY: [
        # Note: `STANDARD_RERUN_STARTED` is automatically added an initial status via a migration
        DirectIngestStatus.FLASH_IN_PROGRESS,
    ],
    DirectIngestInstance.SECONDARY: [
        DirectIngestStatus.FLASH_CANCELLATION_IN_PROGRESS,
        DirectIngestStatus.FLASH_CANCELED,
        DirectIngestStatus.FLASH_IN_PROGRESS,
        DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
        DirectIngestStatus.STANDARD_RERUN_STARTED,
    ],
}

# Valid instance statuses that are associated with the start of a rerun.
VALID_START_OF_RERUN_STATUSES: Dict[DirectIngestInstance, List[DirectIngestStatus]] = {
    DirectIngestInstance.PRIMARY: [
        DirectIngestStatus.STANDARD_RERUN_STARTED,
    ],
    DirectIngestInstance.SECONDARY: [
        DirectIngestStatus.STANDARD_RERUN_STARTED,
        DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
    ],
}

# Valid status transitions that are applicable to both PRIMARY and SECONDARY.
SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS: Dict[
    DirectIngestStatus, List[DirectIngestStatus]
] = {
    DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS: [
        DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
        DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
    ],
    DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS: [
        DirectIngestStatus.STANDARD_RERUN_STARTED,
        DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
        DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
        DirectIngestStatus.UP_TO_DATE,
    ],
    DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS: [
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
            *SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS
            ],
            DirectIngestStatus.FLASH_COMPLETED,
            DirectIngestStatus.UP_TO_DATE,
            DirectIngestStatus.STANDARD_RERUN_STARTED,
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
            DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
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
            *SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS
            ],
            DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
            DirectIngestStatus.STALE_RAW_DATA,
            # We would transition from this status if we process new files in SECONDARY
            # (that were not seen in PRIMARY)
            DirectIngestStatus.READY_TO_FLASH,
        ],
        DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS: [
            *SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS
            ],
            DirectIngestStatus.READY_TO_FLASH,
            DirectIngestStatus.BLOCKED_ON_PRIMARY_RAW_DATA_IMPORT,
        ],
        DirectIngestStatus.READY_TO_FLASH: [
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
            # We would transition from this status if files are processed in PRIMARY that are not in SECONDARY
            # ingest views
            DirectIngestStatus.BLOCKED_ON_PRIMARY_RAW_DATA_IMPORT,
            DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
            DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
        ],
        DirectIngestStatus.FLASH_CANCELED: [
            DirectIngestStatus.FLASH_CANCELLATION_IN_PROGRESS,
        ],
        DirectIngestStatus.FLASH_CANCELLATION_IN_PROGRESS:
        # ANY valid secondary status is allowed before proceeding with a flash cancellation
        list(
            (set(DirectIngestStatus))
            - set(INVALID_STATUSES[DirectIngestInstance.SECONDARY])
        ),
        DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS: [
            *SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS
            ],
            DirectIngestStatus.BLOCKED_ON_PRIMARY_RAW_DATA_IMPORT,
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
        DirectIngestStatus.NO_RERUN_IN_PROGRESS: [
            DirectIngestStatus.FLASH_CANCELED,
            DirectIngestStatus.FLASH_COMPLETED,
        ],
        DirectIngestStatus.BLOCKED_ON_PRIMARY_RAW_DATA_IMPORT: [
            DirectIngestStatus.STANDARD_RERUN_STARTED,
            DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
            DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
            # We would transition from this status if we are ready to flash and then new raw data is
            # processed in PRIMARY
            DirectIngestStatus.READY_TO_FLASH,
        ],
    },
}


class DirectIngestInstanceStatusChangeListener:
    """An abstract class used to react to changes in instance statuses."""

    @abc.abstractmethod
    def on_raw_data_source_instance_change(
        self, raw_data_source_instance: Optional[DirectIngestInstance]
    ) -> None:
        """Reacts to raw data source instance change."""

    @abc.abstractmethod
    def on_ingest_instance_status_change(
        self, previous_status: DirectIngestStatus, new_status: DirectIngestStatus
    ) -> None:
        """Called whenever a status changes for this instance."""


class DirectIngestInstanceStatusManager:
    """An interface for reading and updating DirectIngestInstanceStatuses. For a
    visualization of valid instance statuses transitions, please refer to
    http://go/ingest-instance-status-flow.
    """

    def __init__(self, region_code: str, ingest_instance: DirectIngestInstance):
        self.region_code = region_code.upper()
        self.ingest_instance = ingest_instance

    @abc.abstractmethod
    def get_raw_data_source_instance(self) -> Optional[DirectIngestInstance]:
        """Returns the current raw data source of the ingest instance associated with
        this status manager.
        """

    @abc.abstractmethod
    def change_status_to(self, new_status: DirectIngestStatus) -> None:
        """Change status to the passed in status."""

    @abc.abstractmethod
    def get_current_status(self) -> DirectIngestStatus:
        """Get current status."""

    @abc.abstractmethod
    def get_current_ingest_rerun_start_timestamp(self) -> Optional[datetime.datetime]:
        """Gets the timestamp of the current rerun's start status."""

    @abc.abstractmethod
    def get_current_status_info(self) -> DirectIngestInstanceStatus:
        """Get current status and associated information."""

    @environment.test_only
    @abc.abstractmethod
    def add_instance_status(self, status: DirectIngestStatus) -> None:
        """Add a status (without any validations). Used for testing purposes."""

    @environment.test_only
    @abc.abstractmethod
    def get_all_statuses(self) -> List[DirectIngestInstanceStatus]:
        """Returns all statuses. Used for testing purposes."""

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

        if current_status is None:
            raise ValueError(
                f"[{self.region_code}][{self.ingest_instance.value}] Initial statuses for a state must be set via a "
                "migration. There should always be a current row for ingest instance statuses."
            )

        if (
            current_status == new_status
            and new_status not in HUMAN_INTERVENTION_STATUSES[ingest_instance]
        ):
            # You can always transition to the same status for statuses that do not involve human intervention
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
