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

# Direct ingest statuses that are only valid in a pre-ingest in Dataflow world.
LEGACY_PRE_DATAFLOW_STATUSES = [
    DirectIngestStatus.STANDARD_RERUN_STARTED,
    DirectIngestStatus.BLOCKED_ON_PRIMARY_RAW_DATA_IMPORT,
    DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
    DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
    DirectIngestStatus.UP_TO_DATE,
    DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
    DirectIngestStatus.RERUN_CANCELLATION_IN_PROGRESS,
    DirectIngestStatus.RERUN_CANCELED,
    DirectIngestStatus.NO_RERUN_IN_PROGRESS,
]

NEW_POST_DATAFLOW_STATUSES = [
    DirectIngestStatus.INITIAL_STATE,
    DirectIngestStatus.RAW_DATA_UP_TO_DATE,
    DirectIngestStatus.RAW_DATA_REIMPORT_IMPORT_STARTED,
    DirectIngestStatus.RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS,
    DirectIngestStatus.RAW_DATA_REIMPORT_CANCELED,
    DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
]


def get_invalid_statuses(
    ingest_instance: DirectIngestInstance, is_ingest_in_dataflow_enabled: bool
) -> List[DirectIngestStatus]:
    """Returns a list of statuses that are never valid in the context of a given
    instance.
    """
    if not is_ingest_in_dataflow_enabled:
        if ingest_instance is DirectIngestInstance.PRIMARY:
            return [
                *NEW_POST_DATAFLOW_STATUSES,
                DirectIngestStatus.READY_TO_FLASH,
                DirectIngestStatus.RERUN_CANCELLATION_IN_PROGRESS,
                DirectIngestStatus.RERUN_CANCELED,
                DirectIngestStatus.STALE_RAW_DATA,
                DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
                DirectIngestStatus.NO_RERUN_IN_PROGRESS,
                DirectIngestStatus.BLOCKED_ON_PRIMARY_RAW_DATA_IMPORT,
            ]

        if ingest_instance is DirectIngestInstance.SECONDARY:
            return [
                *NEW_POST_DATAFLOW_STATUSES,
                DirectIngestStatus.UP_TO_DATE,
            ]

    if ingest_instance == DirectIngestInstance.PRIMARY:
        return [
            *LEGACY_PRE_DATAFLOW_STATUSES,
            DirectIngestStatus.READY_TO_FLASH,
            DirectIngestStatus.RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS,
            DirectIngestStatus.RAW_DATA_REIMPORT_CANCELED,
            DirectIngestStatus.STALE_RAW_DATA,
            DirectIngestStatus.RAW_DATA_REIMPORT_IMPORT_STARTED,
            DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
        ]

    if ingest_instance == DirectIngestInstance.SECONDARY:
        return [
            *LEGACY_PRE_DATAFLOW_STATUSES,
            DirectIngestStatus.INITIAL_STATE,
            DirectIngestStatus.RAW_DATA_UP_TO_DATE,
        ]

    raise ValueError(
        f"Unsupported args: {ingest_instance=}, {is_ingest_in_dataflow_enabled=}"
    )


def get_human_intervention_statuses(
    ingest_instance: DirectIngestInstance, is_ingest_in_dataflow_enabled: bool
) -> List[DirectIngestStatus]:
    """Returns a list of statuses that an instance can only transition to via some human
    intervention.
    """
    if not is_ingest_in_dataflow_enabled:
        if ingest_instance is DirectIngestInstance.PRIMARY:
            return [
                # Note: `STANDARD_RERUN_STARTED` is automatically added an initial
                # status via a migration
                DirectIngestStatus.FLASH_IN_PROGRESS,
            ]

        if ingest_instance is DirectIngestInstance.SECONDARY:
            return [
                DirectIngestStatus.RERUN_CANCELLATION_IN_PROGRESS,
                DirectIngestStatus.RERUN_CANCELED,
                DirectIngestStatus.FLASH_IN_PROGRESS,
                DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
                DirectIngestStatus.STANDARD_RERUN_STARTED,
            ]

    if ingest_instance is DirectIngestInstance.PRIMARY:
        return [
            DirectIngestStatus.FLASH_IN_PROGRESS,
        ]

    if ingest_instance is DirectIngestInstance.SECONDARY:
        return [
            DirectIngestStatus.RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS,
            DirectIngestStatus.RAW_DATA_REIMPORT_CANCELED,
            DirectIngestStatus.FLASH_IN_PROGRESS,
            DirectIngestStatus.RAW_DATA_REIMPORT_IMPORT_STARTED,
        ]

    raise ValueError(
        f"Unsupported args: {ingest_instance=}, {is_ingest_in_dataflow_enabled=}"
    )


# TODO(#20930): Delete this once ingest in Dataflow is shipped to all states
# Valid status transitions that are applicable to both PRIMARY and SECONDARY.
LEGACY_SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS: Dict[
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


def get_valid_current_status_transitions(
    ingest_instance: DirectIngestInstance, is_ingest_in_dataflow_enabled: bool
) -> Dict[DirectIngestStatus, List[DirectIngestStatus]]:
    """Returns a dictionary representing all valid status transitions, where the keys
    are a status and the values are the list of all statuses that can precede that
    status.
    """
    if not is_ingest_in_dataflow_enabled:
        if ingest_instance is DirectIngestInstance.PRIMARY:
            return {
                DirectIngestStatus.STANDARD_RERUN_STARTED: [
                    DirectIngestStatus.UP_TO_DATE,
                    DirectIngestStatus.FLASH_COMPLETED,
                ],
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS: [
                    *LEGACY_SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
                        DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS
                    ],
                    DirectIngestStatus.FLASH_COMPLETED,
                    DirectIngestStatus.UP_TO_DATE,
                    DirectIngestStatus.STANDARD_RERUN_STARTED,
                ],
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS: LEGACY_SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
                    DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS
                ],
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS: [
                    *LEGACY_SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
                        DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS
                    ],
                    DirectIngestStatus.UP_TO_DATE,
                ],
                DirectIngestStatus.FLASH_COMPLETED: LEGACY_SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
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
                    if s
                    not in get_invalid_statuses(
                        DirectIngestInstance.PRIMARY, is_ingest_in_dataflow_enabled
                    )
                ],
            }
        if ingest_instance is DirectIngestInstance.SECONDARY:
            return {
                DirectIngestStatus.STANDARD_RERUN_STARTED: [
                    DirectIngestStatus.NO_RERUN_IN_PROGRESS
                ],
                DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED: [
                    DirectIngestStatus.NO_RERUN_IN_PROGRESS,
                ],
                # SECONDARY specific check for FLASH_IN_PROGRESS.
                DirectIngestStatus.FLASH_IN_PROGRESS: [
                    DirectIngestStatus.READY_TO_FLASH
                ],
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS: [
                    *LEGACY_SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
                        DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS
                    ],
                    DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
                    DirectIngestStatus.STALE_RAW_DATA,
                    # We would transition from this status if we process new files in SECONDARY
                    # (that were not seen in PRIMARY)
                    DirectIngestStatus.READY_TO_FLASH,
                ],
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS: [
                    *LEGACY_SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
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
                DirectIngestStatus.RERUN_CANCELED: [
                    DirectIngestStatus.RERUN_CANCELLATION_IN_PROGRESS,
                ],
                DirectIngestStatus.RERUN_CANCELLATION_IN_PROGRESS:
                # ANY valid secondary status is allowed before proceeding with a flash cancellation
                list(
                    (set(DirectIngestStatus))
                    - set(
                        get_invalid_statuses(
                            DirectIngestInstance.SECONDARY,
                            is_ingest_in_dataflow_enabled,
                        )
                    )
                ),
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS: [
                    *LEGACY_SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
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
                DirectIngestStatus.FLASH_COMPLETED: LEGACY_SHARED_VALID_PREVIOUS_STATUS_TRANSITIONS[
                    DirectIngestStatus.FLASH_COMPLETED
                ],
                DirectIngestStatus.NO_RERUN_IN_PROGRESS: [
                    DirectIngestStatus.RERUN_CANCELED,
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
            }

    if ingest_instance is DirectIngestInstance.PRIMARY:
        return {
            DirectIngestStatus.INITIAL_STATE: [],
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS: [
                DirectIngestStatus.FLASH_COMPLETED,
                DirectIngestStatus.RAW_DATA_UP_TO_DATE,
                DirectIngestStatus.INITIAL_STATE,
            ],
            DirectIngestStatus.FLASH_COMPLETED: [DirectIngestStatus.FLASH_IN_PROGRESS],
            # PRIMARY ONLY status.
            DirectIngestStatus.RAW_DATA_UP_TO_DATE: [
                DirectIngestStatus.FLASH_COMPLETED,
                DirectIngestStatus.INITIAL_STATE,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
            ],
            # PRIMARY specific check for FLASH_IN_PROGRESS. Flashing could start when
            # PRIMARY is in any state - it doesn't matter because everything in PRIMARY
            # will be overwritten.
            DirectIngestStatus.FLASH_IN_PROGRESS: [
                s
                for s in list(DirectIngestStatus)
                if s
                not in get_invalid_statuses(
                    DirectIngestInstance.PRIMARY, is_ingest_in_dataflow_enabled
                )
            ],
        }
    if ingest_instance is DirectIngestInstance.SECONDARY:
        return {
            DirectIngestStatus.RAW_DATA_REIMPORT_IMPORT_STARTED: [
                DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
            ],
            # SECONDARY specific check for FLASH_IN_PROGRESS.
            DirectIngestStatus.FLASH_IN_PROGRESS: [DirectIngestStatus.READY_TO_FLASH],
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS: [
                DirectIngestStatus.RAW_DATA_REIMPORT_IMPORT_STARTED,
                DirectIngestStatus.STALE_RAW_DATA,
                DirectIngestStatus.READY_TO_FLASH,
            ],
            DirectIngestStatus.READY_TO_FLASH: [
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
            ],
            DirectIngestStatus.RAW_DATA_REIMPORT_CANCELED: [
                DirectIngestStatus.RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS,
            ],
            DirectIngestStatus.RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS:
            # ANY valid secondary status is allowed before proceeding with a flash cancellation
            list(
                (set(DirectIngestStatus))
                - set(
                    get_invalid_statuses(
                        DirectIngestInstance.SECONDARY, is_ingest_in_dataflow_enabled
                    )
                )
            ),
            DirectIngestStatus.STALE_RAW_DATA: [
                DirectIngestStatus.RAW_DATA_REIMPORT_IMPORT_STARTED,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.READY_TO_FLASH,
            ],
            DirectIngestStatus.FLASH_COMPLETED: [DirectIngestStatus.FLASH_IN_PROGRESS],
            DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS: [
                DirectIngestStatus.RAW_DATA_REIMPORT_CANCELED,
                DirectIngestStatus.FLASH_COMPLETED,
            ],
        }

    raise ValueError(
        f"Unsupported args: {ingest_instance=}, {is_ingest_in_dataflow_enabled=}"
    )


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

    def __init__(
        self,
        region_code: str,
        ingest_instance: DirectIngestInstance,
        is_ingest_in_dataflow_enabled: bool,
    ):
        self.region_code = region_code.upper()
        self.ingest_instance = ingest_instance
        self.is_ingest_in_dataflow_enabled = is_ingest_in_dataflow_enabled

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

    @abc.abstractmethod
    def get_statuses_since(
        self, start_timestamp: datetime.datetime
    ) -> List[DirectIngestInstanceStatus]:
        """Returns all of the statuses since the given timestamp (exclusive), with the
        most recent first."""

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
        if new_status in get_invalid_statuses(
            ingest_instance, self.is_ingest_in_dataflow_enabled
        ):
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
            and new_status
            not in get_human_intervention_statuses(
                ingest_instance, self.is_ingest_in_dataflow_enabled
            )
        ):
            # You can always transition to the same status for statuses that do not involve human intervention
            return

        valid_current_statuses = get_valid_current_status_transitions(
            ingest_instance, self.is_ingest_in_dataflow_enabled
        )[new_status]

        if current_status not in valid_current_statuses:
            current_status_str = current_status.value if current_status else None
            raise ValueError(
                f"[{self.region_code}][{self.ingest_instance}] Can only transition from the following statuses to "
                f"{new_status.value}: {[status.value for status in valid_current_statuses]}. Current status is "
                f"{current_status_str}."
            )
