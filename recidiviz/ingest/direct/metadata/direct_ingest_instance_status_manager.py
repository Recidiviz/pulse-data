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
"""A class that manages reading and updating DirectIngestInstanceStatuses. For a visualization of valid instance
statuses transitions, please refer to http://go/ingest-instance-status-flow."""
from datetime import datetime
from typing import Dict, List, Optional

import pytz
from more_itertools import one

from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import DirectIngestInstanceStatus
from recidiviz.utils import environment


def get_invalid_statuses(
    ingest_instance: DirectIngestInstance,
) -> List[DirectIngestStatus]:
    """Returns a list of statuses that are never valid in the context of a given
    instance.
    """
    if ingest_instance == DirectIngestInstance.PRIMARY:
        return [
            DirectIngestStatus.READY_TO_FLASH,
            DirectIngestStatus.RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS,
            DirectIngestStatus.RAW_DATA_REIMPORT_CANCELED,
            DirectIngestStatus.STALE_RAW_DATA,
            DirectIngestStatus.RAW_DATA_REIMPORT_STARTED,
            DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
        ]

    if ingest_instance == DirectIngestInstance.SECONDARY:
        return [
            DirectIngestStatus.INITIAL_STATE,
            DirectIngestStatus.RAW_DATA_UP_TO_DATE,
        ]

    raise ValueError(f"Unsupported args: {ingest_instance=}")


def get_human_intervention_statuses(
    ingest_instance: DirectIngestInstance,
) -> List[DirectIngestStatus]:
    """Returns a list of statuses that an instance can only transition to via some human
    intervention.
    """
    if ingest_instance is DirectIngestInstance.PRIMARY:
        return [
            DirectIngestStatus.FLASH_IN_PROGRESS,
        ]

    if ingest_instance is DirectIngestInstance.SECONDARY:
        return [
            DirectIngestStatus.RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS,
            DirectIngestStatus.RAW_DATA_REIMPORT_CANCELED,
            DirectIngestStatus.FLASH_IN_PROGRESS,
            DirectIngestStatus.RAW_DATA_REIMPORT_STARTED,
        ]

    raise ValueError(f"Unsupported args: {ingest_instance=}")


def get_valid_current_status_transitions(
    ingest_instance: DirectIngestInstance,
) -> Dict[DirectIngestStatus, List[DirectIngestStatus]]:
    """Returns a dictionary representing all valid status transitions, where the keys
    are a status and the values are the list of all statuses that can precede that
    status.
    """
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
                if s not in get_invalid_statuses(DirectIngestInstance.PRIMARY)
            ],
        }
    if ingest_instance is DirectIngestInstance.SECONDARY:
        return {
            DirectIngestStatus.RAW_DATA_REIMPORT_STARTED: [
                DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
            ],
            # SECONDARY specific check for FLASH_IN_PROGRESS.
            DirectIngestStatus.FLASH_IN_PROGRESS: [DirectIngestStatus.READY_TO_FLASH],
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS: [
                DirectIngestStatus.RAW_DATA_REIMPORT_STARTED,
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
                - set(get_invalid_statuses(DirectIngestInstance.SECONDARY))
            ),
            DirectIngestStatus.STALE_RAW_DATA: [
                DirectIngestStatus.RAW_DATA_REIMPORT_STARTED,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.READY_TO_FLASH,
            ],
            DirectIngestStatus.FLASH_COMPLETED: [DirectIngestStatus.FLASH_IN_PROGRESS],
            DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS: [
                DirectIngestStatus.RAW_DATA_REIMPORT_CANCELED,
                DirectIngestStatus.FLASH_COMPLETED,
            ],
        }

    raise ValueError(f"Unsupported args: {ingest_instance=}")


def get_initial_status_for_instance(
    instance: DirectIngestInstance,
) -> DirectIngestStatus:
    return (
        DirectIngestStatus.INITIAL_STATE
        if instance is DirectIngestInstance.PRIMARY
        else DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS
    )


class DirectIngestInstanceStatusManager:
    """An interface for reading and updating DirectIngestInstanceStatuses. For a
    visualization of valid instance statuses transitions, please refer to
    http://go/ingest-instance-status-flow.
    """

    def __init__(self, region_code: str, ingest_instance: DirectIngestInstance):
        self.region_code = region_code.upper()
        self.ingest_instance = ingest_instance
        self.db_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

    def _get_current_status_row(self, session: Session) -> DirectIngestInstanceStatus:
        """Returns the most recent status row for this instance."""
        results = (
            session.query(schema.DirectIngestInstanceStatus)
            .filter_by(
                region_code=self.region_code,
                instance=self.ingest_instance.value,
            )
            .order_by(schema.DirectIngestInstanceStatus.status_timestamp.desc())
            .limit(1)
            .one_or_none()
        )
        if not results:
            raise ValueError(
                f"[{self.region_code}][{self.ingest_instance.value}] Initial statuses for a state must be set via a "
                "migration. There should always be a current row for ingest instance statuses."
            )

        return convert_schema_object_to_entity(results, DirectIngestInstanceStatus)

    def _get_most_recent_row_with_status(
        self, session: Session, status: DirectIngestStatus
    ) -> Optional[DirectIngestInstanceStatus]:
        """Returns the most recent row of a particular status, if present."""
        results = (
            session.query(schema.DirectIngestInstanceStatus)
            .filter_by(
                region_code=self.region_code,
                instance=self.ingest_instance.value,
                status=status.value,
            )
            .order_by(schema.DirectIngestInstanceStatus.status_timestamp.desc())
            .limit(1)
            .one_or_none()
        )

        if results:
            return convert_schema_object_to_entity(results, DirectIngestInstanceStatus)

        return None

    def _get_rows_after_timestamp(
        self,
        session: Session,
        status_timestamp: datetime,
    ) -> List[DirectIngestInstanceStatus]:
        """Returns all rows, if any, whose timestamps are strictly after the passed in
        status_timestamp.

        If `status_filter` is provided, then only return rows for the given status.
        """
        query = session.query(schema.DirectIngestInstanceStatus).filter(
            schema.DirectIngestInstanceStatus.region_code == self.region_code,
            schema.DirectIngestInstanceStatus.instance == self.ingest_instance.value,
            schema.DirectIngestInstanceStatus.status_timestamp > status_timestamp,
        )

        results = query.order_by(
            schema.DirectIngestInstanceStatus.status_timestamp.desc()
        ).all()

        return [
            convert_schema_object_to_entity(result, DirectIngestInstanceStatus)
            for result in results
        ]

    def _add_new_status_row(self, status: DirectIngestStatus) -> DirectIngestStatus:
        """Add new row with the passed in status. Returns the previous status."""
        with SessionFactory.using_database(self.db_key) as session:
            current_status = self._get_current_status_row(session)
            if current_status.status != status:
                new_row = schema.DirectIngestInstanceStatus(
                    region_code=self.region_code,
                    instance=self.ingest_instance.value,
                    status_timestamp=datetime.now(tz=pytz.UTC),
                    status=status.value,
                )
                session.add(new_row)
        return current_status.status

    def _validate_status_transition_from_current_status(
        self, new_status: DirectIngestStatus
    ) -> None:
        """Validate that a transition to `new_status` is feasible."""
        with SessionFactory.using_database(self.db_key) as session:
            current_status = self._get_current_status_row(session=session)
            self.validate_transition(
                ingest_instance=self.ingest_instance,
                current_status=(current_status.status if current_status else None),
                new_status=new_status,
            )

    def _get_status_rows_of_current_raw_data_reimport(
        self, session: Session
    ) -> Optional[List[DirectIngestInstanceStatus]]:
        """Returns all the rows associated with a current raw data re-import, if
        applicable.
        """
        if self.ingest_instance == DirectIngestInstance.PRIMARY:
            raise ValueError(
                "We should not be doing a raw data reimport for the PRIMARY instance."
            )

        most_recent_completed = self._get_most_recent_row_with_status(
            session=session,
            status=DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
        )
        if most_recent_completed:
            current_rerun_status_rows: List[
                DirectIngestInstanceStatus
            ] = self._get_rows_after_timestamp(
                session=session,
                status_timestamp=most_recent_completed.status_timestamp,
            )
            return current_rerun_status_rows

        # If there isn't yet a completed rerun, return all status rows.
        return self._get_rows_after_timestamp(
            session=session, status_timestamp=datetime.min
        )

    def _get_current_raw_data_reimport_start_status(
        self, query_session: Session
    ) -> Optional[DirectIngestInstanceStatus]:
        """Returns the status that indicates the start of a rerun / raw data reimport."""
        if self.ingest_instance is not DirectIngestInstance.SECONDARY:
            raise ValueError(
                "A reimport is not a valid concept for PRIMARY instances. This should "
                "not be called."
            )

        current_rerun_status_rows = self._get_status_rows_of_current_raw_data_reimport(
            session=query_session
        )
        if not current_rerun_status_rows:
            return None

        valid_start_statuses = [DirectIngestStatus.RAW_DATA_REIMPORT_STARTED]

        return one(
            row
            for row in current_rerun_status_rows
            if row.status in valid_start_statuses
        )

    def get_current_ingest_rerun_start_timestamp(self) -> Optional[datetime]:
        """Gets the timestamp of the current rerun's start status."""
        if self.ingest_instance is not DirectIngestInstance.SECONDARY:
            raise ValueError(
                "A reimport is not a valid concept for PRIMARY instances. This should "
                "not be called."
            )

        with SessionFactory.using_database(self.db_key) as session:
            current_rerun_start_status = (
                self._get_current_raw_data_reimport_start_status(session)
            )
            if not current_rerun_start_status:
                # Check for current status - this will throw if there isn't one set
                self._get_current_status_row(session)
                return None
            return current_rerun_start_status.status_timestamp

    def change_status_to(self, new_status: DirectIngestStatus) -> None:
        """Change status to the passed in status."""
        self._validate_status_transition_from_current_status(new_status=new_status)
        self._add_new_status_row(status=new_status)

    @environment.test_only
    def add_initial_status(self) -> None:
        """Seeds the DB with the expected initial status that would normally be set
        via a migration in production environments.
        """
        self.add_instance_status(get_initial_status_for_instance(self.ingest_instance))

    @environment.test_only
    def add_instance_status(
        self,
        status: DirectIngestStatus,
    ) -> None:
        """Add a status (without any validations). Used for testing purposes."""

        if status in get_invalid_statuses(self.ingest_instance):
            raise ValueError(
                f"Cannot add invalid status [{status.value}] for instance "
                f"[{self.ingest_instance.value}]."
            )

        with SessionFactory.using_database(self.db_key) as session:
            new_row = schema.DirectIngestInstanceStatus(
                region_code=self.region_code,
                instance=self.ingest_instance.value,
                status_timestamp=datetime.now(tz=pytz.UTC),
                status=status.value,
            )
            session.add(new_row)

    @environment.test_only
    def get_all_statuses(self) -> List[DirectIngestInstanceStatus]:
        """Returns all statuses. Used for testing purposes."""
        with SessionFactory.using_database(self.db_key) as session:
            return self._get_rows_after_timestamp(session, datetime.min)

    def get_statuses_since(
        self, start_timestamp: datetime
    ) -> List[DirectIngestInstanceStatus]:
        """Returns all of the statuses since the given timestamp (exclusive), with the
        most recent first."""
        with SessionFactory.using_database(self.db_key) as session:
            return self._get_rows_after_timestamp(
                session,
                status_timestamp=start_timestamp,
            )

    def get_current_status(self) -> DirectIngestStatus:
        """Get current status."""
        with SessionFactory.using_database(self.db_key) as session:
            status_row: DirectIngestInstanceStatus = self._get_current_status_row(
                session
            )
            return status_row.status

    def get_current_status_info(self) -> DirectIngestInstanceStatus:
        """Get current status and associated information."""
        with SessionFactory.using_database(self.db_key) as session:
            return self._get_current_status_row(session)

    def validate_transition(
        self,
        ingest_instance: DirectIngestInstance,
        current_status: Optional[DirectIngestStatus],
        new_status: DirectIngestStatus,
    ) -> None:
        """Validates that the transition from the current status to the new status is
        valid and throws a ValueError if it is not.
        """
        if new_status in get_invalid_statuses(ingest_instance):
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
            and new_status not in get_human_intervention_statuses(ingest_instance)
        ):
            # You can always transition to the same status for statuses that do not involve human intervention
            return

        valid_current_statuses = get_valid_current_status_transitions(ingest_instance)[
            new_status
        ]

        if current_status not in valid_current_statuses:
            current_status_str = current_status.value if current_status else None
            raise ValueError(
                f"[{self.region_code}][{self.ingest_instance}] Can only transition from the following statuses to "
                f"{new_status.value}: {[status.value for status in valid_current_statuses]}. Current status is "
                f"{current_status_str}."
            )
