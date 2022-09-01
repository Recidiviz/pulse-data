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
from typing import List, Optional

from more_itertools import one

from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    VALID_INITIAL_STATUSES,
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import DirectIngestInstanceStatus
from recidiviz.utils import environment


class PostgresDirectIngestInstanceStatusManager(DirectIngestInstanceStatusManager):
    """An interface for reading and updating DirectIngestInstanceStatuses. For a
    visualization of valid instance statuses transitions, please refer to
    http://go/ingest-instance-status-flow.
    """

    def __init__(self, region_code: str, ingest_instance: DirectIngestInstance):
        super().__init__(region_code=region_code, ingest_instance=ingest_instance)
        self.db_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

    @staticmethod
    def _direct_ingest_instance_status_as_entity(
        schema_metadata: schema.DirectIngestInstanceStatus,
    ) -> DirectIngestInstanceStatus:
        entity_metadata = convert_schema_object_to_entity(schema_metadata)

        if not isinstance(entity_metadata, DirectIngestInstanceStatus):
            raise ValueError(
                f"Unexpected metadata entity type: {type(entity_metadata)}"
            )

        return entity_metadata

    def _get_current_status_row(
        self, session: Session
    ) -> Optional[DirectIngestInstanceStatus]:
        """Returns the most recent status row for this instance (if present)."""
        results = (
            session.query(schema.DirectIngestInstanceStatus)
            .filter_by(
                region_code=self.region_code,
                instance=self.ingest_instance.value,
            )
            .order_by(schema.DirectIngestInstanceStatus.timestamp.desc())
            .limit(1)
            .one_or_none()
        )

        if results:
            return self._direct_ingest_instance_status_as_entity(results)

        return None

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
            .order_by(schema.DirectIngestInstanceStatus.timestamp.desc())
            .limit(1)
            .one_or_none()
        )

        if results:
            return self._direct_ingest_instance_status_as_entity(results)

        return None

    def _get_rows_after_timestamp(
        self, session: Session, timestamp: datetime
    ) -> List[DirectIngestInstanceStatus]:
        """Returns all rows, if any, whose timestamps are strictly after the passed in
        timestamp.
        """
        results = (
            session.query(schema.DirectIngestInstanceStatus)
            .filter(
                schema.DirectIngestInstanceStatus.region_code == self.region_code,
                schema.DirectIngestInstanceStatus.instance
                == self.ingest_instance.value,
                schema.DirectIngestInstanceStatus.timestamp > timestamp,
            )
            .order_by(schema.DirectIngestInstanceStatus.timestamp.desc())
            .all()
        )

        return [
            self._direct_ingest_instance_status_as_entity(result) for result in results
        ]

    def _add_new_status_row(self, status: DirectIngestStatus) -> None:
        """Add new row with the passed in status."""
        with SessionFactory.using_database(self.db_key) as session:
            current_status = self._get_current_status_row(session)
            if not current_status or current_status.status != status:
                new_row = schema.DirectIngestInstanceStatus(
                    region_code=self.region_code,
                    instance=self.ingest_instance.value,
                    timestamp=datetime.now(),
                    status=status.value,
                )
                session.add(new_row)

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

    def _get_status_rows_of_current_rerun(
        self, session: Session
    ) -> Optional[List[DirectIngestInstanceStatus]]:
        """Returns all the rows associated with a current rerun, if applicable."""
        most_recent_completed = self._get_most_recent_row_with_status(
            session=session,
            # Terminating status is UP_TO_DATE in PRIMARY and NO_RERUN_IN_PROGRESS in SECONDARY.
            status=DirectIngestStatus.UP_TO_DATE
            if self.ingest_instance == DirectIngestInstance.PRIMARY
            else DirectIngestStatus.NO_RERUN_IN_PROGRESS,
        )
        if most_recent_completed:
            current_rerun_status_rows: List[
                DirectIngestInstanceStatus
            ] = self._get_rows_after_timestamp(
                session=session, timestamp=most_recent_completed.timestamp
            )
            return current_rerun_status_rows

        # If there isn't yet a completed rerun, return all status rows.
        return self._get_rows_after_timestamp(session=session, timestamp=datetime.min)

    def get_raw_data_source_instance(self) -> DirectIngestInstance:
        """Returns the current raw data source of the ingest instance associated with
        this status manager.
        """
        with SessionFactory.using_database(self.db_key) as session:
            # Raw data source can only be PRIMARY for PRIMARY instances.
            if self.ingest_instance == DirectIngestInstance.PRIMARY:
                return DirectIngestInstance.PRIMARY

            # Raw data source can be PRIMARY or SECONDARY for SECONDARY instances,
            # depending on the configurations of the secondary rerun.
            current_rerun_status_rows = self._get_status_rows_of_current_rerun(
                session=session
            )
            if not current_rerun_status_rows:
                raise ValueError(
                    f"[{self.region_code}][{self.ingest_instance}] Expected rerun to be in progress."
                )

            current_rerun_start_status = one(
                row.status
                for row in current_rerun_status_rows
                if row.status in VALID_INITIAL_STATUSES[self.ingest_instance]
            )

            # If the rerun only involves regenerating and running ingest views, then the
            # raw data source is PRIMARY.
            if current_rerun_start_status == DirectIngestStatus.STANDARD_RERUN_STARTED:
                return DirectIngestInstance.PRIMARY

            # Otherwise, this means that the raw data source is SECONDARY.
            return DirectIngestInstance.SECONDARY

    def change_status_to(self, new_status: DirectIngestStatus) -> None:
        """Change status to the passed in status."""
        self._validate_status_transition_from_current_status(new_status=new_status)
        self._add_new_status_row(status=new_status)

    # This one specifically for test setup!
    @environment.test_only
    def add_instance_status(
        self,
        status: DirectIngestStatus,
    ) -> None:
        """Add a status (without any validations). Used for testing purposes."""
        self._add_new_status_row(status)

    @environment.test_only
    def get_all_statuses(self) -> List[DirectIngestInstanceStatus]:
        with SessionFactory.using_database(self.db_key) as session:
            return self._get_rows_after_timestamp(session, datetime.min)

    def get_current_status(self) -> Optional[DirectIngestStatus]:
        """Get current status."""
        with SessionFactory.using_database(self.db_key) as session:
            status_row: Optional[
                DirectIngestInstanceStatus
            ] = self._get_current_status_row(session)
            return status_row.status if status_row is not None else None

    def get_current_status_info(self) -> Optional[DirectIngestInstanceStatus]:
        """Get current status and associated information."""
        with SessionFactory.using_database(self.db_key) as session:
            return self._get_current_status_row(session)
