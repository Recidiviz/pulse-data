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
"""Class for managing reads and writes to DirectIngestRawDataFlashStatus"""
import datetime

from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations import entities
from recidiviz.utils import environment


class DirectIngestRawDataFlashStatusManager:
    """Class for managing reads and writes to DirectIngestRawDataFlashStatus"""

    def __init__(
        self,
        region_code: str,
    ) -> None:
        self.region_code = region_code.upper()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

    def _get_most_recent_flashing_row(
        self, session: Session
    ) -> schema.DirectIngestRawDataFlashStatus:
        return (
            session.query(schema.DirectIngestRawDataFlashStatus)
            .order_by(schema.DirectIngestRawDataFlashStatus.status_timestamp.desc())
            .limit(1)
            .one()
        )

    def is_flashing_in_progress(self) -> bool:
        """Indicates whether or not flashing is currently in progress."""
        with SessionFactory.using_database(self.database_key) as session:
            return self._get_most_recent_flashing_row(session).flashing_in_progress

    def get_flashing_status(self) -> entities.DirectIngestRawDataFlashStatus:
        """Returns the most recent flashing status row."""
        with SessionFactory.using_database(self.database_key) as session:
            return convert_schema_object_to_entity(
                self._get_most_recent_flashing_row(session),
                entities.DirectIngestRawDataFlashStatus,
            )

    def set_flashing_started(self) -> None:
        """Adds a new status row to the DirectIngestRawDataFlashStatus table with
        flashing_in_progress=True. If the flashing_in_progress column in the current
        status row is already set to True, no new row is added.
        """
        self._add_flashing_status_row(True)

    def set_flashing_finished(self) -> None:
        """Adds a new status row to the DirectIngestRawDataFlashStatus table with
        flashing_in_progress=False.  If the flashing_in_progress column in the current
        status row is already set to False, no new row is added.
        """
        self._add_flashing_status_row(False)

    def _add_flashing_status_row(self, flashing_in_progress: bool) -> None:
        """Adds a new status row to the DirectIngestRawDataFlashStatus table with
        |flashing_in_progress| indicating whether or not flashing is in progress. If
        |flashing_in_progress| is the same as the current status row's flashing_in_progress
        value, no new row is added.
        """
        if self.is_flashing_in_progress() == flashing_in_progress:
            return

        with SessionFactory.using_database(self.database_key) as session:
            new_status_row = schema.DirectIngestRawDataFlashStatus(
                region_code=self.region_code,
                status_timestamp=datetime.datetime.now(tz=datetime.UTC),
                flashing_in_progress=flashing_in_progress,
            )

            session.add(new_status_row)

    @environment.test_only
    def add_initial_status(self, status_timestamp: datetime.datetime) -> None:
        """Seeds the DB with the initial status that would normally be set via a
        migration in deployed environments.
        """
        with SessionFactory.using_database(self.database_key) as session:
            new_status_row = schema.DirectIngestRawDataFlashStatus(
                region_code=self.region_code,
                status_timestamp=status_timestamp,
                flashing_in_progress=False,
            )

            session.add(new_status_row)
