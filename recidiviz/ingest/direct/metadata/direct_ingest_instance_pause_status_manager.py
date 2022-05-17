# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""A class that manages reading and updating DirectIngestInstancePauseStatuses."""
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations.schema import (
    DirectIngestInstancePauseStatus,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils import environment


class DirectIngestInstancePauseStatusManager:
    """An interface for reading and updating DirectIngestInstancePauseStatuses."""

    def __init__(self, region_code: str, ingest_instance: DirectIngestInstance):
        self.region_code = region_code.upper()
        self.ingest_instance = ingest_instance

        self.db_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

    def _get_status_using_session(
        self, session: Session
    ) -> DirectIngestInstancePauseStatus:
        return (
            session.query(DirectIngestInstancePauseStatus)
            .filter(
                DirectIngestInstancePauseStatus.region_code == self.region_code,
                DirectIngestInstancePauseStatus.instance == self.ingest_instance.value,
            )
            .one()
        )

    def is_instance_paused(self) -> bool:
        with SessionFactory.using_database(self.db_key, autocommit=False) as session:
            return self._get_status_using_session(session).is_paused

    def pause_instance(self) -> None:
        with SessionFactory.using_database(self.db_key) as session:
            status = self._get_status_using_session(session)
            status.is_paused = True

    def unpause_instance(self) -> None:
        with SessionFactory.using_database(self.db_key) as session:
            status = self._get_status_using_session(session)
            status.is_paused = False

    # This one specifically for test setup!
    @staticmethod
    @environment.test_only
    def add_instance(
        region_code: str, ingest_instance: DirectIngestInstance, is_paused: bool
    ) -> "DirectIngestInstancePauseStatusManager":
        with SessionFactory.using_database(
            SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        ) as session:
            session.add(
                DirectIngestInstancePauseStatus(
                    region_code=region_code.upper(),
                    instance=ingest_instance.value,
                    is_paused=is_paused,
                )
            )

        return DirectIngestInstancePauseStatusManager(region_code, ingest_instance)
