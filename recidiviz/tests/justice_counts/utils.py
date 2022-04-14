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
"""Implements tests for the Justice Counts Control Panel backend API."""

import datetime
from typing import Optional
from unittest import TestCase

import pytest
from sqlalchemy.engine import Engine

from recidiviz.persistence.database.schema.justice_counts.schema import (
    AcquisitionMethod,
    Agency,
    Project,
    Report,
    ReportStatus,
    UserAccount,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.postgres import local_postgres_helpers


@pytest.mark.uses_db
class JusticeCountsDatabaseTestCase(TestCase):
    """Base class for unit tests that act on the Justice Counts database."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS)
        self.env_vars = (
            local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()
        )

        # Auto-generate all tables that exist in our schema in this database
        self.engine = self.get_engine()
        self.database_key.declarative_meta.metadata.create_all(self.engine)

    def get_engine(self) -> Engine:
        """Return the Engine that this test class should use to connect to
        the database. By default, initialize a new engine. Subclasses can
        override this method to point to an engine that already exists."""
        return SQLAlchemyEngineManager.init_engine_for_postgres_instance(
            database_key=self.database_key,
            db_url=local_postgres_helpers.on_disk_postgres_db_url(),
        )

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)
        local_postgres_helpers.restore_local_env_vars(self.env_vars)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )


class JusticeCountsSchemaTestObjects:
    """Class for test schema objects"""

    def __init__(self) -> None:
        self.test_agency_A = Agency(name="Agency Alpha")
        self.test_agency_B = Agency(name="Agency Beta")
        self.test_user_A = UserAccount(
            name="Jane Doe",
            auth0_user_id="auth0_id_A",
            email_address="user@gmail.com",
            agencies=[self.test_agency_A],
        )
        self.test_user_B = UserAccount(
            name="John Doe",
            email_address="user@email.gov",
            auth0_user_id="auth0_id_B",
            agencies=[self.test_agency_B],
        )
        self.test_report_monthly = Report(
            source=self.test_agency_A,
            type="MONTHLY",
            instance="generated_instance_id",
            status=ReportStatus.NOT_STARTED,
            date_range_start=datetime.date.fromisoformat("2022-06-01"),
            date_range_end=datetime.date.fromisoformat("2022-07-01"),
            project=Project.JUSTICE_COUNTS_CONTROL_PANEL,
            acquisition_method=AcquisitionMethod.CONTROL_PANEL,
            created_at=datetime.date.fromisoformat("2022-05-30"),
        )
        self.test_report_annual = Report(
            source=self.test_agency_B,
            type="ANNUAL",
            instance="generated_instance_id",
            status=ReportStatus.DRAFT,
            date_range_start=datetime.date.fromisoformat("2022-01-01"),
            date_range_end=datetime.date.fromisoformat("2023-01-01"),
            modified_by=[self.test_user_B.id],
            project=Project.JUSTICE_COUNTS_CONTROL_PANEL,
            acquisition_method=AcquisitionMethod.CONTROL_PANEL,
            last_modified_at=datetime.datetime.fromisoformat("2022-07-05T08:00:00"),
            created_at=datetime.date.fromisoformat("2021-12-30"),
        )
