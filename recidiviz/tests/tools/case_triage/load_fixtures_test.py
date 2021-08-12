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
"""Implements tests for the load fixtures script."""
from typing import Optional
from unittest import TestCase

import pytest
from pytest_alembic import runner

from recidiviz.case_triage.views.view_config import ETL_TABLES
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.case_triage.load_fixtures import reset_case_triage_fixtures
from recidiviz.tools.postgres import local_postgres_helpers


@pytest.mark.uses_db
class TestLoadFixtures(TestCase):
    """Implements tests for the load fixtures script."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.db_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        self.env_vars = (
            local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()
        )

        # We need to build up the database using the migrations instead of
        # by just loading from the internal representation because the different
        # methods induce different orders.
        # The migration order is the one seen in staging/prod, as well as what
        # we do in development.
        engine = SQLAlchemyEngineManager.init_engine_for_postgres_instance(
            database_key=self.db_key,
            db_url=local_postgres_helpers.on_disk_postgres_db_url(),
        )
        with runner(
            {
                "file": self.db_key.alembic_file,
                "script_location": self.db_key.migrations_location,
            },
            engine,
        ) as r:
            r.migrate_up_to("head")

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.db_key)
        local_postgres_helpers.restore_local_env_vars(self.env_vars)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_load_fixtures_succeeds(self) -> None:
        # First order of business, this shouldn't crash.
        reset_case_triage_fixtures()

        # Make sure values are actually written to the tables we know about.
        with SessionFactory.using_database(
            self.db_key, autocommit=False
        ) as read_session:
            for fixture_class in ETL_TABLES:
                self.assertTrue(len(read_session.query(fixture_class).all()) > 0)
