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

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.schema.pathways.schema import (
    LibertyToPrisonTransitions,
    PathwaysBase,
    PrisonToSupervisionTransitions,
    SupervisionPopulationOverTime,
)
from recidiviz.persistence.database.schema_utils import (
    SchemaType,
    get_pathways_database_entities,
    get_pathways_table_classes,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tools.pathways.load_fixtures import (
    create_dbs,
    import_pathways_from_gcs,
    reset_pathways_fixtures,
)
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import (
    get_on_disk_postgres_database_name,
)


@pytest.mark.uses_db
class TestLoadFixtures(TestCase):
    """Implements tests for the load fixtures script."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.db_key = SQLAlchemyDatabaseKey(
            SchemaType.PATHWAYS, get_on_disk_postgres_database_name()
        )
        self.env_vars = (
            local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()
        )

        self.engine = SQLAlchemyEngineManager.init_engine_for_postgres_instance(
            database_key=self.db_key,
            db_url=local_postgres_helpers.on_disk_postgres_db_url(),
        )
        PathwaysBase.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.db_key)
        local_postgres_helpers.restore_local_env_vars(self.env_vars)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_reset_pathways_fixtures(self) -> None:
        # First order of business, this shouldn't crash.
        reset_pathways_fixtures(self.engine, get_pathways_database_entities(), "US_XX")

        # Make sure values are actually written to the tables we know about.
        with SessionFactory.using_database(
            self.db_key, autocommit=False
        ) as read_session:
            for fixture_class in get_pathways_table_classes():
                self.assertTrue(len(read_session.query(fixture_class).all()) > 0)

    def test_create_dbs(self) -> None:
        tn_key = SQLAlchemyDatabaseKey(SchemaType.PATHWAYS, "us_tn")
        tn_engine = SQLAlchemyEngineManager.init_engine_for_postgres_instance(
            database_key=tn_key,
            db_url=local_postgres_helpers.on_disk_postgres_db_url("us_tn"),
        )

        # Ensure DB does not exist
        with pytest.raises(Exception):
            tn_engine.connect()

        create_dbs(["US_TN"], engine=self.engine)

        # Should no longer throw
        tn_engine.connect()

        # Should no-op
        create_dbs(["US_TN"], engine=self.engine)

    def test_import_pathways_from_gcs(self) -> None:
        bucket = "recidiviz-456-dashboard-event-level-data"
        fake_gcs = FakeGCSFileSystem()
        fake_gcs.upload_from_string(
            path=GcsfsFilePath.from_absolute_path(
                f"gs://{bucket}/US_XX/liberty_to_prison_transitions.csv"
            ),
            # Columns here are in the order they're exported in the BQ views
            contents="2022-01-01,2022,1,months_0_6,1,20-25,MALE,WHITE,D1,months_0_3,US_XX",
            content_type="text/csv",
        )
        fake_gcs.upload_from_string(
            path=GcsfsFilePath.from_absolute_path(
                f"gs://{bucket}/US_XX/supervision_population_over_time.csv"
            ),
            contents="US_XX,0001,2022-01-01,months_7_12,District 1,HIGH,HISPANIC",
            content_type="text/csv",
        )

        import_pathways_from_gcs(
            self.engine,
            [LibertyToPrisonTransitions, SupervisionPopulationOverTime],
            bucket,
            "US_XX",
            fake_gcs,
        )

        with SessionFactory.using_database(
            self.db_key, autocommit=False
        ) as read_session:
            self.assertTrue(
                len(read_session.query(LibertyToPrisonTransitions).all()) > 0
            )
            self.assertTrue(
                len(read_session.query(SupervisionPopulationOverTime).all()) > 0
            )
            # We didn't import this table
            self.assertTrue(
                len(read_session.query(PrisonToSupervisionTransitions).all()) == 0
            )
