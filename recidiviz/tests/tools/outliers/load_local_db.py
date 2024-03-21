# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Implements tests for the load_local_db script."""
from typing import Optional
from unittest import TestCase

import pytest

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.persistence.database.schema.outliers.schema import (
    OutliersBase,
    SupervisionOfficer,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_all_table_classes_in_schema,
    get_outliers_database_entities,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.outliers.load_local_db import (
    import_outliers_from_gcs,
    reset_outliers_fixtures,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import (
    get_on_disk_postgres_database_name,
)
from recidiviz.tools.utils.fixture_helpers import create_dbs


@pytest.mark.uses_db
class TestLoadLocalDb(TestCase):
    """Implements tests for the load_local_db script."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.db_key = SQLAlchemyDatabaseKey(
            SchemaType.OUTLIERS, get_on_disk_postgres_database_name()
        )
        self.env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars()
        )

        self.engine = SQLAlchemyEngineManager.init_engine_for_postgres_instance(
            database_key=self.db_key,
            db_url=local_postgres_helpers.on_disk_postgres_db_url(),
        )
        OutliersBase.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(self.db_key)
        local_postgres_helpers.restore_local_env_vars(self.env_vars)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_reset_outliers_fixtures(self) -> None:
        # First order of business, this shouldn't crash.
        reset_outliers_fixtures(self.engine, get_outliers_database_entities(), "US_PA")

        # Make sure values are actually written to the tables we know about.
        with SessionFactory.using_database(
            self.db_key, autocommit=False
        ) as read_session:
            for fixture_class in get_all_table_classes_in_schema(SchemaType.OUTLIERS):
                self.assertTrue(len(read_session.query(fixture_class).all()) > 0)

    def test_create_dbs(self) -> None:
        pa_key = SQLAlchemyDatabaseKey(SchemaType.OUTLIERS, "us_pa")
        pa_engine = SQLAlchemyEngineManager.init_engine_for_postgres_instance(
            database_key=pa_key,
            db_url=local_postgres_helpers.on_disk_postgres_db_url("us_pa"),
        )

        # Ensure DB does not exist
        with pytest.raises(Exception):
            pa_engine.connect()

        create_dbs(["US_PA"], SchemaType.OUTLIERS, engine=self.engine)

        # Should no longer throw
        pa_engine.connect()

        # Should no-op
        create_dbs(["US_PA"], SchemaType.OUTLIERS, engine=self.engine)

    def test_import_outliers_from_gcs(self) -> None:
        bucket = "recidiviz-456-outliers-etl-data"
        fake_gcs = FakeGCSFileSystem()
        fake_csv_path = GcsfsFilePath.from_absolute_path(
            f"gs://{bucket}/US_XX/supervision_officers.csv"
        )
        fake_gcs.upload_from_string(
            path=fake_csv_path,
            # Columns here are in the order they're exported in the BQ views
            contents='US_XX,02,00002,"{""surname"": ""DOE""}",officerhash2,101,1,OTHER',
            content_type="text/csv",
        )
        import_outliers_from_gcs(
            self.engine, [SupervisionOfficer], bucket, "US_XX", fake_gcs
        )

        with SessionFactory.using_database(
            self.db_key, autocommit=False
        ) as read_session:
            self.assertTrue(len(read_session.query(SupervisionOfficer).all()) > 0)
