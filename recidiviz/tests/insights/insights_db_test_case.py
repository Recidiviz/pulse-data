#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
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
"""This class implements tests for the OutliersQuerier class"""
from typing import Optional
from unittest import TestCase

from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import on_disk_postgres_db_url

INSIGHTS_US_PA_DB = "insights_us_pa"
OUTLIERS_US_PA_DB = "outliers_us_pa"


class InsightsDbTestCase(TestCase):
    """Implements class methods for Insights DB related tests."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database(
            additional_databases_to_create=[INSIGHTS_US_PA_DB, OUTLIERS_US_PA_DB]
        )

    def setUp(self) -> None:
        # TODO(#29848): Remove and use only Insights DB manager once `configurations` and `user_metadata` tables are migrated
        self.outliers_database_key = SQLAlchemyDatabaseKey(
            SchemaType.OUTLIERS, db_name="us_pa"
        )
        self.outliers_engine = SQLAlchemyEngineManager.init_engine_for_db_instance(
            database_key=self.outliers_database_key,
            db_url=on_disk_postgres_db_url(database=OUTLIERS_US_PA_DB),
        )
        self.insights_database_key = SQLAlchemyDatabaseKey(
            SchemaType.INSIGHTS, db_name="us_pa"
        )
        self.insights_engine = SQLAlchemyEngineManager.init_engine_for_db_instance(
            database_key=self.insights_database_key,
            db_url=on_disk_postgres_db_url(database=INSIGHTS_US_PA_DB),
        )
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.outliers_database_key, engine=self.outliers_engine
        )
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.insights_database_key, engine=self.insights_engine
        )

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.outliers_database_key
        )
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.insights_database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )
