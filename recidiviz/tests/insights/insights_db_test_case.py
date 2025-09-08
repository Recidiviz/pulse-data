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
from unittest import TestCase
from unittest.mock import patch

from recidiviz.outliers.types import StateCode
from recidiviz.persistence.database.schema.insights.schema import InsightsBase
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

INSIGHTS_US_XX_DB = "insights_us_xx"


class InsightsDbTestCase(TestCase):
    """Implements class methods for Insights DB related tests."""

    # Stores the location of the postgres DB for this test run
    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database(
                additional_databases_to_create=[INSIGHTS_US_XX_DB]
            )
        )

    def setUp(self) -> None:
        self.enabled_states_patcher = patch(
            "recidiviz.outliers.querier.querier.get_outliers_enabled_states",
            return_value=[StateCode.US_XX.value],
        )
        self.enabled_states_patcher.start()
        self.insights_database_key = SQLAlchemyDatabaseKey(
            SchemaType.INSIGHTS, db_name="us_xx"
        )
        self.insights_engine = SQLAlchemyEngineManager.init_engine_for_db_instance(
            database_key=self.insights_database_key,
            db_url=self.postgres_launch_result.url(database_name=INSIGHTS_US_XX_DB),
        )
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result,
            self.insights_database_key,
            engine=self.insights_engine,
        )

        InsightsBase.metadata.drop_all(self.insights_engine)
        InsightsBase.metadata.create_all(self.insights_engine)

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.insights_database_key
        )
        self.enabled_states_patcher.stop()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )
