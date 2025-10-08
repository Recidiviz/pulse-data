# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Implements tests for the Admin Panel load_fixtures script."""
import unittest

import pytest

from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_region_codes,
)
from recidiviz.persistence.database.schema.operations.schema import (
    DirectIngestRawDataFlashStatus,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.admin_panel.load_operations_db_fixtures import (
    get_all_operations_table_classes_with_fixtures,
    reset_operations_db_fixtures,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult


@pytest.mark.uses_db
class TestOperationsLoadFixtures(unittest.TestCase):
    """Implements tests for the Admin Panel load_fixtures script."""

    # Stores the location of the postgres DB for this test run
    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.canonical_for_schema(
            SchemaType.OPERATIONS
        )
        self.engine = local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def test_load_fixtures_succeeds(self) -> None:
        # Clear DB and then load in fixture data
        reset_operations_db_fixtures(self.engine)

        # Assert that DB is now non-empty
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as read_session:
            for fixture_class in get_all_operations_table_classes_with_fixtures():
                row_count = len(read_session.query(fixture_class).all())
                self.assertTrue(
                    row_count > 0, f"Found no rows in table [{fixture_class}]"
                )

    def test_direct_ingest_raw_data_flash_status_contains_data_for_all_states(
        self,
    ) -> None:
        """Enforces that the fixture for the status table at
        recidiviz/tools/admin_panel/fixtures/operations_db/direct_ingest_raw_data_flash_status.csv
        has data for all states.
        """

        # Clear DB and then load in fixture data
        reset_operations_db_fixtures(self.engine)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as read_session:
            actual_states = {
                region[0]
                for region in read_session.query(
                    DirectIngestRawDataFlashStatus.region_code
                ).all()
            }
            expected_states = {name.upper() for name in get_existing_region_codes()}

            self.assertEqual(actual_states, expected_states)
