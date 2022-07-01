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
from collections import defaultdict
from typing import Optional

import pytest

from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_region_dir_names,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations.schema import (
    DirectIngestInstancePauseStatus,
    DirectIngestInstanceStatus,
)
from recidiviz.persistence.database.schema_utils import (
    SchemaType,
    get_operations_table_classes,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.admin_panel.load_operations_db_fixtures import (
    reset_operations_db_fixtures,
)
from recidiviz.tools.postgres import local_postgres_helpers


@pytest.mark.uses_db
class TestOperationsLoadFixtures(unittest.TestCase):
    """Implements tests for the Admin Panel load_fixtures script."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.canonical_for_schema(
            SchemaType.OPERATIONS
        )
        self.engine = local_postgres_helpers.use_on_disk_postgresql_database(
            self.database_key
        )

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_load_fixtures_succeeds(self) -> None:
        # Clear DB and then load in fixture data
        reset_operations_db_fixtures(self.engine)

        # Assert that DB is now non-empty
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as read_session:
            for fixture_class in get_operations_table_classes():
                if fixture_class.name == "direct_ingest_sftp_file_metadata":
                    # TODO(#10214): Add admin panel fixtures for the SFTP table.
                    continue
                row_count = len(read_session.query(fixture_class).all())
                self.assertTrue(
                    row_count > 0, f"Found no rows in table [{fixture_class}]"
                )

    def test_direct_ingest_instance_pause_status_contains_data_for_all_states(
        self,
    ) -> None:
        """Enforces that the fixture for the pause status table at
        recidiviz/tools/admin_panel/fixtures/operations_db/direct_ingest_instance_pause_status.csv
        has data for all states.
        """

        # Clear DB and then load in fixture data
        reset_operations_db_fixtures(self.engine)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as read_session:
            rows = read_session.query(DirectIngestInstancePauseStatus).all()

            instance_to_state_codes = defaultdict(set)
            for row in rows:
                instance_to_state_codes[DirectIngestInstance(row.instance)].add(
                    row.region_code
                )

            required_states = {name.upper() for name in get_existing_region_dir_names()}

            for instance in DirectIngestInstance:
                self.assertEqual(required_states, instance_to_state_codes[instance])

    def test_direct_ingest_instance_status_contains_data_for_all_states(
        self,
    ) -> None:
        """Enforces that the fixture for the pause status table at
        recidiviz/tools/admin_panel/fixtures/operations_db/direct_ingest_instance_status.csv
        has data for all states.
        """

        # Clear DB and then load in fixture data
        reset_operations_db_fixtures(self.engine)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as read_session:
            rows = read_session.query(DirectIngestInstanceStatus).all()

            instance_to_state_codes = defaultdict(set)
            for row in rows:
                instance_to_state_codes[DirectIngestInstance(row.instance)].add(
                    row.region_code
                )

            required_states = {name.upper() for name in get_existing_region_dir_names()}

            for instance in DirectIngestInstance:
                self.assertEqual(required_states, instance_to_state_codes[instance])
