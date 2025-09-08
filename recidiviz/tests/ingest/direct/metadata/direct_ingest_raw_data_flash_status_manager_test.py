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
"""Implements tests for the DirectIngestRawDataFlashStatus."""
import datetime
from unittest import TestCase

import pytest

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.metadata.direct_ingest_raw_data_flash_status_manager import (
    DirectIngestRawDataFlashStatusManager,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestRawDataFlashStatus,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult


@pytest.mark.uses_db
class DirectIngestRawDataFlashStatusTest(TestCase):
    """Implements tests for DirectIngestRawDataFlashStatus."""

    # Stores the location of the postgres DB for this test run
    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.operations_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.operations_key
        )
        self.us_xx_manager = DirectIngestRawDataFlashStatusManager(
            StateCode.US_XX.value,
        )
        self.initial_status_time = datetime.datetime(2019, 1, 1, tzinfo=datetime.UTC)
        self.us_xx_manager.add_initial_status(self.initial_status_time)

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.operations_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def test_initial_status_false(self) -> None:
        self.assertFalse(self.us_xx_manager.is_flashing_in_progress())

    def test_get_flashing_status(self) -> None:
        self.assertEqual(
            self.us_xx_manager.get_flashing_status(),
            DirectIngestRawDataFlashStatus(
                region_code=StateCode.US_XX.value,
                status_timestamp=self.initial_status_time,
                flashing_in_progress=False,
            ),
        )

    def test_set_flashing_started(self) -> None:
        self.us_xx_manager.set_flashing_started()
        self.assertTrue(self.us_xx_manager.is_flashing_in_progress())

        # make sure adding again is a no-op
        status_1 = self.us_xx_manager.get_flashing_status()
        self.us_xx_manager.set_flashing_started()
        status_2 = self.us_xx_manager.get_flashing_status()

        self.assertEqual(status_1, status_2)

    def test_set_flashing_finished(self) -> None:
        self.us_xx_manager.set_flashing_started()
        self.assertTrue(self.us_xx_manager.is_flashing_in_progress())

        self.us_xx_manager.set_flashing_finished()
        self.assertFalse(self.us_xx_manager.is_flashing_in_progress())

        # make sure adding again is a no-op
        status_1 = self.us_xx_manager.get_flashing_status()
        self.us_xx_manager.set_flashing_finished()
        status_2 = self.us_xx_manager.get_flashing_status()

        self.assertEqual(status_1, status_2)
