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
"""Implements tests for the DirectIngestInstanceStatusManager."""
from typing import Optional
from unittest.case import TestCase

import pytest

from recidiviz.ingest.direct.types.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.ingest.direct.controllers.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres import local_postgres_helpers


@pytest.mark.uses_db
class DirectIngestInstanceStatusManagerTest(TestCase):
    """Implements tests for DirectIngestInstanceStatusManager."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.operations_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_postgres_helpers.use_on_disk_postgresql_database(self.operations_key)

        self.us_xx_manager = DirectIngestInstanceStatusManager.add_instance(
            "US_XX", DirectIngestInstance.PRIMARY, True
        )
        self.us_ww_manager = DirectIngestInstanceStatusManager.add_instance(
            "US_WW", DirectIngestInstance.PRIMARY, False
        )

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.operations_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_is_instance_paused(self) -> None:
        self.assertTrue(self.us_xx_manager.is_instance_paused())
        self.assertFalse(self.us_ww_manager.is_instance_paused())

    def test_pausing(self) -> None:
        self.us_ww_manager.pause_instance()
        self.assertTrue(self.us_ww_manager.is_instance_paused())

        # Pausing an already paused instance should succeed
        self.us_xx_manager.pause_instance()
        self.assertTrue(self.us_xx_manager.is_instance_paused())

    def test_unpausing(self) -> None:
        self.us_xx_manager.unpause_instance()
        self.assertFalse(self.us_xx_manager.is_instance_paused())

        # Unpausing an already unpaused instance should succeed
        self.us_ww_manager.unpause_instance()
        self.assertFalse(self.us_ww_manager.is_instance_paused())
