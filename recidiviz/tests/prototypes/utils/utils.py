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
"""Implements tests for the Prototypes backend API."""

from typing import Optional
from unittest import IsolatedAsyncioTestCase

import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_async_engine_manager import (
    SQLAlchemyAsyncEngineManager,
)
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers


@pytest.mark.uses_db
class PrototypesDatabaseTestCase(IsolatedAsyncioTestCase):
    """Base class for async unit tests for prototype databases."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey(SchemaType.WORKFLOWS, db_name="us_id")
        self.env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars()
        )
        self.maxDiff = None

    async def asyncSetUp(self) -> None:
        self.engine = await self.async_get_engine()
        async with self.engine.begin() as conn:
            await conn.run_sync(self.database_key.declarative_meta.metadata.create_all)

    async def async_get_engine(self) -> AsyncEngine:
        """Return the Engine that this test class should use to connect to
        the database. By default, initialize a new engine. Subclasses can
        override this method to point to an engine that already exists."""
        return (
            await SQLAlchemyAsyncEngineManager.init_async_engine_for_postgres_instance(
                database_key=self.database_key,
                db_url=local_postgres_helpers.async_on_disk_postgres_db_url(),
            )
        )

    def get_engine(self) -> Engine:
        """Return the Engine that this test class should use to connect to
        the database. By default, initialize a new engine. Subclasses can
        override this method to point to an engine that already exists."""
        return SQLAlchemyEngineManager.init_engine_for_postgres_instance(
            database_key=self.database_key,
            db_url=local_postgres_helpers.on_disk_postgres_db_url(),
        )

    async def asyncTearDown(self) -> None:
        await local_persistence_helpers.async_teardown_on_disk_postgresql_database(
            self.database_key
        )
        local_postgres_helpers.restore_local_env_vars(self.env_vars)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )
