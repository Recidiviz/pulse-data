# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
# ============================================================================
"""Database connection and session setup."""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_async_engine_manager import (
    SQLAlchemyAsyncEngineManager,
)
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.resource_search.src.settings import Settings
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils.environment import in_gcp_production, in_gcp_staging
from recidiviz.utils.secrets import get_secret_from_local_directory

_global_engine: Optional[AsyncEngine] = None


async def initialize_global_engine(db_name: str) -> None:
    """Initializes the global AsyncEngine once."""
    global _global_engine
    if _global_engine is None:
        db_url_to_use = (
            SQLAlchemyAsyncEngineManager.get_server_postgres_asyncpg_instance_url(
                database_key=SQLAlchemyDatabaseKey(
                    schema_type=SchemaType.RESOURCE_SEARCH, db_name=db_name
                ),
            )
            if in_gcp_staging() or in_gcp_production()
            else local_postgres_helpers.async_on_disk_postgres_db_url(
                username=get_secret_from_local_directory(
                    secret_id="resource_search_db_user"  # nosec
                ),
                port=get_secret_from_local_directory(
                    secret_id="resource_search_db_port"  # nosec
                ),
                password=get_secret_from_local_directory(
                    secret_id="resource_search_db_password"  # nosec
                ),
                database=db_name,
            )
        )
        _global_engine = (
            await SQLAlchemyAsyncEngineManager.init_async_engine_for_postgres_instance(
                db_url=db_url_to_use,
                database_key=SQLAlchemyDatabaseKey(
                    schema_type=SchemaType.RESOURCE_SEARCH, db_name=db_name
                ),
            )
        )


async def dispose_global_engine() -> None:
    """Disposes the global AsyncEngine."""
    global _global_engine
    if _global_engine:
        await _global_engine.dispose()
        _global_engine = None


@asynccontextmanager
async def transaction_session(settings: Settings) -> AsyncGenerator[AsyncSession, None]:
    """Provide a transactional scope around a series of operations."""
    # Ensure the engine is initialized before creating a session
    if _global_engine is None:
        await initialize_global_engine(db_name=settings.db_name)

    session = AsyncSession(_global_engine)  # Use the global engine
    async with session.begin() as transaction:
        try:
            yield session
            await transaction.commit()
        except Exception as error:
            logging.error("Transaction Error: %s", error)
            await transaction.rollback()
            raise
        finally:
            await session.close()
