# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""
Class for generating SQLAlchemy AsyncSessions objects for the appropriate schema.
"""
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from recidiviz.persistence.database.sqlalchemy_async_engine_manager import (
    SQLAlchemyAsyncEngineManager,
)
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


class AsyncSessionFactory:
    """Creates SQLAlchemy AsyncSessions for the given database schema"""

    @classmethod
    @asynccontextmanager
    async def using_database(
        cls,
        database_key: SQLAlchemyDatabaseKey,
        *,
        autocommit: bool = True,
        secret_prefix_override: Optional[str] = None,
    ) -> AsyncIterator[AsyncSession]:
        session = None
        try:
            session = await cls._for_database(
                database_key=database_key, secret_prefix_override=secret_prefix_override
            )
            yield session
            if autocommit:
                try:
                    await session.commit()
                except Exception as e:
                    await session.rollback()
                    raise e
        finally:
            if session:
                await session.close()

    @classmethod
    async def _for_database(
        cls, database_key: SQLAlchemyDatabaseKey, secret_prefix_override: Optional[str]
    ) -> AsyncSession:
        engine = await SQLAlchemyAsyncEngineManager.get_async_engine_for_database(
            database_key=database_key, secret_prefix_override=secret_prefix_override
        )
        if engine is None:
            raise ValueError(f"No engine set for key [{database_key}]")

        session = AsyncSession(bind=engine, expire_on_commit=False)
        return session
