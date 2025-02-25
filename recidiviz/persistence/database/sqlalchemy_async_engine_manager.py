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
"""A class to manage all SQLAlchemy AsyncEngines for our database instances."""
import logging
from typing import Any, Dict, Optional

from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from recidiviz.persistence.database.base_engine_manager import BaseEngineManager
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils import environment, secrets


class SQLAlchemyAsyncEngineManager(BaseEngineManager):
    """An async class to manage all SQLAlchemy Engines for our database instances.

    This class uses the synchronous SQLAlchemyEngineManager for non-I/O methods.
    """

    _async_engine_for_database: Dict[SQLAlchemyDatabaseKey, AsyncEngine] = {}

    @classmethod
    async def init_async_engine_for_postgres_instance(
        cls,
        database_key: SQLAlchemyDatabaseKey,
        db_url: URL,
    ) -> AsyncEngine:
        """Initializes an asynchronous SQLAlchemy Engine object for the given Postgres database
        and caches it for future use."""
        additional_kwargs = {}
        if database_key.pool_configuration:
            additional_kwargs["pool_size"] = database_key.pool_configuration.pool_size
            additional_kwargs[
                "max_overflow"
            ] = database_key.pool_configuration.max_overflow
            additional_kwargs[
                "pool_timeout"
            ] = database_key.pool_configuration.pool_timeout
        return await cls.init_async_engine_for_db_instance(
            database_key=database_key,
            db_url=db_url,
            echo_pool=True,
            pool_recycle=3600
            if database_key.schema_type == SchemaType.JUSTICE_COUNTS
            else 600,
            **additional_kwargs,
        )

    @classmethod
    async def init_async_engine_for_db_instance(
        cls,
        database_key: SQLAlchemyDatabaseKey,
        db_url: URL,
        **dialect_specific_kwargs: Any,
    ) -> AsyncEngine:
        """Initializes an asynchronous SQLAlchemy Engine object for the given database
        and caches it for future use."""
        if database_key in cls._async_engine_for_database:
            raise ValueError(f"Already initialized async database [{database_key}]")

        try:
            engine = create_async_engine(
                db_url,
                isolation_level=database_key.isolation_level,
                poolclass=database_key.poolclass,
                **dialect_specific_kwargs,
            )
        except BaseException as e:
            logging.error(
                "Unable to connect to postgres instance for [%s]: %s",
                database_key,
                str(e),
            )

            raise e

        cls._async_engine_for_database[database_key] = engine
        return engine

    @classmethod
    async def teardown_async_engine_for_database_key(
        cls, *, database_key: SQLAlchemyDatabaseKey
    ) -> None:
        await cls._async_engine_for_database.pop(database_key).dispose()

    @classmethod
    async def teardown_async_engines(cls) -> None:
        for engine in cls._async_engine_for_database.values():
            await engine.dispose()
        cls._async_engine_for_database.clear()

    @classmethod
    async def init_async_engine(
        cls,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str] = None,
    ) -> AsyncEngine:
        return await cls.init_async_engine_for_postgres_instance(
            database_key=database_key,
            db_url=cls.get_server_postgres_asyncpg_instance_url(
                database_key=database_key, secret_prefix_override=secret_prefix_override
            ),
        )

    @classmethod
    async def get_async_engine_for_database(
        cls,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str] = None,
    ) -> Optional[AsyncEngine]:
        """Retrieve the async engine for a given database.

        Will attempt to create the engine if it does not already exist."""
        if database_key not in cls._async_engine_for_database:
            if not environment.in_gcp():
                logging.info(
                    "Environment is not GCP, not connecting to postgres instance for [%s].",
                    database_key,
                )
                return None
            await cls.init_async_engine(
                database_key=database_key, secret_prefix_override=secret_prefix_override
            )
        return cls._async_engine_for_database.get(database_key, None)

    @classmethod
    def get_server_postgres_asyncpg_instance_url(
        cls,
        *,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str] = None,
        using_unix_sockets: bool = True,
    ) -> URL:
        """Returns the Cloud SQL instance URL for a given database key.

        The host, user, password, etc. used to determine the instance URL are stored
        in GCP secrets.

        Args:
            secret_prefix_override: If not specified,  we determine the prefix
                for these secrets based on the database schema. However, it is possible that the
                same database schema may live in more than one instance. In this case, use the
                `secret_prefix_override` argument to override this logic.
            using_unix_sockets: If True (default), we omit the hostname and set the query of the
                url to {"host": f"/cloudsql/{cloudsql_instance_id}"}. If False, we set the
                hostname according to secrets.
        """
        schema_type = database_key.schema_type
        instance_id_key = super()._get_cloudsql_instance_id_key(
            schema_type=schema_type, secret_prefix_override=secret_prefix_override
        )
        if instance_id_key is None:
            raise ValueError(
                f"Instance id is not configured for schema type [{schema_type}]"
            )

        db_host = super()._get_db_host(
            database_key=database_key, secret_prefix_override=secret_prefix_override
        )
        db_user = super()._get_db_user(
            database_key=database_key, secret_prefix_override=secret_prefix_override
        )
        db_password = super()._get_db_password(
            database_key=database_key, secret_prefix_override=secret_prefix_override
        )
        db_port = super()._get_db_port()
        cloudsql_instance_id = secrets.get_secret(instance_id_key)
        if cloudsql_instance_id is None:
            raise ValueError(
                f"Unable to retrieve instance id for schema type [{schema_type}]"
            )
        db_name = database_key.db_name

        # Using the asyncpg driver for async operations
        url = URL.create(
            drivername="postgresql+asyncpg",
            username=db_user,
            password=db_password,
            database=db_name,
            host=db_host if not using_unix_sockets else None,
            port=db_port,
            query={"host": f"/cloudsql/{cloudsql_instance_id}"}
            if using_unix_sockets
            else {},
        )

        return url

    @classmethod
    def teardown_engine_for_database_key(
        cls, *, database_key: SQLAlchemyDatabaseKey
    ) -> None:
        print(cls._async_engine_for_database)
        cls._async_engine_for_database.pop(database_key).dispose()
