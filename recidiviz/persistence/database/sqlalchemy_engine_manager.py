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
"""A class to manage all SQLAlchemy Engines for our database instances."""
import logging
from typing import Any, Dict, List, Optional

import sqlalchemy
from sqlalchemy.engine import URL, Engine, create_engine

from recidiviz.monitoring.instruments import get_monitoring_instrument
from recidiviz.monitoring.keys import AttributeKey, CounterInstrumentKey
from recidiviz.persistence.database.base_engine_manager import BaseEngineManager
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils import environment, secrets

CLOUDSQL_PROXY_HOST = "127.0.0.1"
CLOUDSQL_PROXY_MIGRATION_PORT = 5440


class SQLAlchemyEngineManager(BaseEngineManager):
    """A class to manage all syncronous SQLAlchemy Engines for our database instances."""

    _engine_for_database: Dict[SQLAlchemyDatabaseKey, Engine] = {}

    @classmethod
    def init_engine_for_postgres_instance(
        cls,
        database_key: SQLAlchemyDatabaseKey,
        db_url: URL,
    ) -> Engine:
        """Initializes a sqlalchemy Engine object for the given Postgres database /
        schema and caches it for future use."""
        additional_kwargs = {}
        if database_key.pool_configuration:
            additional_kwargs["pool_size"] = database_key.pool_configuration.pool_size
            additional_kwargs[
                "max_overflow"
            ] = database_key.pool_configuration.max_overflow
            additional_kwargs[
                "pool_timeout"
            ] = database_key.pool_configuration.pool_timeout
        return cls.init_engine_for_db_instance(
            database_key=database_key,
            db_url=db_url,
            # Log information about how connections are being reused.
            echo_pool=True,
            pool_recycle=database_key.pool_recycle,
            **additional_kwargs,
        )

    @classmethod
    def init_engine_for_db_instance(
        cls,
        database_key: SQLAlchemyDatabaseKey,
        db_url: URL,
        **dialect_specific_kwargs: Any,
    ) -> Engine:
        """Initializes a sqlalchemy Engine object for the given database / schema and
        caches it for future use."""
        if database_key in cls._engine_for_database:
            raise ValueError(f"Already initialized database [{database_key}]")

        try:
            engine = sqlalchemy.create_engine(
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

            get_monitoring_instrument(
                CounterInstrumentKey.ENGINE_INITIALIZATION_FAILURE
            ).add(
                amount=1,
                attributes={
                    AttributeKey.SCHEMA_TYPE: database_key.schema_type.value,
                    AttributeKey.DATABASE_NAME: database_key.db_name,
                },
            )

            raise e
        cls._engine_for_database[database_key] = engine
        return engine

    @classmethod
    def teardown_engine_for_database_key(
        cls, *, database_key: SQLAlchemyDatabaseKey
    ) -> None:
        cls._engine_for_database.pop(database_key).dispose()

    @classmethod
    def teardown_engines(cls) -> None:
        for engine in cls._engine_for_database.values():
            engine.dispose()
        cls._engine_for_database.clear()

    @classmethod
    def init_engine(
        cls,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str] = None,
    ) -> Engine:
        return cls.init_engine_for_postgres_instance(
            database_key=database_key,
            db_url=cls.get_server_postgres_instance_url(
                database_key=database_key, secret_prefix_override=secret_prefix_override
            ),
        )

    @classmethod
    def attempt_init_engines_for_databases(
        cls,
        database_keys: List[SQLAlchemyDatabaseKey],
        secret_prefix_override: Optional[str] = None,
    ) -> None:
        """Attempts to initialize engines for the provided databases.

        Ignores any connections that fail, so that a single down database does not cause
        our server to crash."""
        for database_key in database_keys:
            try:
                cls.init_engine(
                    database_key=database_key,
                    secret_prefix_override=secret_prefix_override,
                )
            except BaseException:
                pass

    @classmethod
    def get_engine_for_database(
        cls,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str] = None,
    ) -> Optional[Engine]:
        """Retrieve the engine for a given database.

        Will attempt to create the engine if it does not already exist."""
        if database_key not in cls._engine_for_database:
            if not environment.in_gcp():
                logging.info(
                    "Environment is not GCP, not connecting to postgres instance for [%s].",
                    database_key,
                )
                return None
            cls.init_engine(
                database_key=database_key, secret_prefix_override=secret_prefix_override
            )

        engine = cls._engine_for_database.get(database_key, None)

        # Add pool monitoring logging
        if engine and hasattr(engine, "pool"):
            try:
                pool_stats = {
                    "pool_size": engine.pool.size(),
                    "checked_out": engine.pool.checkedout(),
                    "checked_in": engine.pool.checkedin(),
                    "overflow": engine.pool.overflow(),
                    "invalid": engine.pool.invalid(),
                }
                logging.info("Pool stats for %s: %s", database_key, pool_stats)
            except Exception as e:
                logging.warning("Failed to get pool stats for %s: %s", database_key, e)

        return engine

    @classmethod
    def get_server_postgres_instance_url(
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
        db_name = database_key.db_name

        url = URL.create(
            drivername="postgresql",
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
    @environment.local_only
    def get_engine_for_database_with_proxy(
        cls,
        *,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str] = None,
    ) -> Engine:
        """Returns an engine configured to connect to the Cloud SQL Proxy.
        Used when running migrations via run_all_migrations_using_proxy.sh"""
        db_user = super()._get_db_user(
            database_key=database_key, secret_prefix_override=secret_prefix_override
        )
        db_password = super()._get_db_password(
            database_key=database_key, secret_prefix_override=secret_prefix_override
        )
        db_name = database_key.db_name
        url = URL.create(
            drivername="postgresql",
            username=db_user,
            password=db_password,
            host=CLOUDSQL_PROXY_HOST,
            port=CLOUDSQL_PROXY_MIGRATION_PORT,
            database=db_name,
        )
        engine = create_engine(url)
        return engine
