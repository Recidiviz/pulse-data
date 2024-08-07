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
import os
from typing import Any, Dict, List, Optional

import sqlalchemy
from sqlalchemy.engine import URL, Engine, create_engine

from recidiviz.monitoring.instruments import get_monitoring_instrument
from recidiviz.monitoring.keys import AttributeKey, CounterInstrumentKey
from recidiviz.persistence.database.constants import (
    SQLALCHEMY_DB_HOST,
    SQLALCHEMY_DB_NAME,
    SQLALCHEMY_DB_PASSWORD,
    SQLALCHEMY_DB_PORT,
    SQLALCHEMY_DB_USER,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils import environment, secrets

CLOUDSQL_PROXY_HOST = "127.0.0.1"
CLOUDSQL_PROXY_MIGRATION_PORT = 5440


class SQLAlchemyEngineManager:
    """A class to manage all SQLAlchemy Engines for our database instances."""

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
            # Only reuse connections for up to 10 minutes to avoid failures due
            # to stale connections. Cloud SQL will close connections that have
            # been stale for 10 minutes.
            # https://cloud.google.com/sql/docs/postgres/diagnose-issues#compute-engine
            pool_recycle=3600
            if database_key.schema_type == SchemaType.JUSTICE_COUNTS
            else 600,
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
        return cls._engine_for_database.get(database_key, None)

    @classmethod
    def _secret_manager_prefix_for_type(cls, schema_type: SchemaType) -> str:
        # TODO(#8282): Clean up the _v2 suffix eventually.
        match schema_type:
            case SchemaType.OPERATIONS:
                return "operations_v2"
            case SchemaType.JUSTICE_COUNTS:
                return "justice_counts"
            case SchemaType.CASE_TRIAGE:
                return "case_triage"
            case SchemaType.PATHWAYS:
                return "pathways"
            case SchemaType.WORKFLOWS:
                return "workflows"
            case SchemaType.INSIGHTS:
                return "insights"

        raise ValueError(f"Unexpected schema type [{schema_type}].")

    @classmethod
    def _secret_manager_prefix(cls, database_key: SQLAlchemyDatabaseKey) -> str:
        return cls._secret_manager_prefix_for_type(database_key.schema_type)

    @classmethod
    def _get_db_host(cls, *, database_key: SQLAlchemyDatabaseKey) -> str:
        secret_key = f"{cls._secret_manager_prefix(database_key)}_db_host"
        host = secrets.get_secret(secret_key)
        if host is None:
            raise ValueError(f"Unable to retrieve database host for key [{secret_key}]")
        return host

    @classmethod
    def _get_db_password(
        cls,
        *,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str],
    ) -> str:
        secret_key = f"{secret_prefix_override or cls._secret_manager_prefix(database_key)}_db_password"
        password = secrets.get_secret(secret_key)
        if password is None:
            raise ValueError(
                f"Unable to retrieve database password for key [{secret_key}]"
            )
        return password

    @classmethod
    def _get_db_readonly_password(cls, *, database_key: SQLAlchemyDatabaseKey) -> str:
        secret_key = f"{cls._secret_manager_prefix(database_key)}_db_readonly_password"
        password = secrets.get_secret(secret_key)
        if password is None:
            raise ValueError(
                f"Unable to retrieve database readonly password for key [{secret_key}]"
            )
        return password

    @classmethod
    def _get_db_readonly_user(cls, *, database_key: SQLAlchemyDatabaseKey) -> str:
        secret_key = f"{cls._secret_manager_prefix(database_key)}_db_readonly_user"
        user = secrets.get_secret(secret_key)
        if user is None:
            raise ValueError(
                f"Unable to retrieve database readonly user for key [{secret_key}]"
            )
        return user

    @classmethod
    def _get_db_user(
        cls,
        *,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str],
    ) -> str:
        secret_key = f"{secret_prefix_override or cls._secret_manager_prefix(database_key)}_db_user"
        user = secrets.get_secret(secret_key)
        if user is None:
            raise ValueError(f"Unable to retrieve database user for key [{secret_key}]")
        return user

    @classmethod
    def _get_cloudsql_instance_id_key(
        cls,
        schema_type: SchemaType,
        secret_prefix_override: Optional[str],
    ) -> str:
        return f"{secret_prefix_override or cls._secret_manager_prefix_for_type(schema_type)}_cloudsql_instance_id"

    @classmethod
    def get_full_cloudsql_instance_id(
        cls, schema_type: SchemaType, secret_prefix_override: Optional[str] = None
    ) -> str:
        """Rerturns the full instance id stored in secrets with the form
        project_id:region:instance_id.

        For example:
            recidiviz-staging:us-east1:dev-pathways-data
        """
        instance_id_key = cls._get_cloudsql_instance_id_key(
            schema_type=schema_type, secret_prefix_override=secret_prefix_override
        )
        instance_id_full = secrets.get_secret(instance_id_key)

        if instance_id_full is None:
            raise ValueError(
                f"Unable to retrieve instance id for schema type [{schema_type}]"
            )
        return instance_id_full

    @classmethod
    def get_cloudsql_instance_region(cls, schema_type: SchemaType) -> str:
        """The full instance id stored in secrets has the form
        project_id:region:instance_id.

        This returns just the region, for example, 'us-east1' or
        'us-central1'.
        """
        instance_id_full = cls.get_full_cloudsql_instance_id(schema_type)

        # Expected format "project_id:region:instance_id"
        _, region, _ = instance_id_full.split(":")

        return region

    @classmethod
    def get_stripped_cloudsql_instance_id(cls, schema_type: SchemaType) -> str:
        """The full instance id stored in secrets has the form
        project_id:region:instance_id.

        This returns just the final instance_id, for example, 'dev-data' or
        'prod-state-data'.

        Should be used when using the sqladmin_client().
        """
        instance_id_full = cls.get_full_cloudsql_instance_id(schema_type)

        # Expected format "project_id:region:instance_id"
        _, _, instance_id = instance_id_full.split(":")

        return instance_id

    @classmethod
    def get_all_stripped_cloudsql_instance_ids(cls) -> List[str]:
        """Returns stripped instance ids for all sql instances in this project.

        See get_stripped_cloudsql_instance_id() for more info.
        """
        ids_for_all_schemas = [
            cls.get_stripped_cloudsql_instance_id(schema_type)
            for schema_type in SchemaType
            if schema_type.has_cloud_sql_instance
        ]
        return [
            instance_id
            for instance_id in ids_for_all_schemas
            if instance_id is not None
        ]

    @classmethod
    def update_sqlalchemy_env_vars(
        cls,
        database_key: SQLAlchemyDatabaseKey,
        readonly_user: bool = False,
        using_proxy: bool = False,
        secret_prefix_override: Optional[str] = None,
    ) -> Dict[str, Optional[str]]:
        """Updates the appropriate env vars for SQLAlchemy to talk to the postgres instance associated with the schema.

        It returns the old set of env variables that were overridden.
        """

        sqlalchemy_vars = [
            SQLALCHEMY_DB_NAME,
            SQLALCHEMY_DB_HOST,
            SQLALCHEMY_DB_USER,
            SQLALCHEMY_DB_PASSWORD,
        ]
        original_values = {
            env_var: os.environ.get(env_var) for env_var in sqlalchemy_vars
        }

        os.environ[SQLALCHEMY_DB_NAME] = database_key.db_name

        if using_proxy:
            os.environ[SQLALCHEMY_DB_HOST] = CLOUDSQL_PROXY_HOST
            os.environ[SQLALCHEMY_DB_PORT] = str(CLOUDSQL_PROXY_MIGRATION_PORT)
        else:
            os.environ[SQLALCHEMY_DB_HOST] = cls._get_db_host(database_key=database_key)

        if readonly_user:
            os.environ[SQLALCHEMY_DB_USER] = cls._get_db_readonly_user(
                database_key=database_key
            )
            os.environ[SQLALCHEMY_DB_PASSWORD] = cls._get_db_readonly_password(
                database_key=database_key
            )
        else:
            os.environ[SQLALCHEMY_DB_USER] = cls._get_db_user(
                database_key=database_key, secret_prefix_override=secret_prefix_override
            )
            os.environ[SQLALCHEMY_DB_PASSWORD] = cls._get_db_password(
                database_key=database_key, secret_prefix_override=secret_prefix_override
            )

        return original_values

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
        instance_id_key = cls._get_cloudsql_instance_id_key(
            schema_type=schema_type, secret_prefix_override=secret_prefix_override
        )
        if instance_id_key is None:
            raise ValueError(
                f"Instance id is not configured for schema type [{schema_type}]"
            )

        db_host = cls._get_db_host(database_key=database_key)
        db_user = cls._get_db_user(
            database_key=database_key, secret_prefix_override=secret_prefix_override
        )
        db_password = cls._get_db_password(
            database_key=database_key, secret_prefix_override=secret_prefix_override
        )
        db_port = cls._get_db_port()
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
        db_user = cls._get_db_user(
            database_key=database_key, secret_prefix_override=secret_prefix_override
        )
        db_password = cls._get_db_password(
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

    @classmethod
    def _get_db_port(cls) -> int:
        return int(os.environ.get(SQLALCHEMY_DB_PORT, "5432"))
