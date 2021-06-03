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
import os.path
from typing import Any, Dict, List, Optional, Set

import sqlalchemy
from opencensus.stats import aggregation, measure, view
from sqlalchemy.engine import URL, Engine, create_engine

from recidiviz.persistence.database.constants import (
    SQLALCHEMY_DB_HOST,
    SQLALCHEMY_DB_NAME,
    SQLALCHEMY_DB_PASSWORD,
    SQLALCHEMY_DB_USER,
    SQLALCHEMY_SSL_CERT_PATH,
    SQLALCHEMY_SSL_KEY_PATH,
    SQLALCHEMY_USE_SSL,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils import environment, monitoring, secrets

m_failed_engine_initialization = measure.MeasureInt(
    "persistence/database/sqlalchemy_engine_initialization_failures",
    "Counts the number of engine initialization failures",
    "1",
)

failed_engine_initialization_view = view.View(
    "recidiviz/persistence/database/sqlalchemy_engine_initialization_failures",
    "Sum of all engine initialization failures",
    [monitoring.TagKey.SCHEMA_TYPE, monitoring.TagKey.DATABASE_NAME],
    m_failed_engine_initialization,
    aggregation.SumAggregation(),
)

monitoring.register_views([failed_engine_initialization_view])


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

        return cls.init_engine_for_db_instance(
            database_key=database_key,
            db_url=db_url,
            # Log information about how connections are being reused.
            echo_pool=True,
            # Only reuse connections for up to 10 minutes to avoid failures due
            # to stale connections. Cloud SQL will close connections that have
            # been stale for 10 minutes.
            # https://cloud.google.com/sql/docs/postgres/diagnose-issues#compute-engine
            pool_recycle=600,
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
            with monitoring.measurements(
                {
                    monitoring.TagKey.SCHEMA_TYPE: database_key.schema_type.value,
                    monitoring.TagKey.DATABASE_NAME: database_key.db_name,
                }
            ) as measurements:
                measurements.measure_int_put(m_failed_engine_initialization, 1)
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
    def init_engine(cls, database_key: SQLAlchemyDatabaseKey) -> Engine:
        return cls.init_engine_for_postgres_instance(
            database_key=database_key,
            db_url=cls.get_server_postgres_instance_url(database_key=database_key),
        )

    @classmethod
    def attempt_init_engines_for_server(cls, schema_types: Set[SchemaType]) -> None:
        """Attempts to initialize engines for the server for the given schema types.

        Ignores any connections that fail, so that a single down database does not cause
        our server to crash."""
        for database_key in SQLAlchemyDatabaseKey.all():
            if database_key.schema_type in schema_types:
                try:
                    cls.init_engine(database_key)
                except BaseException:
                    pass

    @classmethod
    def get_engine_for_database(
        cls, database_key: SQLAlchemyDatabaseKey
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
            cls.init_engine(database_key)
        return cls._engine_for_database.get(database_key, None)

    @classmethod
    def _secret_manager_prefix_for_type(cls, schema_type: SchemaType) -> str:
        if schema_type == SchemaType.JAILS:
            return "sqlalchemy"
        if schema_type == SchemaType.STATE:
            return "state"
        if schema_type == SchemaType.OPERATIONS:
            return "operations"
        if schema_type == SchemaType.JUSTICE_COUNTS:
            return "justice_counts"
        if schema_type == SchemaType.CASE_TRIAGE:
            return "case_triage"

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
    def _get_db_password(cls, *, database_key: SQLAlchemyDatabaseKey) -> str:
        secret_key = f"{cls._secret_manager_prefix(database_key)}_db_password"
        password = secrets.get_secret(secret_key)
        if password is None:
            raise ValueError(
                f"Unable to retrieve database password for key [{secret_key}]"
            )
        return password

    @classmethod
    def _get_db_migration_password(cls, *, database_key: SQLAlchemyDatabaseKey) -> str:
        secret_key = f"{cls._secret_manager_prefix(database_key)}_db_migration_password"
        password = secrets.get_secret(secret_key)
        if password is None:
            raise ValueError(
                f"Unable to retrieve database migration password for key [{secret_key}]"
            )
        return password

    @classmethod
    def _get_db_migration_user(cls, *, database_key: SQLAlchemyDatabaseKey) -> str:
        secret_key = f"{cls._secret_manager_prefix(database_key)}_db_migration_user"
        user = secrets.get_secret(secret_key)
        if user is None:
            raise ValueError(
                f"Unable to retrieve database migration user for key [{secret_key}]"
            )
        return user

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
    def _get_db_user(cls, *, database_key: SQLAlchemyDatabaseKey) -> str:
        secret_key = f"{cls._secret_manager_prefix(database_key)}_db_user"
        user = secrets.get_secret(secret_key)
        if user is None:
            raise ValueError(f"Unable to retrieve database user for key [{secret_key}]")
        return user

    @classmethod
    def _get_cloudsql_instance_id_key(cls, schema_type: SchemaType) -> str:
        secret_manager_prefix = cls._secret_manager_prefix_for_type(schema_type)
        return f"{secret_manager_prefix}_cloudsql_instance_id"

    @classmethod
    def _get_full_cloudsql_instance_id(cls, schema_type: SchemaType) -> str:
        """Rerturns the full instance id stored in secrets with the form
        project_id:region:instance_id.

        For example:
            recidiviz-staging:us-east1:dev-state-data
        """

        instance_id_key = cls._get_cloudsql_instance_id_key(schema_type)
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
        instance_id_full = cls._get_full_cloudsql_instance_id(schema_type)

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
        instance_id_full = cls._get_full_cloudsql_instance_id(schema_type)

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
        ssl_cert_path: Optional[str] = None,
        migration_user: bool = False,
        readonly_user: bool = False,
    ) -> Dict[str, Optional[str]]:
        """Updates the appropriate env vars for SQLAlchemy to talk to the postgres instance associated with the schema.

        It returns the old set of env variables that were overridden.
        """
        if migration_user and readonly_user:
            raise ValueError("Both migration user and readonly user cannot be set")

        sqlalchemy_vars = [
            SQLALCHEMY_DB_NAME,
            SQLALCHEMY_DB_HOST,
            SQLALCHEMY_DB_USER,
            SQLALCHEMY_DB_PASSWORD,
            SQLALCHEMY_USE_SSL,
        ]
        original_values = {
            env_var: os.environ.get(env_var) for env_var in sqlalchemy_vars
        }

        # TODO(#6226): Delete the legacy db name keys out of Secrets Manager after this
        #   launches
        os.environ[SQLALCHEMY_DB_NAME] = database_key.db_name
        os.environ[SQLALCHEMY_DB_HOST] = cls._get_db_host(database_key=database_key)

        if readonly_user:
            os.environ[SQLALCHEMY_DB_USER] = cls._get_db_readonly_user(
                database_key=database_key
            )
            os.environ[SQLALCHEMY_DB_PASSWORD] = cls._get_db_readonly_password(
                database_key=database_key
            )
        elif migration_user:
            try:
                os.environ[SQLALCHEMY_DB_USER] = cls._get_db_migration_user(
                    database_key=database_key
                )
                os.environ[SQLALCHEMY_DB_PASSWORD] = cls._get_db_migration_password(
                    database_key=database_key
                )
            except ValueError:
                logging.info(
                    "No explicit migration user defined. Falling back to default user."
                )
                os.environ[SQLALCHEMY_DB_USER] = cls._get_db_user(
                    database_key=database_key
                )
                os.environ[SQLALCHEMY_DB_PASSWORD] = cls._get_db_password(
                    database_key=database_key
                )
        else:
            os.environ[SQLALCHEMY_DB_USER] = cls._get_db_user(database_key=database_key)
            os.environ[SQLALCHEMY_DB_PASSWORD] = cls._get_db_password(
                database_key=database_key
            )

        if ssl_cert_path is None:
            os.environ[SQLALCHEMY_USE_SSL] = "0"
        else:
            original_values[SQLALCHEMY_SSL_CERT_PATH] = os.environ.get(
                SQLALCHEMY_SSL_CERT_PATH
            )
            original_values[SQLALCHEMY_SSL_KEY_PATH] = os.environ.get(
                SQLALCHEMY_SSL_KEY_PATH
            )

            os.environ[SQLALCHEMY_USE_SSL] = "1"
            os.environ[SQLALCHEMY_SSL_CERT_PATH] = os.path.join(
                ssl_cert_path, "client-cert.pem"
            )
            os.environ[SQLALCHEMY_SSL_KEY_PATH] = os.path.join(
                ssl_cert_path, "client-key.pem"
            )

        return original_values

    @classmethod
    def get_server_postgres_instance_url(
        cls, *, database_key: SQLAlchemyDatabaseKey
    ) -> URL:
        schema_type = database_key.schema_type
        instance_id_key = cls._get_cloudsql_instance_id_key(schema_type)
        if instance_id_key is None:
            raise ValueError(
                f"Instance id is not configured for schema type [{schema_type}]"
            )

        db_user = cls._get_db_user(database_key=database_key)
        db_password = cls._get_db_password(database_key=database_key)
        db_name = database_key.db_name
        cloudsql_instance_id = secrets.get_secret(instance_id_key)
        db_name = database_key.db_name

        url = URL.create(
            drivername="postgresql",
            username=db_user,
            password=db_password,
            database=db_name,
            query={"host": f"/cloudsql/{cloudsql_instance_id}"},
        )

        return url

    @classmethod
    @environment.local_only
    def get_engine_for_database_with_ssl_certs(
        cls, *, database_key: SQLAlchemyDatabaseKey, ssl_cert_path: str
    ) -> Engine:
        db_user = cls._get_db_user(database_key=database_key)
        db_password = cls._get_db_password(database_key=database_key)
        host_name = cls._get_db_host(database_key=database_key)
        db_name = database_key.db_name

        url = URL.create(
            drivername="postgresql",
            username=db_user,
            password=db_password,
            host=host_name,
            database=db_name,
        )

        return create_engine(
            url,
            connect_args={
                "sslmode": "require",
                "sslcert": os.path.join(ssl_cert_path, "client-cert.pem"),
                "sslkey": os.path.join(ssl_cert_path, "client-key.pem"),
                "sslrootcert": os.path.join(ssl_cert_path, "server-ca.pem"),
            },
        )
