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
import enum
import logging
import os.path
from typing import Any, Dict, Optional, List

import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.persistence.database import SQLALCHEMY_DB_NAME, SQLALCHEMY_DB_HOST, SQLALCHEMY_DB_USER, \
    SQLALCHEMY_DB_PASSWORD, SQLALCHEMY_USE_SSL, SQLALCHEMY_SSL_CERT_PATH, SQLALCHEMY_SSL_KEY_PATH
from recidiviz.persistence.database import migrations
from recidiviz.persistence.database.base_schema import JailsBase, \
    StateBase, OperationsBase, JusticeCountsBase, CaseTriageBase
from recidiviz.persistence.database import migrations
from recidiviz.utils import secrets, environment


@enum.unique
class SchemaType(enum.Enum):
    JAILS = 'JAILS'
    STATE = 'STATE'
    OPERATIONS = 'OPERATIONS'
    JUSTICE_COUNTS = 'JUSTICE_COUNTS'
    CASE_TRIAGE = 'CASE_TRIAGE'


class SQLAlchemyEngineManager:
    """A class to manage all SQLAlchemy Engines for our database instances."""

    _engine_for_schema: Dict[DeclarativeMeta, Engine] = {}

    _SCHEMA_TO_SECRET_MANAGER_PREFIX: Dict[SchemaType, str] = {
        SchemaType.JAILS: 'sqlalchemy',
        SchemaType.STATE: 'state',
        SchemaType.OPERATIONS: 'operations',
        SchemaType.JUSTICE_COUNTS: 'justice_counts',
        SchemaType.CASE_TRIAGE: 'case_triage',
    }

    _SCHEMA_TO_DECLARATIVE_META: Dict[SchemaType, DeclarativeMeta] = {
        SchemaType.JAILS: JailsBase,
        SchemaType.STATE: StateBase,
        SchemaType.OPERATIONS: OperationsBase,
        SchemaType.JUSTICE_COUNTS: JusticeCountsBase,
        SchemaType.CASE_TRIAGE: CaseTriageBase,
    }

    @classmethod
    def init_engine_for_postgres_instance(
            cls,
            db_url: str,
            schema_base: DeclarativeMeta) -> None:
        """Initializes a sqlalchemy Engine object for the given Postgres database / schema and caches it for future use.
        """
        cls.init_engine_for_db_instance(
            db_url,
            schema_base,
            # Only reuse connections for up to 10 minutes to avoid failures due to stale connections. Cloud SQL will
            # close connections that have been stale for 10 minutes.
            # https://cloud.google.com/sql/docs/postgres/diagnose-issues#compute-engine
            pool_recycle=600
        )

    @classmethod
    def init_engine_for_db_instance(
            cls,
            db_url: str,
            schema_base: DeclarativeMeta,
            **dialect_specific_kwargs: Any) -> None:
        """Initializes a sqlalchemy Engine object for the given database / schema and caches it for future use."""

        if schema_base in cls._engine_for_schema:
            raise ValueError(f'Already initialized schema [{schema_base.__name__}]')

        engine = sqlalchemy.create_engine(
            db_url,
            isolation_level=SQLAlchemyEngineManager.get_isolation_level(schema_base),
            **dialect_specific_kwargs)
        schema_base.metadata.create_all(engine)
        cls._engine_for_schema[schema_base] = engine

    @staticmethod
    def get_isolation_level(schema_base: DeclarativeMeta) -> Optional[str]:
        # Set isolation level to SERIALIZABLE for states. This ensures that data read during a transaction is still
        # valid when the transaction is committed, avoiding any inconsistency issues such as #2989. See the following
        # for details on transaction isolation guarantees within Postgres:
        # https://www.postgresql.org/docs/9.1/transaction-iso.html
        #
        # We opt for this over explicit locking to simplify our application logic. If this causes performance issues
        # we may reconsider. See https://www.postgresql.org/docs/9.1/applevel-consistency.html.
        #
        # TODO(#3734): Consider doing this for all databases.
        if schema_base in (StateBase, JusticeCountsBase):
            return 'SERIALIZABLE'
        return None

    @classmethod
    def teardown_engine_for_schema(cls, declarative_base: DeclarativeMeta) -> None:
        cls._engine_for_schema.pop(declarative_base).dispose()

    @classmethod
    def teardown_engines(cls) -> None:
        for engine in cls._engine_for_schema.values():
            engine.dispose()
        cls._engine_for_schema.clear()

    @classmethod
    def init_engine(cls, schema_type: SchemaType) -> None:
        cls.init_engine_for_postgres_instance(
            db_url=cls._get_server_postgres_instance_url(schema_type=schema_type),
            schema_base=cls._SCHEMA_TO_DECLARATIVE_META[schema_type])

    @classmethod
    def init_engines_for_server_postgres_instances(cls) -> None:
        if not environment.in_gae():
            logging.info(
                "Environment is not GAE, not connecting to postgres instances.")
            return

        cls.init_engine(SchemaType.JAILS)
        cls.init_engine(SchemaType.STATE)
        cls.init_engine(SchemaType.OPERATIONS)
        cls.init_engine(SchemaType.JUSTICE_COUNTS)

    @classmethod
    def declarative_method_for_schema(cls, schema_type: SchemaType) -> DeclarativeMeta:
        return cls._SCHEMA_TO_DECLARATIVE_META[schema_type]

    @classmethod
    def get_alembic_file(cls, schema_type: SchemaType) -> str:
        return os.path.join(os.path.dirname(migrations.__file__), f'{schema_type.value.lower()}_alembic.ini')

    @classmethod
    def get_migrations_location(cls, schema_type: SchemaType) -> str:
        return os.path.join(os.path.dirname(migrations.__file__), f'{schema_type.value.lower()}')

    @classmethod
    def get_engine_for_schema_base(
            cls, schema_base: DeclarativeMeta) -> Optional[Engine]:
        return cls._engine_for_schema.get(schema_base, None)

    @classmethod
    def get_db_host_key(cls, schema_type: SchemaType) -> str:
        return f'{cls._SCHEMA_TO_SECRET_MANAGER_PREFIX[schema_type]}_db_host'

    @classmethod
    def get_db_host(cls, schema_type: SchemaType) -> str:
        host = secrets.get_secret(cls.get_db_host_key(schema_type))
        if host is None:
            raise ValueError(f"Unable to retrieve database host for schema type [{schema_type}]")
        return host

    @classmethod
    def get_db_name_key(cls, schema_type: SchemaType) -> str:
        return f'{cls._SCHEMA_TO_SECRET_MANAGER_PREFIX[schema_type]}_db_name'

    @classmethod
    def get_db_name(cls, schema_type: SchemaType) -> str:
        db_name = secrets.get_secret(cls.get_db_name_key(schema_type))
        if db_name is None:
            raise ValueError(f"Unable to retrieve database name for schema type [{schema_type}]")
        return db_name

    @classmethod
    def get_db_password_key(cls, schema_type: SchemaType) -> str:
        return f'{cls._SCHEMA_TO_SECRET_MANAGER_PREFIX[schema_type]}_db_password'

    @classmethod
    def get_db_password(cls, schema_type: SchemaType) -> str:
        password = secrets.get_secret(cls.get_db_password_key(schema_type))
        if password is None:
            raise ValueError(f"Unable to retrieve database password for schema type [{schema_type}]")
        return password

    @classmethod
    def get_db_migration_password_key(cls, schema_type: SchemaType) -> str:
        return f'{cls._SCHEMA_TO_SECRET_MANAGER_PREFIX[schema_type]}_db_migration_password'

    @classmethod
    def get_db_migration_password(cls, schema_type: SchemaType) -> str:
        password = secrets.get_secret(cls.get_db_migration_password_key(schema_type))
        if password is None:
            raise ValueError(f"Unable to retrieve database migration password for schema type [{schema_type}]")
        return password

    @classmethod
    def get_db_migration_user_key(cls, schema_type: SchemaType) -> str:
        return f'{cls._SCHEMA_TO_SECRET_MANAGER_PREFIX[schema_type]}_db_migration_user'

    @classmethod
    def get_db_migration_user(cls, schema_type: SchemaType) -> str:
        user = secrets.get_secret(cls.get_db_migration_user_key(schema_type))
        if user is None:
            raise ValueError(f"Unable to retrieve database migration user for schema type [{schema_type}]")
        return user

    @classmethod
    def get_db_readonly_password_key(cls, schema_type: SchemaType) -> str:
        return f'{cls._SCHEMA_TO_SECRET_MANAGER_PREFIX[schema_type]}_db_readonly_password'

    @classmethod
    def get_db_readonly_password(cls, schema_type: SchemaType) -> str:
        password = secrets.get_secret(cls.get_db_readonly_password_key(schema_type))
        if password is None:
            raise ValueError(f"Unable to retrieve database readonly password for schema type [{schema_type}]")
        return password

    @classmethod
    def get_db_readonly_user_key(cls, schema_type: SchemaType) -> str:
        return f'{cls._SCHEMA_TO_SECRET_MANAGER_PREFIX[schema_type]}_db_readonly_user'

    @classmethod
    def get_db_readonly_user(cls, schema_type: SchemaType) -> str:
        user = secrets.get_secret(cls.get_db_readonly_user_key(schema_type))
        if user is None:
            raise ValueError(f"Unable to retrieve database readonly user for schema type [{schema_type}]")
        return user

    @classmethod
    def get_db_user_key(cls, schema_type: SchemaType) -> str:
        return f'{cls._SCHEMA_TO_SECRET_MANAGER_PREFIX[schema_type]}_db_user'

    @classmethod
    def get_db_user(cls, schema_type: SchemaType) -> str:
        user = secrets.get_secret(cls.get_db_user_key(schema_type))
        if user is None:
            raise ValueError(f"Unable to retrieve database user for schema type [{schema_type}]")
        return user

    @classmethod
    def get_cloudsql_instance_id_key(cls, schema_type: SchemaType) -> str:
        return f'{cls._SCHEMA_TO_SECRET_MANAGER_PREFIX[schema_type]}_cloudsql_instance_id'

    @classmethod
    def get_stripped_cloudsql_instance_id(
            cls, schema_type: SchemaType) -> Optional[str]:
        """The full instance id stored in secrets has the form project_id:zone:instance_id.

        This returns just the final instance_id, for example, 'dev-data' or 'prod-state-data'. If a key is not
        configured for the given schema, returns None.

        Should be used when using the sqladmin_client().
        """
        instance_id_key = cls.get_cloudsql_instance_id_key(schema_type)
        if instance_id_key is None:
            return None
        instance_id_full = secrets.get_secret(instance_id_key)

        if instance_id_full is None:
            raise ValueError(f"Unable to retrieve instance id for schema type [{schema_type}]")

        # Remove Project ID and Zone information from Cloud SQL instance ID.
        # Expected format "project_id:zone:instance_id"
        instance_id = instance_id_full.split(':')[-1]

        return instance_id

    @classmethod
    def get_all_stripped_cloudsql_instance_ids(cls) -> List[str]:
        """Returns stripped instance ids for all sql instances in this project.

        See get_stripped_cloudsql_instance_id() for more info.
        """
        ids_for_all_schemas = [cls.get_stripped_cloudsql_instance_id(schema_type) for schema_type in SchemaType]
        return [instance_id for instance_id in ids_for_all_schemas if instance_id is not None]

    @classmethod
    def update_sqlalchemy_env_vars(cls, schema_type: SchemaType,
                                   ssl_cert_path: Optional[str] = None,
                                   migration_user: bool = False,
                                   readonly_user: bool = False) -> Dict[str, Optional[str]]:
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
        original_values = {env_var: os.environ.get(env_var) for env_var in sqlalchemy_vars}

        os.environ[SQLALCHEMY_DB_NAME] = cls.get_db_name(schema_type)
        os.environ[SQLALCHEMY_DB_HOST] = cls.get_db_host(schema_type)

        if readonly_user:
            os.environ[SQLALCHEMY_DB_USER] = cls.get_db_readonly_user(schema_type)
            os.environ[SQLALCHEMY_DB_PASSWORD] = cls.get_db_readonly_password(schema_type)
        elif migration_user:
            try:
                os.environ[SQLALCHEMY_DB_USER] = cls.get_db_migration_user(schema_type)
                os.environ[SQLALCHEMY_DB_PASSWORD] = cls.get_db_migration_password(schema_type)
            except ValueError:
                logging.info('No explicit migration user defined. Falling back to default user.')
                os.environ[SQLALCHEMY_DB_USER] = cls.get_db_user(schema_type)
                os.environ[SQLALCHEMY_DB_PASSWORD] = cls.get_db_password(schema_type)
        else:
            os.environ[SQLALCHEMY_DB_USER] = cls.get_db_user(schema_type)
            os.environ[SQLALCHEMY_DB_PASSWORD] = cls.get_db_password(schema_type)

        if ssl_cert_path is None:
            os.environ[SQLALCHEMY_USE_SSL] = '0'
        else:
            original_values[SQLALCHEMY_SSL_CERT_PATH] = os.environ.get(SQLALCHEMY_SSL_CERT_PATH)
            original_values[SQLALCHEMY_SSL_KEY_PATH] = os.environ.get(SQLALCHEMY_SSL_KEY_PATH)

            os.environ[SQLALCHEMY_USE_SSL] = '1'
            os.environ[SQLALCHEMY_SSL_CERT_PATH] = os.path.join(ssl_cert_path, 'client-cert.pem')
            os.environ[SQLALCHEMY_SSL_KEY_PATH] = os.path.join(ssl_cert_path, 'client-key.pem')

        return original_values

    @classmethod
    def _get_server_postgres_instance_url(cls, *, schema_type: SchemaType) -> str:
        instance_id_key = cls.get_cloudsql_instance_id_key(schema_type)
        if instance_id_key is None:
            raise ValueError(f'Instance id is not configured for schema type [{schema_type}]')

        db_user = secrets.get_secret(f'{cls._SCHEMA_TO_SECRET_MANAGER_PREFIX[schema_type]}_db_user')
        db_password = secrets.get_secret(f'{cls._SCHEMA_TO_SECRET_MANAGER_PREFIX[schema_type]}_db_password')
        db_name = cls.get_db_name(schema_type)
        cloudsql_instance_id = secrets.get_secret(instance_id_key)

        sqlalchemy_url = ('postgresql://{db_user}:{db_password}@/{db_name}'
                          '?host=/cloudsql/{cloudsql_instance_id}').format(
                              db_user=db_user,
                              db_password=db_password,
                              db_name=db_name,
                              cloudsql_instance_id=cloudsql_instance_id)
        return sqlalchemy_url
