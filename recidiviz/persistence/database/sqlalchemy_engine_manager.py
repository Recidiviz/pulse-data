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
from typing import Any, Dict, Optional, List

import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.persistence.database.base_schema import JailsBase, \
    StateBase, OperationsBase, JusticeCountsBase
from recidiviz.utils import secrets, environment

@enum.unique
class SchemaType(enum.Enum):
    JAILS = 'JAILS'
    STATE = 'STATE'
    OPERATIONS = 'OPERATIONS'
    JUSTICE_COUNTS = 'JUSTICE_COUNTS'


class SQLAlchemyEngineManager:
    """A class to manage all SQLAlchemy Engines for our database instances."""

    _engine_for_schema: Dict[DeclarativeMeta, Engine] = {}

    # An instance id can be None if the schema exists but there is not yet a cloud sql instance for it. In that case
    # the app will skip over trying to connect to it.
    _SCHEMA_TO_INSTANCE_ID_KEY: Dict[SchemaType, Optional[str]] = {
        SchemaType.JAILS: 'cloudsql_instance_id',
        SchemaType.STATE: 'state_cloudsql_instance_id',
        SchemaType.OPERATIONS: 'operations_cloudsql_instance_id',
        SchemaType.JUSTICE_COUNTS: None,
    }

    _SCHEMA_TO_DB_NAME_KEY: Dict[SchemaType, str] = {
        SchemaType.JAILS: 'sqlalchemy_db_name',
        SchemaType.STATE: 'state_db_name',
        SchemaType.OPERATIONS: 'operations_db_name',
        SchemaType.JUSTICE_COUNTS: 'justice_counts_db_name',
    }

    @classmethod
    def init_engine_for_postgres_instance(
            cls,
            db_url: str,
            schema_base: DeclarativeMeta)  -> None:
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
    def init_engines_for_server_postgres_instances(cls) -> None:
        if not environment.in_gae():
            logging.info(
                "Environment is not GAE, not connecting to postgres instances.")
            return

        # Initialize Jails database instance
        cls.init_engine_for_postgres_instance(
            db_url=cls._get_jails_server_postgres_instance_url(),
            schema_base=JailsBase)

        # Initialize State database instance
        cls.init_engine_for_postgres_instance(
            db_url=cls._get_state_server_postgres_instance_url(),
            schema_base=StateBase)

        # Initialize Operations database instance
        cls.init_engine_for_postgres_instance(
            db_url=cls._get_operations_server_postgres_instance_url(),
            schema_base=OperationsBase)

        # TODO(#4175): Add Justice Counts database instance

    @classmethod
    def get_engine_for_schema_base(
            cls, schema_base: DeclarativeMeta) -> Optional[Engine]:
        return cls._engine_for_schema.get(schema_base, None)

    @classmethod
    def get_db_name_key(cls, schema_type: SchemaType) -> str:
        return cls._SCHEMA_TO_DB_NAME_KEY[schema_type]

    @classmethod
    def get_db_name(cls, schema_type: SchemaType) -> str:
        db_name = secrets.get_secret(cls.get_db_name_key(schema_type))
        if db_name is None:
            raise ValueError(f"Unable to retrieve database name for schema type [{schema_type}]")
        return db_name

    @classmethod
    def get_cloudsql_instance_id_key(
            cls, schema_type: SchemaType) -> Optional[str]:
        return cls._SCHEMA_TO_INSTANCE_ID_KEY[schema_type]

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
    def _get_state_server_postgres_instance_url(cls) -> str:
        return cls._get_server_postgres_instance_url(
            db_user_key='state_db_user',
            db_password_key='state_db_password',
            schema_type=SchemaType.STATE)

    @classmethod
    def _get_jails_server_postgres_instance_url(cls) -> str:
        return cls._get_server_postgres_instance_url(
            db_user_key='sqlalchemy_db_user',
            db_password_key='sqlalchemy_db_password',
            schema_type=SchemaType.JAILS)

    @classmethod
    def _get_operations_server_postgres_instance_url(cls) -> str:
        return cls._get_server_postgres_instance_url(
            db_user_key='operations_db_user',
            db_password_key='operations_db_password',
            schema_type=SchemaType.OPERATIONS)

    @classmethod
    def _get_server_postgres_instance_url(cls,
                                          *,
                                          db_user_key: str,
                                          db_password_key: str,
                                          schema_type: SchemaType) -> str:
        instance_id_key = cls.get_cloudsql_instance_id_key(schema_type)
        if instance_id_key is None:
            raise ValueError(f'Instance id is not configured for schema type [{schema_type}]')

        db_user = secrets.get_secret(db_user_key)
        db_password = secrets.get_secret(db_password_key)
        db_name = secrets.get_secret(cls.get_db_name(schema_type))
        cloudsql_instance_id = secrets.get_secret(instance_id_key)

        sqlalchemy_url = ('postgresql://{db_user}:{db_password}@/{db_name}'
                          '?host=/cloudsql/{cloudsql_instance_id}').format(
                              db_user=db_user,
                              db_password=db_password,
                              db_name=db_name,
                              cloudsql_instance_id=cloudsql_instance_id)
        return sqlalchemy_url
