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
"""Defines an object that identifies a database within a CloudSQL instance, whose schema
is managed by SQLAlchemy.
"""

import os
from typing import Optional, Type

import attr
import sqlalchemy
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.common import attr_validators
from recidiviz.persistence.database import migrations
from recidiviz.persistence.database.schema_utils import (
    SchemaType,
    schema_type_to_schema_base,
)
from recidiviz.utils import environment

DEFAULT_DB_NAME = "postgres"


@attr.s
class SQLAlchemyPoolConfiguration:
    """Contains the settings for a SQLAlchemy connection pool. These settings are for QueuePools, which are the
    default connection pool types for a database."""

    # The number of persistent connections to be kept in the pool.
    pool_size: int = attr.ib(default=5)

    # The maximum number of overflow connections for the pool, once pool_size is used up.
    max_overflow: int = attr.ib(default=10)

    # The number of seconds to wait before giving up on returning a connection.
    pool_timeout: int = attr.ib(default=30)


@attr.s(frozen=True)
class SQLAlchemyDatabaseKey:
    """Contains information required to identify a single database within a CloudSQL
    instance. For many CloudSQL instances, there is only a single, default database
    (named 'postgres'), but for some instances, there may be multiple databases with
    the same schema but different data.

    This class assumes all databases within a particular instance are initialized with
    the same schema.
    """

    # Identifies which databse instance to connect to
    schema_type: SchemaType = attr.ib(validator=attr.validators.instance_of(SchemaType))

    # Identifies which individual database to connect to inside the instance
    db_name: str = attr.ib(validator=attr_validators.is_str)

    @property
    def alembic_file(self) -> str:
        """Path to the alembic file used to generate / run migrations on this database."""
        return os.path.join(
            os.path.dirname(migrations.__file__),
            f"{self.schema_type.value.lower()}_alembic.ini",
        )

    @property
    def migrations_location(self) -> str:
        """Path to the alembic migration files that should be run on this database."""
        return os.path.join(
            os.path.dirname(migrations.__file__), f"{self.schema_type.value.lower()}"
        )

    @property
    def declarative_meta(self) -> DeclarativeMeta:
        """The SQLAlchemy schema definition object for this database, e.g. StateBase."""
        return schema_type_to_schema_base(self.schema_type)

    @property
    def is_default_db(self) -> bool:
        return self.db_name == DEFAULT_DB_NAME

    @property
    def isolation_level(self) -> Optional[str]:
        # Set isolation level to SERIALIZABLE for states. This ensures that data read
        # during a transaction is still valid when the transaction is committed,
        # avoiding any inconsistency issues such as #2989. See the following for details
        # on transaction isolation guarantees within Postgres:
        # https://www.postgresql.org/docs/9.1/transaction-iso.html
        #
        # We opt for this over explicit locking to simplify our application logic. If
        # this causes performance issues we may reconsider. See
        # https://www.postgresql.org/docs/9.1/applevel-consistency.html.
        #
        # TODO(#3734): Consider doing this for all databases.
        if self.schema_type == SchemaType.STATE:
            return "SERIALIZABLE"
        return None

    @property
    def poolclass(self) -> Optional[Type[sqlalchemy.pool.Pool]]:
        # Don't pool connections for State databases. We only run a single
        # request at a time for a particular state database, so it is
        # unnecessary to maintain a connection from every copy of our
        # application to all databases all of the time. This does mean that
        # each request using a State database has to setup its own connection.
        if self.schema_type is SchemaType.STATE:
            return sqlalchemy.pool.NullPool
        return None

    @property
    def pool_configuration(self) -> Optional[SQLAlchemyPoolConfiguration]:
        if self.schema_type is SchemaType.OPERATIONS:
            # The operations database has many concurrent / distributed connections from various
            # endpoints. In addition, for pre-ingest, (N~100) database operations are performed
            # in serial in order to prevent unnecessary I/O. Given this read/write pattern, we
            # will lower the pool size and max overflow to ensure that connections get properly
            # closed / recycled. In addition, decreasing the timeout prevents long-held connections
            # during these synchronous operations.
            return SQLAlchemyPoolConfiguration(
                pool_size=2, max_overflow=5, pool_timeout=15
            )
        return None

    @classmethod
    @environment.local_only
    def canonical_for_schema(
        cls,
        schema_type: SchemaType,
    ) -> "SQLAlchemyDatabaseKey":
        """Returns the key to the default database for a given schema. This may have no
        data in it (e.g. in the STATE schema CloudSQL instance where the data will be
        written to state-specific databases and the default database will remain empty).

        This function may be used in tests where we don't care which DB we're using,
        or when generating migrations for a schema.
        """
        return cls(schema_type, db_name=DEFAULT_DB_NAME)

    @classmethod
    def for_schema(cls, schema_type: SchemaType) -> "SQLAlchemyDatabaseKey":
        """A 'safe' version of |canonical_for_schema| that throws if the CloudSQL
        instance for the given schema has multiple databases.
        """
        if schema_type.is_multi_db_schema:
            raise ValueError(
                f"Must provide db name information to create a {schema_type.name} "
                f"database key."
            )
        return cls(schema_type, db_name=DEFAULT_DB_NAME)
