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
from enum import Enum
from typing import Optional, List

import attr
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.persistence.database import migrations
from recidiviz.persistence.database.schema_utils import (
    schema_type_to_schema_base,
    SchemaType,
)
from recidiviz.utils import environment

DEFAULT_DB_NAME = "postgres"


class SQLAlchemyStateDatabaseVersion(Enum):
    """Denotes a particular database for a given state."""

    # TODO(#6226): Once we have cut all traffic over to single-database traffic,
    #   delete the LEGACY type entirely.
    # The single, multi-state 'postgres' DB within the state CloudSQL instance for a
    # given project.
    LEGACY = "postgres"

    # For state US_XX, references the us_xx_primary DB within the state CloudSQL
    # instance for a given project.
    PRIMARY = "primary"

    # For state US_XX, references the us_xx_secondary DB within the state CloudSQL
    # instance for a given project.
    SECONDARY = "secondary"


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

    # Identifies which individual databse to connect to inside the instance
    db_name: str = attr.ib(default=DEFAULT_DB_NAME, validator=attr_validators.is_str)

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
        if self.schema_type in (SchemaType.STATE, SchemaType.JUSTICE_COUNTS):
            return "SERIALIZABLE"
        return None

    @classmethod
    def for_state_code(
        cls, state_code: StateCode, db_version: SQLAlchemyStateDatabaseVersion
    ) -> "SQLAlchemyDatabaseKey":
        """Returns they key to the database corresponding to the provided state code and
        database version.
        """
        # TODO(#6226): Once we have cut all traffic over to single-database traffic,
        # delete the LEGACY type entirely.
        if db_version == SQLAlchemyStateDatabaseVersion.LEGACY:
            db_name = DEFAULT_DB_NAME
        else:
            db_name = f"{state_code.value.lower()}_{db_version.value.lower()}"

        return cls(schema_type=SchemaType.STATE, db_name=db_name)

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
        return cls(schema_type)

    @classmethod
    def for_schema(cls, schema_type: SchemaType) -> "SQLAlchemyDatabaseKey":
        """A 'safe' version of |canonical_for_schema| that throws if the CloudSQL
        instance for the given schema has multiple databases.
        """
        if schema_type == SchemaType.STATE:
            raise ValueError(
                "Must provide db name information to create a STATE database key."
            )
        return cls(schema_type)

    @classmethod
    def all(cls) -> List["SQLAlchemyDatabaseKey"]:
        """Returns a list of keys for **all** databases across all instances that the
        application should connect to for a given project.
        """
        return [
            # This list includes database key that points at the default 'postgres'
            # database in the STATE CloudSQL instance. It can be used for operations
            # that are state agnostic.
            cls(schema_type)
            for schema_type in SchemaType
        ] + [
            # Keys for all state-specific databases
            cls.for_state_code(state_code, version)
            for version in SQLAlchemyStateDatabaseVersion
            for state_code in get_existing_direct_ingest_states()
            # The single "legacy" 'postgres' DB for the state schema instance is created
            # in the loop above.
            if version != SQLAlchemyStateDatabaseVersion.LEGACY
        ]
