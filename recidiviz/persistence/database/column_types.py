# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Custom SQLAlchemy column types shared across database schemas."""
import datetime
import enum
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Dialect


class StringBackedEnum(sa.Enum):
    """An Enum column type that stores its value as a String, thus constraing values to
    an enum while not requiring migrations when enum values are added or removed.

    This class returns an sa.Enum so that the resulting Alembic migrations don't need
    to know about the custom type.
    """

    def __new__(cls, enum_type: type[enum.Enum], **kwargs: Any) -> sa.Enum:
        kwargs.setdefault("native_enum", False)
        kwargs.setdefault("create_constraint", False)
        # Set fixed width so that adding or removing enum values never resizes the
        # underlying VARCHAR and thus never requires a migration.
        kwargs.setdefault("length", 255)
        # Reject non-member values at write time, since there is no DB-level constraint.
        kwargs.setdefault("validate_strings", True)
        return sa.Enum(enum_type, **kwargs)


class UTCDateTime(sa.types.TypeDecorator):
    """A DateTime column whose values are UTC-aware at the application layer.

    Values are stored as a naive TIMESTAMP, and output as timezone-aware UTC datetimes.
    """

    impl = sa.DateTime
    cache_ok = True

    def process_bind_param(  # pylint: disable=unused-argument
        self, value: datetime.datetime | None, dialect: Dialect
    ) -> datetime.datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError(
                f"UTCDateTime column requires a timezone-aware datetime, but got "
                f"naive value [{value}]"
            )
        return value.astimezone(datetime.timezone.utc).replace(tzinfo=None)

    def process_result_value(  # pylint: disable=unused-argument
        self, value: datetime.datetime | None, dialect: Dialect
    ) -> datetime.datetime | None:
        if value is None:
            return None
        return value.replace(tzinfo=datetime.timezone.utc)

    def process_literal_param(  # pylint: disable=unused-argument
        self, value: datetime.datetime | None, dialect: Dialect
    ) -> str:
        return str(value)

    @property
    def python_type(self) -> type:
        return datetime.datetime
