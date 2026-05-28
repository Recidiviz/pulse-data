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
import enum
from typing import Any

import sqlalchemy as sa


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
