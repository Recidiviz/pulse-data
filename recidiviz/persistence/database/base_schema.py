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
# ============================================================================
"""Defines base classes for each of the database schemas."""

from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta

from recidiviz.persistence.database.database_entity import DatabaseEntity

# Defines the base class for all table classes for the Jails schema.
# For actual schema definitions, see /aggregate/schema.py and /county/schema.py.
JailsBase: DeclarativeMeta = declarative_base(cls=DatabaseEntity)


# Defines the base class for all table classes in the state schema.
# For actual schema definitions, see /state/schema.py.
StateBase: DeclarativeMeta = declarative_base(cls=DatabaseEntity)
