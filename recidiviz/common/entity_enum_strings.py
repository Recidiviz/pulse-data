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
# =============================================================================
"""Canonical string values shared across all EntityEnum subclasses.

NOTE: Changing ANY STRING VALUE in this file will require a database migration. The Python
values pointing to the strings can be renamed without issue.

SQLAlchemy represents SQL enums as strings, and uses the string representation to pass values
to the database. This means any change to the string values requires a database migration.
Therefore in order to keep the code as flexible as possible, the string values should never be
used directly. Storing the strings in this file and only referring to them by their values here
allows us to structure the application layer code any way we want, while only requiring the
database to be updated when an enum value is created or removed.
"""

from typing import Dict

# This value should be used ONLY in cases where the external data source
# explicitly specifies a value as "unknown". It should NOT be treated as a
# default value for enums that are not provided (which should be represented
# with None/NULL).
external_unknown = "EXTERNAL_UNKNOWN"

# This value is used when the external data source specifies a known value for
# an enum field, but we internally don't have an enum value that it should map
# to. This should NOT be treated as a default value for enums fields that are
# not provided.
internal_unknown = "INTERNAL_UNKNOWN"

# This value is used by status enums to denote that no status for an entity was
# provided by the source, but the entity itself was found in the source.
present_without_info = "PRESENT_WITHOUT_INFO"

SHARED_ENUM_VALUE_DESCRIPTIONS: Dict[str, str] = {
    internal_unknown: """This value is used when the external data source specifies a known value for an enum field, but we internally don't have an enum value that it should map to. This should NOT be treated as a default value for enums fields that are not provided.""",
    external_unknown: """This value is used ONLY in cases where the external data source explicitly specifies a value as "unknown". It should NOT be treated as a default value for enums that are not provided (which should be represented with None/NULL).""",
    present_without_info: """This value is used by status enums to denote that no status for an entity was provided by the source, but the entity itself was found in the source.""",
}
