# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""SchemaType enumerations for the database schemas."""
import enum


@enum.unique
class SchemaType(enum.Enum):
    STATE = "STATE"
    OPERATIONS = "OPERATIONS"
    JUSTICE_COUNTS = "JUSTICE_COUNTS"
    CASE_TRIAGE = "CASE_TRIAGE"
    PATHWAYS = "PATHWAYS"
    OUTLIERS = "OUTLIERS"
    WORKFLOWS = "WORKFLOWS"

    @property
    def is_multi_db_schema(self) -> bool:
        """Returns True if this schema is segmented into multiple databases"""
        return self in [
            SchemaType.STATE,
            SchemaType.PATHWAYS,
            SchemaType.OUTLIERS,
            SchemaType.WORKFLOWS,
        ]

    @property
    def has_cloud_sql_instance(self) -> bool:
        """Returns True if this schema is ever loaded into a deployed CloudSQL instance."""
        # TODO(#20930): Update to return False for STATE before we delete the instance.
        return True
