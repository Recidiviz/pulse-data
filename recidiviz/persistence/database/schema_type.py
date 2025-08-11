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
    # TODO(#32860): Delete STATE when we delete state/schema.py
    STATE = "STATE"
    OPERATIONS = "OPERATIONS"
    JUSTICE_COUNTS = "JUSTICE_COUNTS"
    CASE_TRIAGE = "CASE_TRIAGE"
    PATHWAYS = "PATHWAYS"
    WORKFLOWS = "WORKFLOWS"
    INSIGHTS = "INSIGHTS"
    RESOURCE_SEARCH = "RESOURCE_SEARCH"

    @property
    def is_multi_db_schema(self) -> bool:
        """Returns True if this schema is segmented into multiple databases"""
        return self in [
            SchemaType.STATE,
            SchemaType.PATHWAYS,
            SchemaType.WORKFLOWS,
            SchemaType.INSIGHTS,
            SchemaType.RESOURCE_SEARCH,
        ]

    @property
    def has_cloud_sql_instance(self) -> bool:
        """Returns True if this schema is ever loaded into a deployed CloudSQL instance."""
        return self is not SchemaType.STATE
