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
"""Constants related to a MetricUnitOfAnalysisType."""
from enum import Enum
from typing import List, Optional

import attr

from recidiviz.common import attr_validators

# This object contains a mapping from original column names in Recidiviz's
# schema to abbreviated column names. Note that we do not rename state_code
# or facility, so those columns are omitted from this object.
COLUMN_SOURCE_DICT = {
    "district": "supervision_district",
    "district_name": "supervision_district_name",
    "office": "supervision_office",
    "office_name": "supervision_office_name",
    "officer_id": "supervising_officer_external_id",
}


class MetricUnitOfAnalysisType(Enum):
    """The type of metric unit of analysis level."""

    STATE_CODE = "STATE"
    FACILITY = "FACILITY"
    FACILITY_COUNSELOR = "FACILITY_COUNSELOR"
    SUPERVISION_DISTRICT = "DISTRICT"
    SUPERVISION_OFFICE = "OFFICE"
    SUPERVISION_OFFICER = "OFFICER"
    SUPERVISION_UNIT = "UNIT"
    PERSON_ID = "PERSON"


@attr.define(frozen=True, kw_only=True)
class MetricUnitOfAnalysis:
    """Class that stores information about a unit of analysis, along with functions to help generate SQL fragments"""

    # The level type object enum
    level_type: MetricUnitOfAnalysisType

    # The source query containing client assignments to the level of analysis
    client_assignment_query: str = attr.field(validator=attr_validators.is_str)

    # List of columns present in the assignment table that serve as the primary keys of the table
    primary_key_columns: List[str]

    # List of columns that provide information about the unit of analysis which does not change over time
    static_attribute_columns: List[str]

    @property
    def level_name_short(self) -> str:
        """Returns lowercase enum name"""
        return self.level_type.value.lower()

    @property
    def pretty_name(self) -> str:
        return self.level_name_short.replace("_", " ").title()

    @property
    def index_columns(self) -> List[str]:
        """Returns concatenated list of primary key and static attribute columns"""
        return self.primary_key_columns + self.static_attribute_columns

    def get_primary_key_columns_query_string(self, prefix: Optional[str] = None) -> str:
        """Returns string containing comma separated primary key column names with optional prefix"""
        prefix_str = f"{prefix}." if prefix else ""
        return ", ".join(f"{prefix_str}{column}" for column in self.primary_key_columns)

    def get_static_attribute_columns_query_string(
        self, prefix: Optional[str] = None
    ) -> str:
        """Returns string containing comma separated static attribute column names with optional prefix"""
        prefix_str = f"{prefix}." if prefix else ""
        return ", ".join(
            f"{prefix_str}{column}" for column in self.static_attribute_columns
        )

    def get_index_columns_query_string(self, prefix: Optional[str] = None) -> str:
        """Returns string containing comma separated index column names with optional prefix"""
        prefix_str = f"{prefix}." if prefix else ""
        return ", ".join(f"{prefix_str}{column}" for column in self.index_columns)

    def get_original_columns_query_string(self, prefix: Optional[str] = None) -> str:
        """Returns string containing comma separated column names from the source table with optional prefix"""
        prefix_str = f"{prefix}." if prefix else ""
        return ", ".join(
            f"{prefix_str}{COLUMN_SOURCE_DICT.get(column, column)}"
            for column in self.index_columns
        )

    def get_index_column_rename_query_string(self, prefix: Optional[str] = None) -> str:
        """
        Returns string containing comma separated column names from the source table aliased with new index columns
        where present in COLUMN_SOURCE_DICT, with optional prefix
        """
        prefix_str = f"{prefix}." if prefix else ""
        renamed_columns = [
            f"{prefix_str}{COLUMN_SOURCE_DICT.get(column, column)} AS {column}"
            for column in self.index_columns
        ]
        return ", ".join(renamed_columns)


METRIC_UNITS_OF_ANALYSIS_BY_TYPE = {
    MetricUnitOfAnalysisType.FACILITY: MetricUnitOfAnalysis(
        level_type=MetricUnitOfAnalysisType.FACILITY,
        client_assignment_query="SELECT * FROM `{project_id}.sessions.location_sessions_materialized`",
        primary_key_columns=["state_code", "facility"],
        static_attribute_columns=["facility_name"],
    ),
    MetricUnitOfAnalysisType.FACILITY_COUNSELOR: MetricUnitOfAnalysis(
        level_type=MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
        client_assignment_query="""
SELECT 
    a.* EXCEPT (incarceration_staff_assignment_id),
    a.incarceration_staff_assignment_id AS facility_counselor_id,
    b.full_name_clean AS facility_counselor_name
FROM
    `{project_id}.sessions.incarceration_staff_assignment_sessions_preprocessed_materialized` a
LEFT JOIN
    `{project_id}.reference_views.state_staff_with_names` b
ON
    a.state_code = b.state_code
    AND a.incarceration_staff_assignment_id = b.staff_id
WHERE
    incarceration_staff_assignment_role_subtype = "COUNSELOR"
""",
        primary_key_columns=["state_code", "facility_counselor_id"],
        static_attribute_columns=["facility_counselor_name"],
    ),
    MetricUnitOfAnalysisType.STATE_CODE: MetricUnitOfAnalysis(
        level_type=MetricUnitOfAnalysisType.STATE_CODE,
        client_assignment_query="SELECT * "
        "FROM `{project_id}.sessions.compartment_sessions_materialized`",
        primary_key_columns=["state_code"],
        static_attribute_columns=[],
    ),
    MetricUnitOfAnalysisType.SUPERVISION_DISTRICT: MetricUnitOfAnalysis(
        level_type=MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
        client_assignment_query="SELECT * "
        "FROM `{project_id}.sessions.location_sessions_materialized`",
        primary_key_columns=["state_code", "district"],
        static_attribute_columns=["district_name"],
    ),
    MetricUnitOfAnalysisType.SUPERVISION_OFFICE: MetricUnitOfAnalysis(
        level_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
        client_assignment_query="SELECT * "
        "FROM `{project_id}.sessions.location_sessions_materialized`",
        primary_key_columns=["state_code", "district", "office"],
        static_attribute_columns=["district_name", "office_name"],
    ),
    MetricUnitOfAnalysisType.SUPERVISION_UNIT: MetricUnitOfAnalysis(
        level_type=MetricUnitOfAnalysisType.SUPERVISION_UNIT,
        client_assignment_query="SELECT * "
        "FROM `{project_id}.sessions.supervision_unit_supervisor_sessions_materialized`",
        primary_key_columns=["state_code", "unit_supervisor"],
        static_attribute_columns=["unit_supervisor_name"],
    ),
    MetricUnitOfAnalysisType.SUPERVISION_OFFICER: MetricUnitOfAnalysis(
        level_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
        client_assignment_query="""
SELECT a.*, b.full_name_clean AS officer_name
FROM 
    `{project_id}.sessions.supervision_officer_sessions_materialized` a
LEFT JOIN 
    `{project_id}.reference_views.state_staff_with_names` b
ON 
    a.state_code = b.state_code
    AND a.supervising_officer_external_id = b.legacy_supervising_officer_external_id
""",
        primary_key_columns=["state_code", "officer_id"],
        static_attribute_columns=["officer_name"],
    ),
    MetricUnitOfAnalysisType.PERSON_ID: MetricUnitOfAnalysis(
        level_type=MetricUnitOfAnalysisType.PERSON_ID,
        client_assignment_query="SELECT * FROM `{project_id}.sessions.system_sessions_materialized`",
        primary_key_columns=["state_code", "person_id"],
        static_attribute_columns=[],
    ),
}
