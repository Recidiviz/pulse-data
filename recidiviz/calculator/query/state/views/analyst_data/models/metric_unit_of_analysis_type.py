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
from typing import Dict, FrozenSet, List, Optional, Tuple

import attr


class MetricUnitOfAnalysisType(Enum):
    """A unit of analysis is the entity that you wish to say something about at the end
    of your study.

    The MetricUnitOfAnalysisType tells us the type of entity that a metric describes.
    For example, a metric that counts currently caseload size for each officer would use
    MetricUnitOfAnalysisType.SUPERVISION_OFFICER.
    """

    STATE_CODE = "STATE"
    FACILITY = "FACILITY"
    FACILITY_COUNSELOR = "FACILITY_COUNSELOR"
    SUPERVISION_DISTRICT = "DISTRICT"
    SUPERVISION_OFFICE = "OFFICE"
    SUPERVISION_OFFICER = "OFFICER"
    SUPERVISION_UNIT = "UNIT"
    PERSON_ID = "PERSON"

    @property
    def short_name(self) -> str:
        """Returns lowercase enum name"""
        return self.value.lower()

    @property
    def pretty_name(self) -> str:
        """Returns enum name in title case"""
        return self.short_name.replace("_", " ").title()


class MetricUnitOfObservationType(Enum):
    """A unit of observation is the item (or items) that you observe, measure, or
    collect while trying to learn something about your unit of analysis.

    The MetricUnitOfObservationType is a type that tells us what each input event / span
    to a metric is about. For example, compartment_sessions rows are each about a single
    person, so the MetricUnitOfObservationType is PERSON.
    """

    SUPERVISION_OFFICER = "OFFICER"
    PERSON_ID = "PERSON"

    @property
    def short_name(self) -> str:
        """Returns lowercase enum name"""
        return self.value.lower()


@attr.define(frozen=True, kw_only=True)
class MetricUnitOfObservation:
    """Class that stores information about a unit of observation, along with functions
    to help generate SQL fragments.
    """

    # The enum for the type of unit of observation
    type: MetricUnitOfObservationType

    # List of columns that serve as the primary keys of a table containing information about the unit
    primary_key_columns: FrozenSet[str]

    def get_primary_key_columns_query_string(self, prefix: Optional[str] = None) -> str:
        """Returns string containing comma separated primary key column names with optional prefix"""
        prefix_str = f"{prefix}." if prefix else ""
        return ", ".join(
            f"{prefix_str}{column}" for column in sorted(self.primary_key_columns)
        )


@attr.define(frozen=True, kw_only=True)
class MetricUnitOfAnalysis:
    """Class that stores information about a unit of analysis, along with functions to
    help generate SQL fragments.
    """

    # The enum for the type of unit of analysis
    type: MetricUnitOfAnalysisType

    # List of columns present in the assignment table that serve as the primary keys of the table
    primary_key_columns: List[str]

    # List of columns that provide information about the unit of analysis which does not change over time
    static_attribute_columns: List[str]

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


METRIC_UNITS_OF_OBSERVATION = [
    MetricUnitOfObservation(
        type=MetricUnitOfObservationType.SUPERVISION_OFFICER,
        primary_key_columns=frozenset(["state_code", "officer_id"]),
    ),
    MetricUnitOfObservation(
        type=MetricUnitOfObservationType.PERSON_ID,
        primary_key_columns=frozenset(["state_code", "person_id"]),
    ),
]

METRIC_UNITS_OF_OBSERVATION_BY_TYPE = {u.type: u for u in METRIC_UNITS_OF_OBSERVATION}


METRIC_UNITS_OF_ANALYSIS = [
    MetricUnitOfAnalysis(
        type=MetricUnitOfAnalysisType.FACILITY,
        primary_key_columns=["state_code", "facility"],
        static_attribute_columns=["facility_name"],
    ),
    MetricUnitOfAnalysis(
        type=MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
        primary_key_columns=["state_code", "facility_counselor_id"],
        static_attribute_columns=["facility_counselor_name"],
    ),
    MetricUnitOfAnalysis(
        type=MetricUnitOfAnalysisType.STATE_CODE,
        primary_key_columns=["state_code"],
        static_attribute_columns=[],
    ),
    MetricUnitOfAnalysis(
        type=MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
        primary_key_columns=["state_code", "district"],
        static_attribute_columns=["district_name"],
    ),
    MetricUnitOfAnalysis(
        type=MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
        primary_key_columns=["state_code", "district", "office"],
        static_attribute_columns=["district_name", "office_name"],
    ),
    MetricUnitOfAnalysis(
        type=MetricUnitOfAnalysisType.SUPERVISION_UNIT,
        primary_key_columns=["state_code", "unit_supervisor"],
        static_attribute_columns=["unit_supervisor_name"],
    ),
    MetricUnitOfAnalysis(
        type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
        primary_key_columns=["state_code", "officer_id"],
        static_attribute_columns=["officer_name"],
    ),
    MetricUnitOfAnalysis(
        type=MetricUnitOfAnalysisType.PERSON_ID,
        primary_key_columns=["state_code", "person_id"],
        static_attribute_columns=[],
    ),
]

METRIC_UNITS_OF_ANALYSIS_BY_TYPE = {u.type: u for u in METRIC_UNITS_OF_ANALYSIS}

# Dictionary of queries define periods of assignment of a unit of observation to a unit of analysis
UNIT_OF_ANALYSIS_ASSIGNMENT_QUERIES_DICT: Dict[
    Tuple[MetricUnitOfObservationType, MetricUnitOfAnalysisType], str
] = {
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.PERSON_ID,
    ): "SELECT * FROM `{project_id}.sessions.system_sessions_materialized`",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
    ): """
SELECT *, supervising_officer_external_id AS officer_id
FROM `{project_id}.sessions.supervision_officer_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
    ): """SELECT
    *, 
    supervision_district AS district,
    supervision_office AS office,
FROM
    `{project_id}.sessions.location_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
    ): "SELECT *, supervision_district AS district, FROM `{project_id}.sessions.location_sessions_materialized`",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.SUPERVISION_UNIT,
    ): "SELECT * FROM `{project_id}.sessions.supervision_unit_supervisor_sessions_materialized`",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.STATE_CODE,
    ): "SELECT * FROM `{project_id}.sessions.compartment_sessions_materialized`",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.FACILITY,
    ): "SELECT * FROM `{project_id}.sessions.location_sessions_materialized`",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
    ): """
SELECT 
    * EXCEPT (incarceration_staff_assignment_id),
    incarceration_staff_assignment_id AS facility_counselor_id,
FROM
    `{project_id}.sessions.incarceration_staff_assignment_sessions_preprocessed_materialized`
WHERE
    incarceration_staff_assignment_role_subtype = "COUNSELOR"
""",
    (
        MetricUnitOfObservationType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
    ): """SELECT * FROM `{project_id}.sessions.supervision_officer_attribute_sessions_materialized`""",
    (
        MetricUnitOfObservationType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
    ): """SELECT
    *, 
    supervision_office_id AS office,
    supervision_district_id AS district,
FROM
    `{project_id}.sessions.supervision_officer_attribute_sessions_materialized`""",
    (
        MetricUnitOfObservationType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
    ): """SELECT
    *, supervision_district_id AS district,
FROM
    `{project_id}.sessions.supervision_officer_attribute_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.SUPERVISION_UNIT,
    ): """SELECT
    *, 
    supervisor_staff_id AS unit_supervisor,
FROM
    `{project_id}.sessions.supervision_officer_attribute_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.STATE_CODE,
    ): """SELECT * FROM `{project_id}.sessions.supervision_officer_attribute_sessions_materialized`""",
}

UNIT_OF_ANALYSIS_STATIC_ATTRIBUTE_COLS_QUERY_DICT: Dict[
    MetricUnitOfAnalysisType, str
] = {
    MetricUnitOfAnalysisType.FACILITY: """
SELECT DISTINCT
    state_code,
    facility,
    facility_name,
FROM
    `{project_id}.sessions.session_location_names_materialized`
""",
    MetricUnitOfAnalysisType.FACILITY_COUNSELOR: """
SELECT
    state_code,
    staff_id AS facility_counselor_id,
    full_name_clean AS facility_counselor_name,
FROM
    `{project_id}.reference_views.state_staff_with_names`
""",
    MetricUnitOfAnalysisType.SUPERVISION_DISTRICT: """
SELECT DISTINCT
    state_code,
    supervision_district AS district,
    supervision_district_name AS district_name,
FROM
    `{project_id}.sessions.session_location_names_materialized`
""",
    MetricUnitOfAnalysisType.SUPERVISION_OFFICE: """
SELECT DISTINCT
    state_code,
    supervision_district AS district,
    supervision_district_name AS district_name,
    supervision_office AS office,
    supervision_office_name AS office_name,
FROM
    `{project_id}.sessions.session_location_names_materialized`
""",
    MetricUnitOfAnalysisType.SUPERVISION_OFFICER: """
SELECT
    a.state_code,
    b.external_id AS officer_id,
    a.full_name_clean AS officer_name,
FROM
    `{project_id}.reference_views.state_staff_with_names` a
INNER JOIN
    `{project_id}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` b
USING
    (staff_id)
""",
    MetricUnitOfAnalysisType.SUPERVISION_UNIT: """
SELECT
    state_code,
    staff_id AS unit_supervisor,
    full_name_clean AS unit_supervisor_name,
FROM
    `{project_id}.reference_views.state_staff_with_names`
""",
}


def get_assignment_query_for_unit_of_analysis(
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    unit_of_observation_type: MetricUnitOfObservationType,
) -> str:
    """Returns the assignment query that associates a unit of analysis with its assigned
    units of observation.
    """
    unit_pair_key = (unit_of_observation_type, unit_of_analysis_type)
    if unit_pair_key not in UNIT_OF_ANALYSIS_ASSIGNMENT_QUERIES_DICT:
        raise ValueError(
            f"No assignment query found for {unit_of_analysis_type=}, "
            f"{unit_of_observation_type=}."
        )
    return UNIT_OF_ANALYSIS_ASSIGNMENT_QUERIES_DICT[unit_pair_key]


def get_static_attributes_query_for_unit_of_analysis(
    unit_of_analysis_type: MetricUnitOfAnalysisType,
) -> Optional[str]:
    """Returns the query that associates a unit of analysis with its static attribute columns"""
    if unit_of_analysis_type in UNIT_OF_ANALYSIS_STATIC_ATTRIBUTE_COLS_QUERY_DICT:
        return UNIT_OF_ANALYSIS_STATIC_ATTRIBUTE_COLS_QUERY_DICT[unit_of_analysis_type]
    return None
