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
from typing import Dict, List, Optional

import attr

from recidiviz.segment.product_type import ProductType


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
    INSIGHTS_CASELOAD_CATEGORY = "INSIGHTS_CASELOAD_CATEGORY"
    SUPERVISION_DISTRICT = "DISTRICT"
    SUPERVISION_OFFICE = "OFFICE"
    SUPERVISION_OFFICER = "OFFICER"
    SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL = (
        "OFFICER_OR_PREVIOUS_IF_TRANSITIONAL"
    )
    SUPERVISION_UNIT = "UNIT"
    WORKFLOWS_CASELOAD = "WORKFLOWS_CASELOAD"
    LOCATION = "LOCATION"
    LOCATION_DETAIL = "LOCATION_DETAIL"
    PERSON_ID = "PERSON"
    ALL_STATES = "ALL_STATES"
    EXPERIMENT_VARIANT = "EXPERIMENT_VARIANT"
    WORKFLOWS_PROVISIONED_USER = "WORKFLOWS_PROVISIONED_USER"
    INSIGHTS_PROVISIONED_USER = "INSIGHTS_PROVISIONED_USER"
    OFFICER_OUTLIER_USAGE_COHORT = "OFFICER_OUTLIER_USAGE_COHORT"
    GLOBAL_PROVISIONED_USER = "GLOBAL_PROVISIONED_USER"
    PRODUCT_ACCESS = "PRODUCT_ACCESS"

    @property
    def short_name(self) -> str:
        """Returns lowercase enum name"""
        return self.value.lower()

    @property
    def pretty_name(self) -> str:
        """Returns enum name in title case"""
        return self.short_name.replace("_", " ").title()


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
        return ", ".join(
            f"{prefix_str}{column}" for column in sorted(self.primary_key_columns)
        )

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

    @classmethod
    def for_type(
        cls, metric_unit_of_analysis_type: MetricUnitOfAnalysisType
    ) -> "MetricUnitOfAnalysis":

        match metric_unit_of_analysis_type:
            case MetricUnitOfAnalysisType.FACILITY:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.FACILITY,
                    primary_key_columns=["state_code", "facility"],
                    static_attribute_columns=["facility_name"],
                )
            case MetricUnitOfAnalysisType.FACILITY_COUNSELOR:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
                    primary_key_columns=["state_code", "facility_counselor_id"],
                    static_attribute_columns=[
                        "facility_counselor_name",
                        "facility_counselor_email_address",
                    ],
                )
            case MetricUnitOfAnalysisType.INSIGHTS_CASELOAD_CATEGORY:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.INSIGHTS_CASELOAD_CATEGORY,
                    primary_key_columns=[
                        "state_code",
                        "caseload_category",
                        "category_type",
                    ],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.STATE_CODE:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.STATE_CODE,
                    primary_key_columns=["state_code"],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.SUPERVISION_DISTRICT:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
                    primary_key_columns=["state_code", "district"],
                    static_attribute_columns=["district_name"],
                )
            case MetricUnitOfAnalysisType.SUPERVISION_OFFICE:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
                    primary_key_columns=["state_code", "district", "office"],
                    static_attribute_columns=["district_name", "office_name"],
                )
            case MetricUnitOfAnalysisType.SUPERVISION_UNIT:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.SUPERVISION_UNIT,
                    primary_key_columns=["state_code", "unit_supervisor"],
                    static_attribute_columns=[
                        "unit_supervisor_name",
                        "unit_supervisor_email_address",
                    ],
                )
            case MetricUnitOfAnalysisType.SUPERVISION_OFFICER:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
                    primary_key_columns=["state_code", "officer_id"],
                    static_attribute_columns=[
                        "officer_name",
                        "officer_email_address",
                        "staff_id",
                    ],
                )
            case (
                MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL
            ):
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
                    primary_key_columns=["state_code", "officer_id"],
                    static_attribute_columns=[
                        "officer_name",
                        "officer_email_address",
                        "staff_id",
                    ],
                )
            case MetricUnitOfAnalysisType.WORKFLOWS_CASELOAD:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.WORKFLOWS_CASELOAD,
                    primary_key_columns=["state_code", "caseload_id"],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.LOCATION_DETAIL:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.LOCATION_DETAIL,
                    primary_key_columns=["state_code", "location_detail_id"],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.LOCATION:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.LOCATION,
                    primary_key_columns=["state_code", "location_name"],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.PERSON_ID:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.PERSON_ID,
                    primary_key_columns=["state_code", "person_id"],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.ALL_STATES:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.ALL_STATES,
                    primary_key_columns=["in_signed_state"],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.EXPERIMENT_VARIANT:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.EXPERIMENT_VARIANT,
                    primary_key_columns=[
                        "state_code",
                        "experiment_id",
                        "variant_id",
                        "variant_date",
                        "is_treated",
                    ],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.WORKFLOWS_PROVISIONED_USER:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.WORKFLOWS_PROVISIONED_USER,
                    primary_key_columns=["state_code", "email_address"],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.INSIGHTS_PROVISIONED_USER:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.INSIGHTS_PROVISIONED_USER,
                    primary_key_columns=["state_code", "email_address"],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.OFFICER_OUTLIER_USAGE_COHORT:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.OFFICER_OUTLIER_USAGE_COHORT,
                    primary_key_columns=[
                        "state_code",
                        "cohort_month_end_date",
                        "metric_id",
                        "outlier_usage_cohort",
                    ],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.GLOBAL_PROVISIONED_USER:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.GLOBAL_PROVISIONED_USER,
                    primary_key_columns=["state_code", "email_address"],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.PRODUCT_ACCESS:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.PRODUCT_ACCESS,
                    primary_key_columns=[
                        "state_code",
                        *[
                            f"is_provisioned_{product_type.pretty_name}"
                            for product_type in ProductType
                        ],
                        *[
                            f"is_primary_user_{product_type.pretty_name}"
                            for product_type in ProductType
                        ],
                    ],
                    static_attribute_columns=[],
                )


_UNIT_OF_ANALYSIS_STATIC_ATTRIBUTE_COLS_QUERY_DICT: Dict[
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
    LOWER(email) AS facility_counselor_email_address,
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
    LOWER(email) AS officer_email_address,
    staff_id,
FROM
    `{project_id}.reference_views.state_staff_with_names` a
INNER JOIN
    `{project_id}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` b
USING
    (staff_id)
""",
    MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL: """
SELECT
    a.state_code,
    b.external_id AS officer_id,
    a.full_name_clean AS officer_name,
    LOWER(email) AS officer_email_address,
    staff_id,
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
    LOWER(email) AS unit_supervisor_email_address,
FROM
    `{project_id}.reference_views.state_staff_with_names`
""",
}


def get_static_attributes_query_for_unit_of_analysis(
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    bq_view: Optional[bool] = True,
) -> Optional[str]:
    """
    Returns the query that associates a unit of analysis with its static attribute columns.
    If bq_view is True, includes the `{project_id}` prefix in all view addresses; otherwise,
    removes prefix because we assume the view will be referenced in Looker, where project id is set elsewhere.
    """
    if unit_of_analysis_type in _UNIT_OF_ANALYSIS_STATIC_ATTRIBUTE_COLS_QUERY_DICT:
        query = _UNIT_OF_ANALYSIS_STATIC_ATTRIBUTE_COLS_QUERY_DICT[
            unit_of_analysis_type
        ]
        if not bq_view:
            query = query.replace("{project_id}.", "")
        return query
    return None
