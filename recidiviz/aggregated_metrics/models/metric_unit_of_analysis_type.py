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
from typing import Dict

import attr

from recidiviz.big_query.big_query_view_column import (
    BigQueryViewColumn,
    Bool,
    Date,
    Integer,
    String,
)
from recidiviz.segment.product_type import ProductType

_STATE_CODE_COLUMN = String(
    name="state_code",
    description="The U.S. state code for the unit being analyzed.",
    mode="REQUIRED",
)


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

    # List of columns (with explicit types and descriptions) that serve as the
    # primary keys of a table containing information about the unit of analysis.
    primary_key_columns: list[BigQueryViewColumn]

    # List of columns (with explicit types and descriptions) that provide
    # information about the unit of analysis which does not change over time.
    static_attribute_columns: list[BigQueryViewColumn]

    @property
    def primary_key_column_names(self) -> list[str]:
        """Provides primary key column names in a stable order."""
        return [c.name for c in self.primary_key_columns]

    @property
    def static_attribute_column_names(self) -> list[str]:
        """Provides static attribute column names in a stable order."""
        return [c.name for c in self.static_attribute_columns]

    @property
    def index_columns(self) -> list[BigQueryViewColumn]:
        """Returns concatenated list of primary key and static attribute columns."""
        return self.primary_key_columns + self.static_attribute_columns

    @property
    def index_column_names(self) -> list[str]:
        """Returns concatenated list of primary key and static attribute column names."""
        return self.primary_key_column_names + self.static_attribute_column_names

    def get_primary_key_columns_query_string(self, prefix: str | None = None) -> str:
        """Returns string containing comma separated primary key column names with optional prefix"""
        prefix_str = f"{prefix}." if prefix else ""
        return ", ".join(
            f"{prefix_str}{column}" for column in sorted(self.primary_key_column_names)
        )

    def get_static_attribute_columns_query_string(
        self, prefix: str | None = None
    ) -> str:
        """Returns string containing comma separated static attribute column names with optional prefix"""
        prefix_str = f"{prefix}." if prefix else ""
        return ", ".join(
            f"{prefix_str}{column}" for column in self.static_attribute_column_names
        )

    def get_index_columns_query_string(self, prefix: str | None = None) -> str:
        """Returns string containing comma separated index column names with optional prefix"""
        prefix_str = f"{prefix}." if prefix else ""
        return ", ".join(f"{prefix_str}{column}" for column in self.index_column_names)

    @classmethod
    def for_type(
        cls, metric_unit_of_analysis_type: MetricUnitOfAnalysisType
    ) -> "MetricUnitOfAnalysis":

        match metric_unit_of_analysis_type:
            case MetricUnitOfAnalysisType.FACILITY:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.FACILITY,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        String(
                            name="facility",
                            description="The facility code for the facility being analyzed.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[
                        String(
                            name="facility_name",
                            description="The display name of the facility.",
                            mode="NULLABLE",
                        ),
                    ],
                )
            case MetricUnitOfAnalysisType.FACILITY_COUNSELOR:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        Integer(
                            name="facility_counselor_id",
                            description="The Recidiviz internal staff id for the facility counselor being analyzed.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[
                        String(
                            name="facility_counselor_name",
                            description="The name of the facility counselor.",
                            mode="NULLABLE",
                        ),
                        String(
                            name="facility_counselor_email_address",
                            description="The email address of the facility counselor.",
                            mode="NULLABLE",
                        ),
                    ],
                )
            case MetricUnitOfAnalysisType.INSIGHTS_CASELOAD_CATEGORY:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.INSIGHTS_CASELOAD_CATEGORY,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        String(
                            name="caseload_category",
                            description="The Insights caseload category being analyzed.",
                            mode="REQUIRED",
                        ),
                        String(
                            name="category_type",
                            description="The type of the Insights caseload category.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.STATE_CODE:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.STATE_CODE,
                    primary_key_columns=[_STATE_CODE_COLUMN],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.SUPERVISION_DISTRICT:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        String(
                            name="district",
                            description="The supervision district code for the district being analyzed.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[
                        String(
                            name="district_name",
                            description="The display name of the supervision district.",
                            mode="NULLABLE",
                        ),
                    ],
                )
            case MetricUnitOfAnalysisType.SUPERVISION_OFFICE:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        String(
                            name="district",
                            description="The supervision district code for the office being analyzed.",
                            mode="REQUIRED",
                        ),
                        String(
                            name="office",
                            description="The supervision office code for the office being analyzed.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[
                        String(
                            name="district_name",
                            description="The display name of the supervision district.",
                            mode="NULLABLE",
                        ),
                        String(
                            name="office_name",
                            description="The display name of the supervision office.",
                            mode="NULLABLE",
                        ),
                    ],
                )
            case MetricUnitOfAnalysisType.SUPERVISION_UNIT:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.SUPERVISION_UNIT,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        Integer(
                            name="unit_supervisor",
                            description="The Recidiviz internal staff id for the unit supervisor being analyzed.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[
                        String(
                            name="unit_supervisor_name",
                            description="The name of the unit supervisor.",
                            mode="NULLABLE",
                        ),
                        String(
                            name="unit_supervisor_email_address",
                            description="The email address of the unit supervisor.",
                            mode="NULLABLE",
                        ),
                    ],
                )
            case MetricUnitOfAnalysisType.SUPERVISION_OFFICER:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        String(
                            name="officer_id",
                            description="The external id for the supervision officer being analyzed.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[
                        String(
                            name="officer_name",
                            description="The name of the supervision officer.",
                            mode="NULLABLE",
                        ),
                        String(
                            name="officer_email_address",
                            description="The email address of the supervision officer.",
                            mode="NULLABLE",
                        ),
                        Integer(
                            name="staff_id",
                            description="The Recidiviz internal staff id for the supervision officer.",
                            mode="NULLABLE",
                        ),
                    ],
                )
            case (
                MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL
            ):
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        String(
                            name="officer_id",
                            description="The external id for the supervision officer being analyzed.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[
                        String(
                            name="officer_name",
                            description="The name of the supervision officer.",
                            mode="NULLABLE",
                        ),
                        String(
                            name="officer_email_address",
                            description="The email address of the supervision officer.",
                            mode="NULLABLE",
                        ),
                        Integer(
                            name="staff_id",
                            description="The Recidiviz internal staff id for the supervision officer.",
                            mode="NULLABLE",
                        ),
                    ],
                )
            case MetricUnitOfAnalysisType.WORKFLOWS_CASELOAD:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.WORKFLOWS_CASELOAD,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        String(
                            name="caseload_id",
                            description="The id for the Workflows caseload being analyzed.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.LOCATION_DETAIL:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.LOCATION_DETAIL,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        String(
                            name="location_detail_id",
                            description="The id for the location detail being analyzed.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.LOCATION:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.LOCATION,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        String(
                            name="location_name",
                            description="The name of the location being analyzed.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.PERSON_ID:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.PERSON_ID,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        Integer(
                            name="person_id",
                            description="The Recidiviz internal person id for the person being analyzed.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.ALL_STATES:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.ALL_STATES,
                    primary_key_columns=[
                        Bool(
                            name="in_signed_state",
                            description="Whether the entity is in a signed state.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.EXPERIMENT_VARIANT:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.EXPERIMENT_VARIANT,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        String(
                            name="experiment_id",
                            description="The id for the experiment.",
                            mode="REQUIRED",
                        ),
                        String(
                            name="variant_id",
                            description="The id for the experiment variant.",
                            mode="REQUIRED",
                        ),
                        Date(
                            name="variant_date",
                            description="The date of the experiment variant.",
                            mode="REQUIRED",
                        ),
                        Bool(
                            name="is_treated",
                            description="Whether the variant is treated.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.WORKFLOWS_PROVISIONED_USER:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.WORKFLOWS_PROVISIONED_USER,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        String(
                            name="email_address",
                            description="The email address for the Workflows provisioned user being analyzed.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[
                        Integer(
                            name="staff_id",
                            description="The Recidiviz internal staff id for the provisioned user.",
                            mode="NULLABLE",
                        ),
                        String(
                            name="user_full_name",
                            description="The full name of the provisioned user.",
                            mode="NULLABLE",
                        ),
                    ],
                )
            case MetricUnitOfAnalysisType.INSIGHTS_PROVISIONED_USER:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.INSIGHTS_PROVISIONED_USER,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        String(
                            name="email_address",
                            description="The email address for the Insights provisioned user being analyzed.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[
                        Integer(
                            name="staff_id",
                            description="The Recidiviz internal staff id for the provisioned user.",
                            mode="NULLABLE",
                        ),
                        String(
                            name="user_full_name",
                            description="The full name of the provisioned user.",
                            mode="NULLABLE",
                        ),
                    ],
                )
            case MetricUnitOfAnalysisType.OFFICER_OUTLIER_USAGE_COHORT:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.OFFICER_OUTLIER_USAGE_COHORT,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        Date(
                            name="cohort_month_end_date",
                            description="The end date of the cohort month.",
                            mode="REQUIRED",
                        ),
                        String(
                            name="metric_id",
                            description="The id of the metric for the outlier usage cohort.",
                            mode="REQUIRED",
                        ),
                        String(
                            name="outlier_usage_cohort",
                            description="The outlier usage cohort designation.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[],
                )
            case MetricUnitOfAnalysisType.GLOBAL_PROVISIONED_USER:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.GLOBAL_PROVISIONED_USER,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        String(
                            name="email_address",
                            description="The email address for the globally provisioned user being analyzed.",
                            mode="REQUIRED",
                        ),
                    ],
                    static_attribute_columns=[
                        Integer(
                            name="staff_id",
                            description="The Recidiviz internal staff id for the provisioned user.",
                            mode="NULLABLE",
                        ),
                        String(
                            name="user_full_name",
                            description="The full name of the provisioned user.",
                            mode="NULLABLE",
                        ),
                    ],
                )
            case MetricUnitOfAnalysisType.PRODUCT_ACCESS:
                return MetricUnitOfAnalysis(
                    type=MetricUnitOfAnalysisType.PRODUCT_ACCESS,
                    primary_key_columns=[
                        _STATE_CODE_COLUMN,
                        *[
                            Bool(
                                name=f"is_provisioned_{product_type.pretty_name}",
                                description=f"Whether the user is provisioned for {product_type.display_name}.",
                                mode="REQUIRED",
                            )
                            for product_type in ProductType
                        ],
                        *[
                            Bool(
                                name=f"is_primary_user_{product_type.pretty_name}",
                                description=f"Whether the user is a primary user of {product_type.display_name}.",
                                mode="REQUIRED",
                            )
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
    MetricUnitOfAnalysisType.GLOBAL_PROVISIONED_USER: """

SELECT
    state_code,
    LOWER(email) AS email_address,
    staff_id,
    full_name_clean AS user_full_name,
FROM
    `{project_id}.reference_views.state_staff_with_names`
""",
    MetricUnitOfAnalysisType.WORKFLOWS_PROVISIONED_USER: """

SELECT
    state_code,
    LOWER(email) AS email_address,
    staff_id,
    full_name_clean AS user_full_name,
FROM
    `{project_id}.reference_views.state_staff_with_names`
""",
    MetricUnitOfAnalysisType.INSIGHTS_PROVISIONED_USER: """

SELECT
    state_code,
    LOWER(email) AS email_address,
    staff_id,
    full_name_clean AS user_full_name,
FROM
    `{project_id}.reference_views.state_staff_with_names`
""",
}


def get_static_attributes_query_for_unit_of_analysis(
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    bq_view: bool | None = True,
) -> str | None:
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
