# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Sessionized view assigning clients to a caseload type category based on
the specialized caseload type of their supervising officer"""
from enum import Enum

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_intersection_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.state.state_staff_caseload_type import (
    StateStaffCaseloadType,
)

_VIEW_NAME = "insights_caseload_category_sessions"
_VIEW_DESCRIPTION = """Sessionized view assigning clients to a caseload type category based on
the specialized caseload type of their supervising officer"""


class InsightsCaseloadCategoryType(Enum):
    """Types of caseload categorization logic supported by Insights"""

    SEX_OFFENSE_BINARY = "SEX_OFFENSE_BINARY"


def get_caseload_category_query_fragment(
    category_type: InsightsCaseloadCategoryType,
) -> str:
    """Returns a query fragment that returns caseload category using StateStaffCaseloadType values
    based on the specified InsightsCaseloadCategoryType"""

    # Specify the CASE-WHEN logic for each InsightsCaseloadCategoryType
    if category_type == InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY:
        query_fragment = f"""
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        CASE
            # Only categorize as SEX_OFFENSE if SEX_OFFENSE is present in person's caseload type array
            WHEN "{StateStaffCaseloadType.SEX_OFFENSE.name}" IN UNNEST(specialized_caseload_type_array)
            THEN "{StateStaffCaseloadType.SEX_OFFENSE.name}"

            # If caseload type information is missing, categorize as "NOT_SEX_OFFENSE"
            WHEN specialized_caseload_type_primary IS NULL
            THEN "NOT_{StateStaffCaseloadType.SEX_OFFENSE.name}"

            # All other non-null caseload types are categorized as "NOT_SEX_OFFENSE"
            ELSE "NOT_{StateStaffCaseloadType.SEX_OFFENSE.name}"
        END AS caseload_category,
        "{category_type.name}" AS category_type,
    FROM
        intersection_spans
"""
    else:
        raise TypeError(
            f"Caseload categorization has not been configured for {category_type.name} category type."
        )
    return query_fragment


def get_insights_caseload_category_sessions_view_builder() -> SimpleBigQueryViewBuilder:
    """Generates a SimpleBigQueryViewBuilder that associates clients with the insights
    caseload type category of their officer, for every configured category type."""
    unioned_query_fragments = "\n    UNION ALL\n".join(
        [
            get_caseload_category_query_fragment(category_type)
            for category_type in InsightsCaseloadCategoryType
        ]
    )
    query_template = f"""
WITH officer_sessions AS (
    SELECT
        state_code,
        supervising_officer_external_id AS officer_id,
        person_id,
        start_date,
        end_date_exclusive,
    FROM 
        `{{project_id}}.sessions.supervision_officer_sessions_materialized`
),
caseload_type_sessions AS (
    SELECT
        state_code,
        officer_id,
        specialized_caseload_type_primary,
        specialized_caseload_type_array,
        start_date,
        end_date_exclusive
    FROM
        `{{project_id}}.sessions.supervision_staff_attribute_sessions_materialized`
)
,
intersection_spans AS (
{create_intersection_spans(
    table_1_name="officer_sessions",
    table_2_name="caseload_type_sessions",
    index_columns=["state_code", "officer_id"],
    table_1_columns=["person_id"],
    table_2_columns=["specialized_caseload_type_array", "specialized_caseload_type_primary"],
    use_left_join=True,
)}
)
,
# Categorize caseload types
caseload_category_type_sessions AS (
    {unioned_query_fragments}
)
,
# Overlapping sessions with the same caseload category are possible here if a
# client has overlapping officer sessions where officers fall under the same
# caseload category. So we sub-sessionize and re-aggregate to account for
# potentially duplicated sessions.

{create_sub_sessions_with_attributes(
    table_name="caseload_category_type_sessions",
    index_columns=["state_code", "person_id"],
    end_date_field_name="end_date_exclusive",
)}
,
sub_sessions_dedup AS (
    SELECT DISTINCT
        state_code,
        person_id,
        caseload_category,
        category_type,
        start_date,
        end_date_exclusive,
    FROM
        sub_sessions_with_attributes
)
{aggregate_adjacent_spans(
    table_name="sub_sessions_with_attributes", 
    index_columns=["state_code", "person_id"], 
    attribute=["caseload_category", "category_type"],
    end_date_field_name="end_date_exclusive"
)}
"""
    return SimpleBigQueryViewBuilder(
        dataset_id=ANALYST_VIEWS_DATASET,
        view_id=_VIEW_NAME,
        description=_VIEW_DESCRIPTION,
        view_query_template=query_template,
        clustering_fields=["state_code", "category_type"],
        should_materialize=True,
    )
