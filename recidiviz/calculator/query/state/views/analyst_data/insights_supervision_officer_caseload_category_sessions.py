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
"""Sessionized view assigning officers to a caseload category within a category type based on
their specialized caseload type"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.calculator.query.state.views.analyst_data.insights_caseload_category_sessions import (
    InsightsCaseloadCategoryType,
    get_caseload_category_query_fragment,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "insights_supervision_officer_caseload_category_sessions"
_VIEW_DESCRIPTION = """Sessionized view assigning officers to a caseload category within a category type based on
their specialized caseload type"""


def _get_insights_supervision_officer_caseload_category_sessions_view_builder() -> (
    SimpleBigQueryViewBuilder
):
    """Generates a SimpleBigQueryViewBuilder that associates officers with their insights
    caseload type category, for every configured category type."""
    unioned_query_fragments = "\n    UNION ALL\n".join(
        [
            f"""
    SELECT
        state_code,
        officer_id,
        start_date,
        end_date_exclusive,
        {get_caseload_category_query_fragment(category_type)} AS caseload_category,
        "{category_type.name}" AS category_type,
    FROM
        caseload_type_sessions
"""
            for category_type in InsightsCaseloadCategoryType
        ]
    )
    query_template = f"""
WITH caseload_type_sessions AS (
    SELECT
        state_code,
        officer_id,
        specialized_caseload_type_primary,
        specialized_caseload_type_array,
        start_date,
        end_date_exclusive
    FROM
        `{{project_id}}.sessions.supervision_staff_attribute_sessions_materialized`
),
# Categorize caseload types
caseload_category_type_sessions AS (
    {unioned_query_fragments}
)
{aggregate_adjacent_spans(
    table_name="caseload_category_type_sessions", 
    index_columns=["state_code", "officer_id"],
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


INSIGHTS_SUPERVISION_OFFICER_CASELOAD_CATEGORY_SESSIONS_VIEW_BUILDER = (
    _get_insights_supervision_officer_caseload_category_sessions_view_builder()
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INSIGHTS_SUPERVISION_OFFICER_CASELOAD_CATEGORY_SESSIONS_VIEW_BUILDER.build_and_print()
