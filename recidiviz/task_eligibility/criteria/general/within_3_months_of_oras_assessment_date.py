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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
# ============================================================================
"""
Creates spans of time when someone is eligible while they are within 3 months
of their ORAS assessment
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "WITHIN_3_MONTHS_OF_ORAS_ASSESSMENT_DATE"

_QUERY = f"""
WITH assessment_dates AS (
    SELECT
        state_code,
        person_id,
        assessment_date AS start_date,
        DATE_ADD(assessment_date, INTERVAL 3 MONTH) AS end_date,
        assessment_date,
        DATE_ADD(assessment_date, INTERVAL 3 MONTH) AS assessment_expiration_date,
        TRUE AS meets_criteria
    FROM `{{project_id}}.sessions.assessment_score_sessions_materialized`
    WHERE 
        assessment_type = "ORAS_COMMUNITY_SUPERVISION"
),
{create_sub_sessions_with_attributes('assessment_dates')},
dedup_cte AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        LOGICAL_OR(meets_criteria) AS meets_criteria,
        MAX(assessment_date) AS assessment_date,
        MAX(assessment_expiration_date) AS assessment_expiration_date,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    meets_criteria,
    TO_JSON(STRUCT(
        assessment_date AS assessment_date,
        assessment_expiration_date AS assessment_expiration_date
    )) AS reason,
    assessment_date,
    assessment_expiration_date,
FROM ({aggregate_adjacent_spans('dedup_cte', attribute=['assessment_date', 'meets_criteria', 
                                                                           'assessment_expiration_date'])})
"""

_REASONS_FIELDS = [
    ReasonsField(
        name="assessment_date",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="Date of the person's ORAS Community Supervision assessment",
    ),
    ReasonsField(
        name="assessment_expiration_date",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="Date 3 months after the ORAS assessment, when this criterion expires",
    ),
]

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        criteria_spans_query_template=_QUERY,
        reasons_fields=_REASONS_FIELDS,
        meets_criteria_default=False,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
