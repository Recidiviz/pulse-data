# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Defines a criterion span view that shows spans of time during which someone's
assessed risk level was `LOW` after starting on the 'INTAKE' supervision level. This
criterion considers all assessments in the 'RISK' class and is not specific to the type
of assessment.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "ASSESSED_RISK_LOW_AFTER_INTAKE_SUPERVISION_LEVEL"

# TODO(#34751): Decide how to handle assessments with null `assessment_level` values.
_QUERY_TEMPLATE = f"""
    WITH risk_level_spans AS (
        SELECT
            state_code,
            person_id,
            assessment_type,
            assessment_level,
            assessment_date,
            score_end_date_exclusive,
        FROM `{{project_id}}.sessions.risk_assessment_score_sessions_materialized`
    ),
    intake_sessions AS (
        SELECT 
            state_code,
            person_id,
            start_date,
            start_date AS intake_start_date,
            end_date_exclusive,
        FROM `{{project_id}}.sessions.supervision_level_sessions_materialized`
        WHERE supervision_level = 'INTAKE'
    )
    SELECT
        i.state_code,
        i.person_id,
        r.assessment_date AS start_date,
        -- Span ends when either there's another score or the intake span ends
        -- Use nonnull_end_date_clause because we don't want to subtract 1 day from these dates
        LEAST(
            {nonnull_end_date_clause("r.score_end_date_exclusive")},
            {nonnull_end_date_clause("i.end_date_exclusive")}
        ) AS end_date,
        (assessment_level='LOW') AS meets_criteria,
        assessment_date,
        intake_start_date,
        assessment_type,
        assessment_level,
        TO_JSON(STRUCT(
            assessment_date,
            intake_start_date,
            assessment_type,
            assessment_level
        )) AS reason,
    FROM intake_sessions i
    INNER JOIN risk_level_spans r
        ON i.state_code = r.state_code
        AND i.person_id = r.person_id
        -- restrict to assessments happening after the start of an intake period
        AND r.assessment_date BETWEEN i.start_date AND {nonnull_end_date_exclusive_clause('i.end_date_exclusive')}
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        meets_criteria_default=False,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=[
            ReasonsField(
                name="assessment_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Assessment date",
            ),
            ReasonsField(
                name="intake_start_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Start date of latest intake session",
            ),
            ReasonsField(
                name="assessment_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Assessment type",
            ),
            ReasonsField(
                name="assessment_level",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Assessed risk level",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
