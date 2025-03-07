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
assessed risk level was `LOW` after Unassigned supervision level
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

_CRITERIA_NAME = "ASSESSED_RISK_LOW_AFTER_UNASSIGNED_SUPERVISION_LEVEL"

# TODO(#39124): This criteria currently creates overlapping spans for TX because of upstream issues in how
# assessment data is ingested and should not be used without resolving that (either in this criteria or a state
# specific one)
_QUERY_TEMPLATE = f"""
    WITH risk_level_spans AS (
        SELECT 
            state_code,
            person_id,
            assessment_date,
            score_end_date_exclusive,
            assessment_level,
        FROM `{{project_id}}.sessions.assessment_score_sessions_materialized`
        WHERE assessment_class='RISK'
        /* TODO(#34751): Decide how to handle assessments with null
            `assessment_level` values. Note that the following filter will currently
            drop these sessions. This means that, if someone went from 'LOW' to NULL and
            back to 'LOW', that NULL session would interrupt the clock and force a
            restart on accruing time toward meeting this criterion. We should check to
            see if this is always the behavior we want, just in case there are scenarios
            where we think the NULL session actually shouldn't have that effect. */
            AND assessment_level='LOW'
    ),
    unassigned_sessions AS (
        SELECT 
           state_code,
           person_id,
           start_date,
           start_date AS unassigned_start_date,
           end_date_exclusive,
        FROM `{{project_id}}.sessions.supervision_level_sessions_materialized` sl
        WHERE supervision_level = 'UNASSIGNED'
    )
    SELECT
        u.state_code,
        u.person_id,
        r.assessment_date AS start_date,
        -- Span ends when either there's another score or the unassigned span ends
        -- Use nonnull_end_date_clause because we don't want to subtract 1 day from these dates
        LEAST(
          {nonnull_end_date_clause("score_end_date_exclusive")},
          {nonnull_end_date_clause("u.end_date_exclusive")}
        ) AS end_date,
        -- We've already limited to LOW assessments and so as long as they happen after the start of an unassigned
        -- period, the criteria is met 
        TRUE AS meets_criteria,
        assessment_date,
        unassigned_start_date,
        assessment_level,
        TO_JSON(STRUCT(
            assessment_date,
            assessment_level,
            unassigned_start_date
        )) AS reason,        
    FROM unassigned_sessions u
    INNER JOIN risk_level_spans r
        ON u.person_id = r.person_id
        AND r.assessment_date BETWEEN u.start_date AND {nonnull_end_date_exclusive_clause('u.end_date_exclusive')}
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        meets_criteria_default=False,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=[
            ReasonsField(
                name="assessment_level",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Assessed risk level",
            ),
            ReasonsField(
                name="unassigned_start_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Start date of latest unassigned session",
            ),
            ReasonsField(
                name="assessment_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Assessment date",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
