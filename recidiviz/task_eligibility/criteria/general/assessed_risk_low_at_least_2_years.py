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
assessed risk level has continuously been 'LOW' for at least 2 years. This criterion
considers all assessments in the 'RISK' class and is not specific to the type of
assessment.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "ASSESSED_RISK_LOW_AT_LEAST_2_YEARS"

# TODO(#34709): Move this criterion logic into a criterion builder in the
# `general_criteria_builders.py` file, where it can be generalized/parameterized.
# TODO(#34751): Decide how to handle assessments with null `assessment_level` values.
_QUERY_TEMPLATE = f"""
    WITH risk_assessments_prioritized AS (
        /* Though `assessment_score_sessions_materialized` is already sessionized, there
        can be overlapping sessions there if multiple assessment types have been used to
        assess risk in a state. To work around this, we'll take the assessment data from
        that view but end up constructing criterion-specific spans later in this query,
        since we want to avoid having overlapping spans. */
        SELECT
            state_code,
            person_id,
            assessment_level,
            assessment_date,
        FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized`
        WHERE assessment_class='RISK'
        /* If someone has an assessment with a null `assessment_level` or a non-'LOW'
        `assessment_level` on a given day, prioritize that assessment over any 'LOW'
        assessment on the same day. Because we're just checking for 'LOW' vs. non-'LOW'
        in this criterion, it doesn't matter which of the non-'LOW' assessments we
        choose in case there are multiple. We just need to know whether it was 'LOW' or
        not. */
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY state_code, person_id, assessment_date
            ORDER BY 
                CASE
                    WHEN COALESCE(assessment_level, 'UNKNOWN')!='LOW' THEN 1
                    ELSE 2
                    END
        ) = 1
    ),
    risk_level_spans AS (
        /* Create risk-assessment spans. We create non-overlapping spans such that if a
        person has a previous risk assessment of one type, that span will end when a
        subsequent risk assessment is completed, even if the subsequent assessment is of
        a different type (as long as it's still a risk assessment). */
        SELECT
            state_code,
            person_id,
            assessment_level,
            assessment_date AS start_date,
            LEAD(assessment_date) OVER (
                PARTITION BY state_code, person_id
                ORDER BY assessment_date
            ) AS end_date,
        FROM risk_assessments_prioritized
    ),
    critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            start_date AS start_datetime,
            end_date AS end_datetime,
            /* We'll set the critical date to be 2 years out from the date on which the
            risk level was initially determined, if and only if the assessed risk level
            is 'LOW'. */
            IF(
                assessment_level='LOW',
                DATE_ADD(start_date, INTERVAL 2 YEAR),
                NULL
            ) AS critical_date,
        FROM (
            /* Aggregate adjacent spans to reduce number of rows + ensure that any
            continuous period of time at a single risk level will show up as a single
            span. */
            {aggregate_adjacent_spans(
                "risk_level_spans",
                attribute=['assessment_level']
            )}
        )
    ),
    {critical_date_has_passed_spans_cte()}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(
            critical_date AS eligible_date
        )) AS reason,
        critical_date AS eligible_date,
    FROM critical_date_has_passed_spans
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    meets_criteria_default=False,
    sessions_dataset=SESSIONS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="eligible_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date when individual has (or will have) been at a qualifying risk level for the required time period",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
