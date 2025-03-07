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
"""Defines a criterion span view that shows spans of time during which someone's
assessed risk level has continuously been `LOW` for at least 2 years.
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
_QUERY_TEMPLATE = f"""
    WITH risk_level_spans AS (
        SELECT 
            state_code,
            person_id,
            assessment_date AS start_date,
            score_end_date_exclusive AS end_date,
            assessment_level,
        FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized`
        -- TODO(#38876): Handle upcoming StrongR 2.0 Case in TN preemptively
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
    /* TODO(#34750): Rework this criterion to handle potential cases where someone might
    have overlapping risk-assessment spans. The `assessment_score_sessions_materialized`
    view spans are non-overlapping by person + assessment type, so it's possible that if
    there were multiple assessment types (all under the 'RISK' class) being used at
    once, we could end up with overlapping spans coming out of the above CTE. While (at
    the time of writing this), each state appears to be using only one type of risk
    assessment per client/resident, this may not always be the case, and this criterion
    would not be equipped to handle that potential scenario. */
    risk_level_spans_aggregated AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            assessment_level,
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
    critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            start_date AS start_datetime,
            end_date AS end_datetime,
            assessment_level,
            /* We'll set the critical date to be 2 years out from the date on which the
            risk level was initially determined. We set the critical date for all spans
            because we've already filtered out any non-'LOW' spans. */
            DATE_ADD(start_date, INTERVAL 2 YEAR) AS critical_date,
        FROM risk_level_spans_aggregated
    ),
    {critical_date_has_passed_spans_cte(attributes=['assessment_level'])}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(
            assessment_level AS assessment_level,
            critical_date AS eligible_date
        )) AS reason,
        assessment_level,
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
            name="assessment_level",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Assessed risk level",
        ),
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
