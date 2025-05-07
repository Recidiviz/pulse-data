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
assessed risk level while on supervision has continuously been 'LOW' for at least 2
years. This criterion considers all assessments in the 'RISK' class and is not specific
to the type of assessment.

NB: this criterion considers risk assessments while on supervision and resets whenever
a continuous period on supervision (according to `prioritized_supervision_sessions`)
ends, such that assessments from a previous continuous supervision cycle don't count
toward the criterion.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_intersection_spans,
)
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

_CRITERIA_NAME = "ASSESSED_RISK_LOW_WHILE_ON_SUPERVISION_AT_LEAST_2_YEARS"

# TODO(#34709): Generalize this criterion. We might consider moving this criterion logic
# into a criterion builder in the `general_criteria_builders.py` file, where it can be
# generalized/parameterized.
# TODO(#34751): Decide how to handle assessments with null `assessment_level` values.
_QUERY_TEMPLATE = f"""
    WITH prioritized_supervision_sessions AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
        FROM `{{project_id}}.{{sessions_dataset}}.prioritized_supervision_sessions_materialized`
    ),
    prioritized_supervision_sessions_aggregated AS (
        SELECT
            state_code,
            person_id,
            start_date AS supervision_start_date,
            start_date,
            end_date_exclusive,
        FROM (
            /* The `prioritized_supervision_sessions` view is sessionized on
            `compartment_level_1` and `compartment_level_2`. Here, we aggregate adjacent
            spans regardless of those compartment values, such that we get a single span
            for each continuous period on supervision. */
            {aggregate_adjacent_spans(
                "prioritized_supervision_sessions",
                end_date_field_name="end_date_exclusive",
            )}
        )
    ),
    risk_level_spans AS (
        SELECT
            state_code,
            person_id,
            assessment_level,
            assessment_date,
            assessment_date AS start_date,
            score_end_date_exclusive AS end_date_exclusive,
        FROM `{{project_id}}.{{sessions_dataset}}.risk_assessment_score_sessions_materialized`
    ),
    assessed_risk_levels_during_supervision AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            /* If the assessment happened before the date on which the current
            continuous period of supervision started, then we don't want that assessment
            level to carry over, so we set the assessment level to NULL. */
            IF(
                assessment_date<supervision_start_date,
                NULL,
                assessment_level
            ) AS assessment_level,
        FROM (
            /* We use `create_intersection_spans` with `use_left_join=True` here to get
            a set of spans that cover all periods of time on supervision (from the
            `prioritized_supervision_sessions_aggregated` CTE) and have risk-level info
            (from `risk_level_spans`). Due to this logic, risk-level data from previous
            supervision sessions can carry over into the next (since a risk-level span
            only ends when a subsequent assessment is completed). We therefore will need
            to null out risk-level info (which we do up above) when an assessment
            precedes a supervision session. */
            {create_intersection_spans(
                table_1_name="prioritized_supervision_sessions_aggregated",
                table_2_name="risk_level_spans",
                index_columns=['state_code', 'person_id'],
                use_left_join=True,
                table_1_columns=['supervision_start_date'],
                table_2_columns=['assessment_level', 'assessment_date'],
            )}
        )
    ),
    critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            start_date AS start_datetime,
            end_date_exclusive AS end_datetime,
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
                "assessed_risk_levels_during_supervision",
                attribute=['assessment_level'],
                end_date_field_name="end_date_exclusive",
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
            description="Date when individual has been at a qualifying risk level for the required time period",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
