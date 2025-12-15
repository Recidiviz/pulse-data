# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Defines a criteria span view that shows spans of time during which
someone has an early discharge eligibility date before their full term completion date
(aka expiration date). When a client's early discharge eligibility date is equal to
their full term completion date it is implied they will have normal supervision
discharge on their sentence expiration date."""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_spans_cte,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    task_deadline_critical_date_update_datetimes_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SUPERVISION_EARLY_DISCHARGE_BEFORE_FULL_TERM_COMPLETION_DATE"

_QUERY_TEMPLATE = f"""
WITH
/*
Pull the early discharge date "critical date" from the task deadline table
*/
{task_deadline_critical_date_update_datetimes_cte(
    task_type=StateTaskType.DISCHARGE_EARLY_FROM_SUPERVISION,
    critical_date_column='eligible_date')
},
/*
Turn the task deadline records into spans with start_datetime and end_datetime cols
*/
{critical_date_spans_cte()},
/*
Combine early discharge date spans with projected completion/expiration date spans
*/
early_discharge_and_full_term_dates AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        projected_completion_date_max,
        NULL AS critical_date,
    FROM `{{project_id}}.{{sessions_dataset}}.supervision_projected_completion_date_spans_materialized`

    UNION ALL

    SELECT
        state_code,
        person_id,
        CAST(start_datetime AS DATE) AS start_date,
        CAST(end_datetime AS DATE) AS end_date,
        NULL AS projected_completion_date_max,
        critical_date
    FROM critical_date_spans
),
/*
Split the overlapping spans into sub-spans
*/
{create_sub_sessions_with_attributes(table_name='early_discharge_and_full_term_dates')},
/*
Collapse the sub-spans by picking the MAX completion date and critical date for each
sub-span. There should only be 1 non-null value for both if each source table does not
have overlaps.
*/
combine_sub_sessions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        MAX(projected_completion_date_max) AS projected_completion_date,
        MAX(critical_date) AS early_discharge_date,
    FROM sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    early_discharge_date < {nonnull_end_date_clause('projected_completion_date')} AS meets_criteria,
    TO_JSON(STRUCT(projected_completion_date AS eligible_date)) AS reason,
    projected_completion_date,
FROM combine_sub_sessions
WHERE early_discharge_date IS NOT NULL
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=__doc__,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="projected_completion_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Maximum projected completion date out of all sentences that a client is serving during their supervision period",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
