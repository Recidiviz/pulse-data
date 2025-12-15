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
"""Defines a criteria span view that shows spans of time during which someone
has completed half their minimum term incarceration sentence.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "INCARCERATION_PAST_HALF_MIN_TERM_RELEASE_DATE"

_QUERY_TEMPLATE = f"""
--TODO(#37417): Consider refactoring to use sentence imposed groups
--TODO(#39106): Migrate CTE to `sentence_first_serving_date` view
WITH sentence_serving_starts AS
(
SELECT 
  state_code,
  person_id,
  sentence_id,
  MIN(start_date) AS effective_date
FROM `{{project_id}}.{{sentence_sessions_dataset}}.sentence_serving_period_materialized`
GROUP BY 1,2,3
)
,
critical_date_spans AS 
(
SELECT
    state_code,
    person_id,
    start_date AS start_datetime,
    end_date_exclusive AS end_datetime,
    (DATE_ADD(MAX(effective_date),INTERVAL
        CAST(CEILING(DATE_DIFF(MAX(sentence_projected_full_term_release_date_min),MAX(effective_date),DAY))/2 AS INT64) DAY)) AS critical_date
FROM `{{project_id}}.{{sentence_sessions_dataset}}.person_projected_date_sessions_materialized`,
UNNEST(sentence_array)
JOIN `{{project_id}}.{{sentence_sessions_dataset}}.sentences_and_charges_materialized`
    USING(person_id, state_code, sentence_id)
JOIN sentence_serving_starts
    USING(person_id, state_code, sentence_id)
WHERE sentence_type = 'STATE_PRISON'
GROUP BY 1, 2, 3, 4
),
{critical_date_has_passed_spans_cte()}
SELECT
    cd.state_code,
    cd.person_id,
    cd.start_date,
    cd.end_date,
    cd.critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(
        cd.critical_date AS eligible_date
    )) AS reason,
    cd.critical_date AS half_min_term_release_date,
FROM critical_date_has_passed_spans cd
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="half_min_term_release_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date where a client will have completed half their minimum term incarceration sentence",
        ),
    ],
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
