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
"""Defines a criteria span view that shows spans of time during which someone is
serving a special supervision case"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_pa_query_fragments import (
    case_when_special_case,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_PA_SERVING_SPECIAL_CASE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone is
serving a special supervision case"""

_QUERY_TEMPLATE = f"""
WITH supervision_spans AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        termination_date AS end_date,
        {case_when_special_case()} THEN TRUE ELSE FALSE END AS meets_criteria,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period`
    WHERE state_code = 'US_PA'
),
{create_sub_sessions_with_attributes('supervision_spans')} 
, deduped_supervision_spans AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        logical_or(meets_criteria) as meets_criteria, # if serving special case simultaneously with other case type, count as special case 
     FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
)
SELECT 
    state_code,
    person_id,
    start_date,
    end_date, 
    meets_criteria,
    TO_JSON(STRUCT(CASE WHEN meets_criteria THEN 'special' ELSE 'non-special' END AS case_type,
                    # this reason is for front end display for the spc workflow 
                    # if special case, we display that we check for "medium or high" sanctions, else just "high" sanctions
                   CASE WHEN meets_criteria THEN 'medium or high' ELSE 'high' END AS sanction_type)) AS reason,
    CASE WHEN meets_criteria THEN 'special' ELSE 'non-special' END AS case_type,
    CASE WHEN meets_criteria THEN 'medium or high' ELSE 'high' END AS sanction_type,
FROM({aggregate_adjacent_spans(table_name='deduped_supervision_spans', attribute = 'meets_criteria')})
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    state_code=StateCode.US_PA,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    reasons_fields=[
        ReasonsField(
            name="case_type",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Indicates whether someone is serving a special supervision case",
        ),
        ReasonsField(
            name="sanction_type",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Indicates what sanctions we are checking for - if special case, they must have no medium or high sanctions within past 12 months. If non-special case, they must have no high sanctions.",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
