# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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

"""
Defines a criteria view that shows spans of time for which clients have either served
25 years in prison or have served half of their minimum sentence.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import TASK_ELIGIBILITY_CRITERIA_GENERAL
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_PA_SERVED_HALF_MINIMUM_OR_25_YEARS"

_DESCRIPTION = """
Defines a criteria view that shows spans of time for which clients have either served
25 years in prison or have served half of their minimum sentence.
"""

_QUERY_TEMPLATE = f"""
WITH served_half_min_and_25_years AS (
    SELECT 
        * EXCEPT (reason),
        DATE(JSON_EXTRACT_SCALAR(reason, "$.minimum_time_served_date")) AS half_min_term_or_25_years_date,
        '25_YEARS_INCARCERATED' AS criteria_fulfilled_first
    FROM `{{project_id}}.{{tes_criteria_general_dataset}}.incarcerated_at_least_25_years_materialized`
    WHERE state_code = 'US_PA'

    UNION ALL

    SELECT 
        * EXCEPT (reason),
        DATE(JSON_EXTRACT_SCALAR(reason, "$.half_min_term_release_date")) AS half_min_term_or_25_years_date,
        'PAST_HALF_MIN_RELEASE_DATE' AS criteria_fulfilled_first
    FROM `{{project_id}}.{{tes_criteria_general_dataset}}.incarceration_past_half_min_term_release_date_materialized`
    WHERE state_code = 'US_PA'
),

{create_sub_sessions_with_attributes('served_half_min_and_25_years')}

SELECT 
    state_code, 
    person_id, 
    start_date, 
    end_date, 
    meets_criteria, 
    TO_JSON(STRUCT(half_min_term_or_25_years_date AS eligible_date, 
                   criteria_fulfilled_first AS criteria_fulfilled_first)) AS reason,
    half_min_term_or_25_years_date,
    criteria_fulfilled_first,
FROM sub_sessions_with_attributes 
WHERE start_date != COALESCE(end_date, "9999-12-31") 
-- If there are two subsessions with the same date, keep the one meeting the criteria, 
-- if both meet the criteria, choose the one that had the earliest eligible date 
QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, person_id, start_date, end_date 
                          ORDER BY meets_criteria DESC, half_min_term_or_25_years_date ASC) = 1
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_PA,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        tes_criteria_general_dataset=TASK_ELIGIBILITY_CRITERIA_GENERAL,
        reasons_fields=[
            ReasonsField(
                name="half_min_term_or_25_years_date",
                type=bigquery.enums.SqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="criteria_fulfilled_first",
                type=bigquery.enums.SqlTypeNames.STRING,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
