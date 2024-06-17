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
# ============================================================================
""" Defines a criteria span view that shows spans of time during which
someone meets either the time served OR the special cases criteria
for special circumstances supervision """
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    combining_several_criteria_into_one,
    extract_object_from_json,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_PA_MEETS_ONE_OF_SPECIAL_CIRCUMSTANCES_CRITERIA"

_DESCRIPTION = """ Defines a criteria span view that shows spans of time during which
someone meets either the time served OR the special cases criteria 
for special circumstances supervision """


_CRITERIA_QUERY_1 = f"""
    SELECT
        * EXCEPT (reason),
        {extract_object_from_json(object_column = 'eligible_date', 
                            object_type = 'DATE')} AS time_served_eligible_date,
        NULL AS special_case_eligible_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_us_pa}}.meets_special_circumstances_criteria_for_time_served_materialized`
"""

_CRITERIA_QUERY_2 = f"""
    SELECT
        * EXCEPT (reason),
        NULL AS time_served_eligible_date,
        {extract_object_from_json(object_column = 'eligible_date', 
                                  object_type = 'DATE')} AS special_case_eligible_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_us_pa}}.meets_special_circumstances_criteria_for_special_cases_materialized`
"""

_JSON_CONTENT = f"""MIN(time_served_eligible_date) AS time_served_eligible_date,
                    MIN(special_case_eligible_date) AS special_case_eligible_date,
                   '{_CRITERIA_NAME}' AS criteria_name"""

_QUERY_TEMPLATE = f"""
WITH combined_query AS (
{combining_several_criteria_into_one(
        select_statements_for_criteria_lst=[_CRITERIA_QUERY_1,
                                             _CRITERIA_QUERY_2],
        meets_criteria="LOGICAL_OR(meets_criteria)",
        json_content=_JSON_CONTENT,
    )}
)
SELECT
    *,
    JSON_EXTRACT(reason, "$.time_served_eligible_date") AS time_served_eligible_date,
    JSON_EXTRACT(reason, "$.special_case_eligible_date") AS special_case_eligible_date,
FROM
    combined_query
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_PA,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    task_eligibility_criteria_us_pa=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_PA
    ),
    reasons_fields=[
        ReasonsField(
            name="time_served_eligible_date",
            type=bigquery.enums.SqlTypeNames.DATE,
            description="#TODO(#29059): Add reasons field description",
        ),
        ReasonsField(
            name="special_case_eligible_date",
            type=bigquery.enums.SqlTypeNames.DATE,
            description="#TODO(#29059): Add reasons field description",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
