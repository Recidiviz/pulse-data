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
# ============================================================================
"""
Defines a criteria span view that shows spans of time during which
someone is incarcerated within 6 months of their full term completion date,
parole eligibility date, or tentative parole date.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    TASK_ELIGIBILITY_CRITERIA_GENERAL,
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    extract_object_from_json,
)
from recidiviz.task_eligibility.utils.us_ix_query_fragments import (
    IX_STATE_CODE_WHERE_CLAUSE,
    ix_combining_several_criteria_into_one_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_INCARCERATION_WITHIN_6_MONTHS_OF_FTCD_OR_PED_OR_TPD"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone is incarcerated within 6 months of their full term completion date,
parole eligibility date, or tentative parole date.
"""

_CRITERIA_QUERY_1 = f"""
    SELECT
        * EXCEPT (reason),
        {extract_object_from_json(object_column = 'full_term_completion_date', 
                            object_type = 'DATE')} AS full_term_completion_date,
        NULL AS parole_eligibility_date,
        NULL AS tentative_parole_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_general}}.incarceration_within_6_months_of_full_term_completion_date_materialized`
    {IX_STATE_CODE_WHERE_CLAUSE}"""

_CRITERIA_QUERY_2 = f"""
    SELECT
        * EXCEPT (reason),
        NULL AS full_term_completion_date,
        {extract_object_from_json(object_column = 'parole_eligibility_date', 
                            object_type = 'DATE')} AS parole_eligibility_date,
        NULL AS tentative_parole_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_general}}.incarceration_within_6_months_of_parole_eligibility_date_materialized`
    {IX_STATE_CODE_WHERE_CLAUSE}"""

_CRITERIA_QUERY_3 = f"""
    SELECT
        * EXCEPT (reason),
        NULL AS full_term_completion_date,
        NULL AS parole_eligibility_date,
        {extract_object_from_json(object_column = 'tentative_parole_date', 
                                  object_type = 'DATE')} AS tentative_parole_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_us_ix}}.tentative_parole_date_within_6_months_materialized` """

_JSON_CONTENT = """MIN(full_term_completion_date) AS full_term_completion_date,
                    MIN(parole_eligibility_date) AS parole_eligibility_date,
                    MIN(tentative_parole_date) AS tentative_parole_date"""

_QUERY_TEMPLATE = f"""
{ix_combining_several_criteria_into_one_view_builder(
        select_statements_for_criteria_lst=[_CRITERIA_QUERY_1,
                                             _CRITERIA_QUERY_2,
                                             _CRITERIA_QUERY_3],
        meets_criteria="LOGICAL_OR(meets_criteria)",
        json_content=_JSON_CONTENT,
    )}
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    task_eligibility_criteria_general=TASK_ELIGIBILITY_CRITERIA_GENERAL,
    task_eligibility_criteria_us_ix=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_IX
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()