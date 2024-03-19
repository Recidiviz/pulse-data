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
"""
Defines a criteria span view that shows spans of time during which
someone is incarcerated within 6 months of their full term completion date, 
parole_review_date or CPP release date.
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
    combining_several_criteria_into_one,
    extract_object_from_json,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ND_INCARCERATION_WITHIN_6_MONTHS_OF_FTCD_OR_PRD_OR_CPP_RELEASE"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone is incarcerated within 6 months of their full term completion date, 
parole_review_date or CPP release date.
"""

_CRITERIA_QUERY_1 = f"""
    SELECT
        * EXCEPT (reason),
        {extract_object_from_json(object_column = 'full_term_completion_date', 
                            object_type = 'DATE')} AS full_term_completion_date,
        NULL AS parole_review_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_general}}.incarceration_within_6_months_of_full_term_completion_date_materialized`
    WHERE state_code = 'US_ND'"""

_CRITERIA_QUERY_2 = f"""
    SELECT
        * EXCEPT (reason),
        NULL AS full_term_completion_date,
        {extract_object_from_json(object_column = 'parole_review_date', 
                                  object_type = 'DATE')} AS parole_review_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_US_ND}}.incarceration_within_6_months_of_parole_review_date_materialized`"""

# TODO(#27763): Add CPP date as _CRITERIA_QUERY_3

_JSON_CONTENT = f"""MIN(full_term_completion_date) AS full_term_completion_date,
                    MIN(parole_review_date) AS parole_review_date,
                   '{_CRITERIA_NAME}' AS criteria_name"""

_QUERY_TEMPLATE = f"""
{combining_several_criteria_into_one(
        select_statements_for_criteria_lst=[_CRITERIA_QUERY_1,
                                             _CRITERIA_QUERY_2],
        meets_criteria="LOGICAL_OR(meets_criteria)",
        json_content=_JSON_CONTENT,
    )}
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_ND,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    task_eligibility_criteria_general=TASK_ELIGIBILITY_CRITERIA_GENERAL,
    task_eligibility_criteria_US_ND=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_ND
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
