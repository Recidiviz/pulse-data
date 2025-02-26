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
Defines a criteria span view that shows spans of time during which someone is 
1 year away from the tentative parole date (TPD) AND has a life sentence.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    TASK_ELIGIBILITY_CRITERIA_GENERAL,
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_ix_query_fragments import (
    ix_combining_several_criteria_into_one_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_INCARCERATION_WITHIN_1_YEAR_OF_TPD_AND_LIFE_SENTENCE"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which someone is 
1 year away from the tentative parole date (TPD) AND has a life sentence.
"""

_CRITERIA_QUERY_1 = """
    SELECT
        * EXCEPT (reason, meets_criteria),
        NOT meets_criteria AS meets_criteria,
        NULL AS tentative_parole_date,
        NOT meets_criteria AS life_sentence,
    FROM `{project_id}.{task_eligibility_criteria_general}.not_serving_a_life_sentence_materialized`
    WHERE state_code = 'US_IX'"""

_CRITERIA_QUERY_2 = """
    SELECT
        * EXCEPT (reason),
        SAFE_CAST(JSON_VALUE(reason, '$.tentative_parole_date') AS DATE) AS tentative_parole_date,
        False AS life_sentence,
    FROM `{project_id}.{task_eligibility_criteria_us_ix}.tentative_parole_date_within_1_year_materialized`"""


_JSON_CONTENT = """MIN(tentative_parole_date) AS tentative_parole_date,
                   LOGICAL_OR(life_sentence) AS life_sentence"""

_QUERY_TEMPLATE = f"""
{ix_combining_several_criteria_into_one_view_builder(
        select_statements_for_criteria_lst=[_CRITERIA_QUERY_1,
                                             _CRITERIA_QUERY_2],
        meets_criteria="(LOGICAL_AND(meets_criteria AND (num_criteria >= 2)))",
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
