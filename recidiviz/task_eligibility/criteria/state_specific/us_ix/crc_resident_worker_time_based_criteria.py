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
Shows the spans of time during which someone in ID is eligible time-wise
for a transfer to a Community Reentry Center (CRC) as a resident worker. 
For this to be true, the person must have one of the following three conditions:
    1. Tentative Parole Date (TPD) within seven (7) years OR
        Full Term Release Date (FTRD) within seven (7) years 
    2. Parole Eligibility Date (PED) within seven (7) years AND
        Parole Hearing Date (PHD) within seven (7) years AND
        Full Term Release Date (FTRD) within 20 years
    3. Life sentence AND 
        Tentative Parole Date (TPD) within 3 years
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
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

_CRITERIA_NAME = "US_IX_CRC_RESIDENT_WORKER_TIME_BASED_CRITERIA"

_DESCRIPTION = """
Shows the spans of time during which someone in ID is eligible time-wise
for a transfer to a Community Reentry Center (CRC) as a resident worker. 
For this to be true, the person must have one of the following three conditions:
    1. Tentative Parole Date (TPD) within seven (7) years OR
        Full Term Release Date (FTRD) within seven (7) years 
    2. Parole Eligibility Date (PED) within seven (7) years AND
        Parole Hearing Date (PHD) within seven (7) years AND
        Full Term Release Date (FTRD) within 20 years
    3. Life sentence AND 
        Tentative Parole Date (TPD) within 3 years
"""

_CRITERIA_QUERY_1 = """
    SELECT
        *
    FROM `{project_id}.{task_eligibility_criteria_us_ix}.incarceration_within_7_years_of_ftcd_or_tpd_materialized`"""

_CRITERIA_QUERY_2 = """
    SELECT
        *
    FROM `{project_id}.{task_eligibility_criteria_us_ix}.incarceration_within_7_years_of_ped_and_phd_and_20_years_of_ftcd_materialized`"""

_CRITERIA_QUERY_3 = """
    SELECT
        *
    FROM `{project_id}.{task_eligibility_criteria_us_ix}.incarceration_within_3_years_of_tpd_and_life_sentence_materialized`"""

_JSON_CONTENT = """ARRAY_AGG(reason) AS reasons"""

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
    task_eligibility_criteria_us_ix=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_IX
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
