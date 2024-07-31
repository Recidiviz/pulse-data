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
    FROM `{project_id}.{task_eligibility_criteria_us_ix}.incarceration_within_7_years_of_ftcd_or_tpd_materialized`
    WHERE meets_criteria"""

_CRITERIA_QUERY_2 = """
    SELECT
        *
    FROM `{project_id}.{task_eligibility_criteria_us_ix}.incarceration_within_7_years_of_ped_and_phd_and_20_years_of_ftcd_materialized`
    WHERE meets_criteria"""

_CRITERIA_QUERY_3 = """
    SELECT
        *
    FROM `{project_id}.{task_eligibility_criteria_us_ix}.incarceration_within_3_years_of_tpd_and_life_sentence_materialized`
    WHERE meets_criteria"""

_JSON_CONTENT = """ARRAY_AGG(reason) AS reasons"""

_QUERY_TEMPLATE = f"""
WITH combined_query AS (
{combining_several_criteria_into_one(
        select_statements_for_criteria_lst=[_CRITERIA_QUERY_1,
                                             _CRITERIA_QUERY_2,
                                             _CRITERIA_QUERY_3],
        meets_criteria="LOGICAL_OR(meets_criteria)",
        json_content=_JSON_CONTENT,
    )}
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    meets_criteria,
    ANY_VALUE(reason) AS reason,
    ANY_VALUE(JSON_EXTRACT(reason_unnest, "$.full_term_completion_date")) AS full_term_completion_date,
    ANY_VALUE(JSON_EXTRACT(reason_unnest, "$.next_parole_hearing_date")) AS next_parole_hearing_date,
    ANY_VALUE(JSON_EXTRACT(reason_unnest, "$.parole_eligibility_date")) AS parole_eligibility_date,
    ANY_VALUE(JSON_EXTRACT(reason_unnest, "$.tentative_parole_date")) AS tentative_parole_date,
    ANY_VALUE(JSON_EXTRACT(reason_unnest, "$.ineligible_offenses")) AS ineligible_offenses,
FROM
    combined_query,
    UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(reason, "$."), "$.reasons")) AS reason_unnest
GROUP BY
    1, 2, 3, 4, 5
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    task_eligibility_criteria_us_ix=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_IX
    ),
    reasons_fields=[
        ReasonsField(
            name="full_term_completion_date",
            type=bigquery.enums.SqlTypeNames.DATE,
            description="Full Term Release Date (FTRD)",
        ),
        ReasonsField(
            name="next_parole_hearing_date",
            type=bigquery.enums.SqlTypeNames.DATE,
            description="Parole Hearing Date (PHD)",
        ),
        ReasonsField(
            name="parole_eligibility_date",
            type=bigquery.enums.SqlTypeNames.DATE,
            description="Parole Eligibility Date (PED)",
        ),
        ReasonsField(
            name="tentative_parole_date",
            type=bigquery.enums.SqlTypeNames.DATE,
            description="Tentative Parole Date (TPD)",
        ),
        ReasonsField(
            name="ineligible_offenses",
            type=bigquery.enums.SqlTypeNames.RECORD,
            description="List of ineligible Offenses",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
