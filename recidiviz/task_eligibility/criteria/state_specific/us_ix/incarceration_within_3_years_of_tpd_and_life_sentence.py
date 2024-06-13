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
3 years away from the tentative parole date (TPD) AND has a life sentence.
"""
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    TASK_ELIGIBILITY_CRITERIA_GENERAL,
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
from recidiviz.task_eligibility.utils.us_ix_query_fragments import (
    IX_STATE_CODE_WHERE_CLAUSE,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_INCARCERATION_WITHIN_3_YEARS_OF_TPD_AND_LIFE_SENTENCE"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which someone is 
3 years away from the tentative parole date (TPD) AND has a life sentence.
"""

_CRITERIA_QUERY_1 = f"""
    SELECT
        * EXCEPT (reason, reason_v2, meets_criteria),
        NOT meets_criteria AS meets_criteria,
        NULL AS tentative_parole_date,
        {extract_object_from_json(object_column = 'life_sentence', 
                                  object_type = 'BOOLEAN')} AS life_sentence,
    FROM `{{project_id}}.{{task_eligibility_criteria_general}}.not_serving_a_life_sentence_materialized`
    {IX_STATE_CODE_WHERE_CLAUSE}"""

_CRITERIA_QUERY_2 = f"""
    SELECT
        * EXCEPT (reason, reason_v2),
        {extract_object_from_json(object_column = 'tentative_parole_date', 
                                  object_type = 'DATE')} AS tentative_parole_date,
        False AS life_sentence,
    FROM `{{project_id}}.{{task_eligibility_criteria_us_ix}}.tentative_parole_date_within_3_years_materialized`"""

_JSON_CONTENT = f"""MIN(tentative_parole_date) AS tentative_parole_date,
                   LOGICAL_OR(life_sentence) AS life_sentence,
                   '{_CRITERIA_NAME}' AS criteria_name"""

_QUERY_TEMPLATE = f"""
WITH combined_query AS (
{combining_several_criteria_into_one(
        select_statements_for_criteria_lst=[_CRITERIA_QUERY_1,
                                             _CRITERIA_QUERY_2],
        meets_criteria="(LOGICAL_AND(meets_criteria AND (num_criteria >= 2)))",
        json_content=_JSON_CONTENT,
    )}
)
SELECT
    *,
    JSON_EXTRACT(reason, "$.ineligible_offenses") AS ineligible_offenses,
    JSON_EXTRACT(reason, "$.life_sentence") AS life_sentence,
    JSON_EXTRACT(reason, "$.tentative_parole_date") AS tentative_parole_date,
FROM
    combined_query
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
    reasons_fields=[
        ReasonsField(
            name="ineligible_offenses",
            type=bigquery.enums.SqlTypeNames.RECORD,
            description="#TODO(#29059): Add reasons field description",
        ),
        ReasonsField(
            name="tentative_parole_date",
            type=bigquery.enums.SqlTypeNames.DATE,
            description="#TODO(#29059): Add reasons field description",
        ),
        ReasonsField(
            name="life_sentence",
            type=bigquery.enums.SqlTypeNames.BOOLEAN,
            description="#TODO(#29059): Add reasons field description",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
