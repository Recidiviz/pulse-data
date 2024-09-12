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

"""
Shows the spans of time during which someone in ND is enrolled in a relevant program.
According to CMs, people enrolled in a relevant program are not eligible for facility transfer.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_nd_query_fragments import (
    TRAINING_PROGRAMMING_NOTE_TEXT_REGEX_FOR_CRITERIA,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ND_NOT_ENROLLED_IN_RELEVANT_PROGRAM"

_DESCRIPTION = """
Shows the spans of time during which someone in ND is enrolled in a relevant program.
According to CMs, people enrolled in a relevant program are not eligible for facility transfer.
"""

_QUERY_TEMPLATE = f"""
WITH discharged_and_current_programs AS (
    SELECT 
      peid.state_code,
      peid.person_id,
      spa.start_date,
      spa.discharge_date AS end_date,
      CONCAT(spa.program_location_id, ' - ', spa.program_id) AS program,
      spa.participation_status,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_program_assignment` spa
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
        USING (person_id)
    WHERE spa.state_code = 'US_ND'
        AND spa.program_id IS NOT NULL
        AND spa.participation_status IN ('DISCHARGED', 
                                        'DISCHARGED_SUCCESSFUL', 
                                        'DISCHARGED_UNSUCCESSFUL', 
                                        'DISCHARGED_SUCCESSFUL_WITH_DISCRETION', 
                                        'DISCHARGED_OTHER', 
                                        'DISCHARGED_UNKNOWN', 
                                        'IN_PROGRESS')
        -- Don't surface case manager assignments
        AND NOT REGEXP_CONTAINS(spa.program_id, r'ASSIGNED CASE MANAGER')
        AND REGEXP_CONTAINS(spa.program_id, r'{TRAINING_PROGRAMMING_NOTE_TEXT_REGEX_FOR_CRITERIA}')
    GROUP BY 1,2,3,4,5,6
),
{create_sub_sessions_with_attributes(table_name='discharged_and_current_programs')}
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    FALSE AS meets_criteria,
    TO_JSON(STRUCT(
        STRING_AGG(program, ', ' ORDER BY program) AS program_descriptions,
        STRING_AGG(participation_status, ', ' ORDER BY program) AS program_statuses
    )) AS reason,
    STRING_AGG(program, ', ' ORDER BY program) AS program_descriptions,
    STRING_AGG(participation_status, ', ' ORDER BY program) AS program_statuses,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_ND,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="program_descriptions",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Descriptions of the programs happening during a given span.",
        ),
        ReasonsField(
            name="program_statuses",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Statuses of the programs happening during a given span.",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
