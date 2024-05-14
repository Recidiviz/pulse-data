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
"""Queries information needed to fill out a work-release form in ND
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    array_agg_case_notes_by_external_id,
    join_current_task_eligibility_spans_with_external_id,
    opportunity_query_final_select_with_case_notes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.almost_eligible_query_fragments import (
    clients_eligible,
    json_to_array_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ND_WORK_RELEASE_FORM_RECORD_VIEW_NAME = "us_nd_work_release_form_record"

US_ND_WORK_RELEASE_FORM_RECORD_DESCRIPTION = """
    Queries information needed to fill out a work-release form in ND
    """

US_ND_WORK_RELEASE_FORM_RECORD_QUERY_TEMPLATE = f"""

WITH current_incarceration_pop_cte AS (
    {join_current_task_eligibility_spans_with_external_id(state_code= "'US_ND'", 
    tes_task_query_view = 'work_release_form_materialized',
    id_type = "'US_ND_ELITE'")}
),

case_notes_cte AS (
-- Get together all case_notes

    -- Positive Behavior Reports (PBR)
    SELECT 
        peid.external_id,
        "Positive Behavior Reports (in the past year)" AS criteria,
        facility AS note_title, 
        incident_details AS note_body,
        sic.incident_date AS event_date,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident` sic
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
        USING (person_id)
    WHERE sic.state_code= 'US_ND'
        AND sic.incident_type = 'POSITIVE'
        AND sic.incident_date > DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)

    UNION ALL

    -- Assignments (this includes programming, career readiness and jobs)
    SELECT 
        peid.external_id,
        "Assignments" AS criteria,
        spa.participation_status AS note_title,
        CONCAT(
            spa.program_location_id,
            " - Service: ",
            SPLIT(spa.program_id, '@@')[SAFE_OFFSET(0)],
            " - Activity Description: ",
            SPLIT(spa.program_id, '@@')[SAFE_OFFSET(1)]
        ) AS note_body,
        COALESCE(spa.discharge_date, spa.start_date, spa.referral_date) AS event_date,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_program_assignment` spa
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
        USING (person_id)
    WHERE spa.state_code = 'US_ND'
        AND spa.program_id IS NOT NULL
        AND spa.participation_status = 'IN_PROGRESS'
    GROUP BY 1,2,3,4,5
    HAVING note_body IS NOT NULL
), 
json_to_array_cte AS (
    {json_to_array_cte('current_incarceration_pop_cte')}
),

eligible_and_almost_eligible AS (
    -- ELIGIBLE
    {clients_eligible(from_cte = 'current_incarceration_pop_cte')}
),

array_case_notes_cte AS (
{array_agg_case_notes_by_external_id()}
)

{opportunity_query_final_select_with_case_notes()}
"""

US_ND_WORK_RELEASE_FORM_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_ND_WORK_RELEASE_FORM_RECORD_VIEW_NAME,
    view_query_template=US_ND_WORK_RELEASE_FORM_RECORD_QUERY_TEMPLATE,
    description=US_ND_WORK_RELEASE_FORM_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_ND
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_WORK_RELEASE_FORM_RECORD_VIEW_BUILDER.build_and_print()
