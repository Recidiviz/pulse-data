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
"""Queries information needed to fill out a ATP form in ND
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    array_agg_case_notes_by_external_id,
    join_current_task_eligibility_spans_with_external_id,
    opportunity_query_final_select_with_case_notes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.us_nd_query_fragments import (
    get_infractions_as_case_notes,
    get_positive_behavior_reports_as_case_notes,
    get_program_assignments_as_case_notes,
    reformat_ids,
    get_offender_case_notes,
    SSI_NOTE_WHERE_CLAUSE,
    HEALTH_NOTE_TEXT_REGEX,
    TRAINING_PROGRAMMING_NOTE_TEXT_REGEX,
    WORK_NOTE_TEXT_REGEX,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ND_TRANSFER_TO_ATP_RECORD_VIEW_NAME = "us_nd_transfer_to_atp_form_record"

US_ND_TRANSFER_TO_ATP_RECORD_DESCRIPTION = """
    Queries information needed to fill out a ATP form in ND
    """

US_ND_TRANSFER_TO_ATP_RECORD_QUERY_TEMPLATE = f"""

WITH eligible_and_almost_eligible AS (
    {join_current_task_eligibility_spans_with_external_id(
        state_code="'US_ND'",
        tes_task_query_view='transfer_to_atp_form_materialized',
        id_type="'US_ND_ELITE'",
        eligible_and_almost_eligible_only=True,
    )}
),

case_notes_cte AS (
-- Get together all case_notes

    -- Positive Behavior Reports (PBR)
    {get_positive_behavior_reports_as_case_notes()}

    UNION ALL

    ({get_infractions_as_case_notes()})

    UNION ALL

    -- Health Assignments
    {get_program_assignments_as_case_notes(
        additional_where_clause=f"REGEXP_CONTAINS(spa.program_id, r'{HEALTH_NOTE_TEXT_REGEX}')", 
        criteria='Health')}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY peid.external_id, note_title, note_body ORDER BY event_date DESC) = 1

    UNION ALL

    -- Training/Program Assignments
    {get_program_assignments_as_case_notes(
        additional_where_clause=f"REGEXP_CONTAINS(spa.program_id, r'{TRAINING_PROGRAMMING_NOTE_TEXT_REGEX}')", 
        criteria='Programming')}

    UNION ALL

    -- Job Assignments
    {get_program_assignments_as_case_notes(
        additional_where_clause=f"REGEXP_CONTAINS(spa.program_id, r'{WORK_NOTE_TEXT_REGEX}')", 
        criteria='Jobs')}
        
    UNION ALL
    
    -- Social Security Insurance
    {get_offender_case_notes(criteria = 'Social Security Insurance', 
                             additional_where_clause=SSI_NOTE_WHERE_CLAUSE)}

    UNION ALL

    -- Alerts
    SELECT 
        peid.external_id,
        'Active Alerts' AS criteria,
        rrac.Description AS note_title,
        eoa.COMMENT_TEXT AS note_body,
        SAFE_CAST(LEFT(eoa.ALERT_DATE, 10) AS DATE) AS event_date,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.recidiviz_elite_offender_alerts_latest` eoa
    INNER JOIN `{{project_id}}.{{raw_data_views_dataset}}.RECIDIVIZ_REFERENCE_alert_codes` rrac
        ON eoa.ALERT_CODE = rrac.Code
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
        ON peid.external_id = {reformat_ids('eoa.OFFENDER_BOOK_ID')}
            AND peid.state_code = 'US_ND'
            AND peid.id_type = 'US_ND_ELITE_BOOKING'
    WHERE ALERT_STATUS = 'ACTIVE'
    AND ALERT_CODE IN ('SEXOF', 'SEX', 'VICTIM', 'VIC', 'CHILD', 'VIOLENT','NOCONT', 'OAC')
),

array_case_notes_cte AS (
{array_agg_case_notes_by_external_id()}
)

{opportunity_query_final_select_with_case_notes()}
"""

US_ND_TRANSFER_TO_ATP_FORM_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_ND_TRANSFER_TO_ATP_RECORD_VIEW_NAME,
    view_query_template=US_ND_TRANSFER_TO_ATP_RECORD_QUERY_TEMPLATE,
    description=US_ND_TRANSFER_TO_ATP_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_ND
    ),
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    raw_data_views_dataset=raw_tables_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_TRANSFER_TO_ATP_FORM_RECORD_VIEW_BUILDER.build_and_print()
