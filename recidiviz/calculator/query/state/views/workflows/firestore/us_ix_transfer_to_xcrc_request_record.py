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
# =============================================================================
"""
Queries information needed to surface eligible folks for XCRC in ID.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    array_agg_case_notes_by_external_id,
    current_employment_case_notes,
    current_violent_statutes_being_served,
    join_current_task_eligibility_spans_with_external_id,
    opportunity_query_final_select_with_case_notes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.task_eligibility.dataset_config import (
    TASK_ELIGIBILITY_CRITERIA_GENERAL,
    task_eligibility_criteria_state_specific_dataset,
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.us_ix_query_fragments import (
    DOR_CASE_NOTES_COLUMNS,
    I9_NOTE_TX_REGEX,
    I9_NOTES_STR,
    INSTITUTIONAL_BEHAVIOR_NOTES_STR,
    MEDICAL_CLEARANCE_STR,
    MEDICAL_CLEARANCE_TX_REGEX,
    NOTE_BODY_REGEX,
    NOTE_TITLE_REGEX,
    detainer_case_notes,
    dor_query,
    escape_absconsion_or_eluding_police_case_notes,
    ix_fuzzy_matched_case_notes,
    ix_general_case_notes,
    ix_offender_alerts_case_notes,
    program_enrollment_query,
    victim_alert_notes,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_TRANSFER_TO_XCRC_VIEW_NAME = "us_ix_transfer_to_xcrc_request_record"

US_IX_TRANSFER_TO_XCRC_DESCRIPTION = """
Queries information needed to surface eligible folks for XCRC in ID.
"""

US_IX_TRANSFER_TO_XCRC_QUERY_TEMPLATE = f"""
WITH eligible_and_almost_eligible AS (
    -- Keep only people currently in a CRC who are eligible or almost eligible
    {join_current_task_eligibility_spans_with_external_id(
        state_code= "'US_IX'", 
        tes_task_query_view = 'transfer_to_xcrc_request_materialized',
        id_type = "'US_IX_DOC'",
        eligible_and_almost_eligible_only=True,
    )}),

    case_notes_cte AS (
        -- Offender alerts (excluding victims)
    {ix_offender_alerts_case_notes(where_clause = "WHERE AlertId != '133'")}

        UNION ALL

        -- Current employment
    {current_employment_case_notes('US_IX')}

        UNION ALL

        -- Institutional Behavior Notes
            -- Corrective Action
    {ix_general_case_notes(where_clause_addition="AND ContactModeDesc = 'Corrective Action'", 
                           criteria_str=INSTITUTIONAL_BEHAVIOR_NOTES_STR)}
        
        UNION ALL
        
        -- Positive [behavior notes]
    {ix_general_case_notes(where_clause_addition="AND ContactModeDesc = 'Positive'", 
                           criteria_str=INSTITUTIONAL_BEHAVIOR_NOTES_STR)}

        UNION ALL

        -- I-9 Documents
    {ix_general_case_notes(
        where_clause_addition=f"AND REGEXP_CONTAINS(UPPER(note.Details), r'{I9_NOTE_TX_REGEX}')", 
        criteria_str=I9_NOTES_STR,
        in_the_past_x_months=60)}

        UNION ALL 

        -- Medical clearance
    {ix_general_case_notes( 
        where_clause_addition=f"AND REGEXP_CONTAINS(UPPER(note.Details), r'{MEDICAL_CLEARANCE_TX_REGEX}')",
        criteria_str=MEDICAL_CLEARANCE_STR,
        in_the_past_x_months=6)}

        UNION ALL

        -- Violent charges being served
    ({current_violent_statutes_being_served(state_code = 'US_IX')})

        UNION ALL

        -- NCIC/ILETS
    {ix_fuzzy_matched_case_notes(where_clause = "WHERE ncic_ilets_nco_check")}

        UNION ALL

        -- Recent escape, absconsion or eluding police
    {escape_absconsion_or_eluding_police_case_notes()}

        UNION ALL

        -- Detainers
    {detainer_case_notes()}

        UNION ALL

        -- Program Enrollment
    {program_enrollment_query()}

        UNION ALL

        -- DORs
    {dor_query(columns_str=DOR_CASE_NOTES_COLUMNS, 
               classes_to_include=['A', 'B', 'C'])}
    WHERE event_date > DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 6 MONTH)

        UNION ALL

        -- Victim alerts
    {victim_alert_notes()}
    ),

    array_case_notes_cte AS (
    {array_agg_case_notes_by_external_id()}
    )

{opportunity_query_final_select_with_case_notes()}"""

US_IX_TRANSFER_TO_XCRC_REQUEST_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_IX_TRANSFER_TO_XCRC_VIEW_NAME,
    view_query_template=US_IX_TRANSFER_TO_XCRC_QUERY_TEMPLATE,
    description=US_IX_TRANSFER_TO_XCRC_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_criteria_dataset=TASK_ELIGIBILITY_CRITERIA_GENERAL,
    sessions_dataset=SESSIONS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_IX
    ),
    task_eligibility_criteria_us_ix_dataset=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_IX
    ),
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
    note_title_regex=NOTE_TITLE_REGEX,
    note_body_regex=NOTE_BODY_REGEX,
    should_materialize=True,
    supplemental_dataset=SUPPLEMENTAL_DATA_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_TRANSFER_TO_XCRC_REQUEST_RECORD_VIEW_BUILDER.build_and_print()
