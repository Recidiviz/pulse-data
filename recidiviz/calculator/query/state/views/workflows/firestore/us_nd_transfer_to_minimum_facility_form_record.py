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
"""Queries information needed to fill out a MINIMUM facility referral form in ND
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    array_agg_case_notes_by_external_id,
    join_current_task_eligibility_spans_with_external_id,
    opportunity_query_final_select_with_case_notes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_criteria_state_specific_dataset,
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.us_nd_query_fragments import (
    HEALTH_NOTE_TEXT_REGEX,
    MINIMUM_HOUSING_REFERRAL_QUERY,
    SSI_NOTE_WHERE_CLAUSE,
    TRAINING_PROGRAMMING_NOTE_TEXT_REGEX,
    WORK_NOTE_TEXT_REGEX,
    eligible_and_almost_eligible_minus_referrals,
    get_ids_as_case_notes,
    get_infractions_as_case_notes,
    get_offender_case_notes,
    get_positive_behavior_reports_as_case_notes,
    get_program_assignments_as_case_notes,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ND_TRANSFER_TO_MINIMUM_FACILITY_VIEW_NAME = (
    "us_nd_transfer_to_minimum_facility_form_record"
)

US_ND_TRANSFER_TO_MINIMUM_FACILITY_DESCRIPTION = """
    Queries information needed to fill out a MINIMUM facility referral form in ND
    """

US_ND_TRANSFER_TO_MINIMUM_FACILITY_QUERY_TEMPLATE = f"""

WITH eligible_and_almost_eligible AS (
    {join_current_task_eligibility_spans_with_external_id(state_code= "'US_ND'", 
    tes_task_query_view = 'transfer_to_minimum_facility_form_materialized',
    id_type = "'US_ND_ELITE'",
    eligible_only=True)}
),

eligible_and_almost_eligible_minus_referrals AS (
    {eligible_and_almost_eligible_minus_referrals()}
),

min_housing_referrals AS (
{MINIMUM_HOUSING_REFERRAL_QUERY}
),

case_notes_cte AS (
-- Get together all case_notes

    -- Positive Behavior Reports (PBR)
    {get_positive_behavior_reports_as_case_notes()}

    UNION ALL

    -- Social Security Insurance due to disabilities
    {get_offender_case_notes(criteria = 'Social Security Insurance', 
                             additional_where_clause=SSI_NOTE_WHERE_CLAUSE)}

    UNION ALL

    ({get_infractions_as_case_notes()})

    UNION ALL

    -- Health Assignments
    {get_program_assignments_as_case_notes(
        additional_where_clause=f"REGEXP_CONTAINS(spa.program_id, r'{HEALTH_NOTE_TEXT_REGEX}') AND spa.participation_status='IN_PROGRESS'", 
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

    -- IDs
    {get_ids_as_case_notes()}

    UNION ALL 
    
    -- Is this person required to be transferred to MRCC?
    SELECT 
        peid.external_id,
        'May have to be transferred to MRCC' AS criteria,
        "This person has" AS note_title,
        CONCAT(
            IF(
                SAFE_CAST(JSON_EXTRACT_SCALAR(reason, '$.has_an_aomms_sentence') AS BOOL),
                'Armed Offender Minimum Mandatory Sentence (AOMMS) -',
                ''),
            IF(
                SAFE_CAST(JSON_EXTRACT_SCALAR(reason, '$.has_registration_requirements') AS BOOL),
                'Registration requirements -',
                ''),
            IF(
                SAFE_CAST(JSON_EXTRACT_SCALAR(reason, '$.has_to_serve_85_percent_of_sentence') AS BOOL),
                '85% sentence',
                '') 
        ) AS note_body,
        wr.start_date AS event_date
    FROM `{{project_id}}.{{task_eligibility_criteria_dataset}}.requires_committee_approval_for_work_release_materialized` wr
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
        USING(person_id, state_code)
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person` sp
        USING(person_id, state_code)
    WHERE meets_criteria
        AND CURRENT_DATE BETWEEN start_date AND IFNULL(end_date, '9999-12-31')
        AND peid.id_type = 'US_ND_ELITE'
        AND sp.gender = 'MALE'

    UNION ALL

    -- STATIC scores for folks who require committee requirements
    SELECT 
        peid2.external_id,
        'Relevant Risk Score' AS criteria,
        'STATIC' AS note_title,
        REGEXP_EXTRACT(CATEGORY_TEXT, r'.*STATIC.*') AS note_body,
        SAFE_CAST(LEFT(CREATE_DATETIME, 10) AS DATE) AS event_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_dataset}}.requires_committee_approval_for_work_release_materialized` ca
    -- We need two IDS: US_ND_ELITE_BOOKING to pull STATIC scores, and US_ND_ELITE to display in the front end
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
        USING(state_code, person_id)
    INNER JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.elite_offender_report_texts_latest` eor
        ON peid.external_id = eor.OFFENDER_BOOK_ID
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid2
        USING(state_code, person_id)
    WHERE CURRENT_DATE('US/Eastern') BETWEEN ca.start_date AND {nonnull_end_date_clause('ca.end_date')}
        AND peid.id_type = 'US_ND_ELITE_BOOKING'
        AND eor.CATEGORY_TYPE = 'LSI-R'
        AND REGEXP_CONTAINS(eor.CATEGORY_TEXT, r'STATIC')
        AND peid2.id_type = 'US_ND_ELITE'

    UNION ALL

    -- Minimum Housing Referrals in the past 2 years
    SELECT 
        peid2.external_id,
        'Minimum Housing Referrals (in the past 2 years)' AS criteria,
        evaluation_result AS note_title,
        IFNULL(committee_comment_text, '') AS note_body,
        evaluation_date AS event_date
    FROM min_housing_referrals mr
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
    ON mr.external_id = peid.external_id
        AND peid.state_code = 'US_ND'
        AND peid.id_type = 'US_ND_ELITE_BOOKING'
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid2
    ON peid.person_id = peid2.person_id
        AND peid.state_code = peid2.state_code
        AND peid2.id_type = 'US_ND_ELITE'
    WHERE evaluation_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 2 YEAR)
), 

array_case_notes_cte AS (
{array_agg_case_notes_by_external_id(from_cte = 'eligible_and_almost_eligible_minus_referrals')}
)
{opportunity_query_final_select_with_case_notes(
    from_cte = 'eligible_and_almost_eligible_minus_referrals')}
"""

US_ND_TRANSFER_TO_MINIMUM_FACILITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_ND_TRANSFER_TO_MINIMUM_FACILITY_VIEW_NAME,
    view_query_template=US_ND_TRANSFER_TO_MINIMUM_FACILITY_QUERY_TEMPLATE,
    description=US_ND_TRANSFER_TO_MINIMUM_FACILITY_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_ND
    ),
    task_eligibility_criteria_dataset=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_ND
    ),
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_TRANSFER_TO_MINIMUM_FACILITY_VIEW_BUILDER.build_and_print()
