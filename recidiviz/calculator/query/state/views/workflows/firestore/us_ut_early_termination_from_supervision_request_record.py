# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Queries information needed to surface eligible clients for early termination from supervision in UT
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    array_agg_case_notes_by_external_id,
    join_current_task_eligibility_spans_with_external_id,
    opportunity_query_final_select_with_case_notes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_UT_EARLY_TERMINATION_FROM_SUPERVISION_RECORD_VIEW_NAME = (
    "us_ut_early_termination_from_supervision_request_record"
)

US_UT_EARLY_TERMINATION_FROM_SUPERVISION_RECORD_QUERY_TEMPLATE = f"""

WITH all_current_spans AS (
{join_current_task_eligibility_spans_with_external_id(
    state_code= "'US_UT'",
    tes_task_query_view = 'early_termination_from_supervision_request_materialized',
    id_type = "'US_UT_DOC'",
    eligible_and_almost_eligible_only = True,
)}
),

all_current_spans_with_tab_names AS (
    # Almost eligible
    SELECT 
        * EXCEPT(reasons_unnested),
        CASE 
            WHEN reasons_unnested = 'US_UT_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_PAST_HALF_FULL_TERM_RELEASE_DATE' THEN 'EARLY_REQUESTS'
            WHEN reasons_unnested IN ('SUPERVISION_CONTINUOUS_EMPLOYMENT_FOR_3_MONTHS', 'US_UT_HAS_COMPLETED_ORDERED_ASSESSMENTS') THEN 'REPORT_DUE_ALMOST_ELIGIBLE'
            ELSE 'REPORT_DUE_ELIGIBLE'
        END AS metadata_tab_name
    FROM all_current_spans, 
    UNNEST(ineligible_criteria) AS reasons_unnested
    WHERE is_almost_eligible
    -- This is to future-proof this query. If we ever allow people to be almost eligible
    -- for two or more criteria, this will make sure we don't have duplicates in our final query.
    -- Additionally, it will make sure anyone who is missing the half-time date criteria
    -- is assigned to the 'EARLY_REQUESTS' tab.
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY state_code, person_id 
        ORDER BY CASE reasons_unnested
            WHEN 'US_UT_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_PAST_HALF_FULL_TERM_RELEASE_DATE' THEN 1
            ELSE 2 END) = 1
    
    UNION ALL 

    SELECT *, 'REPORT_DUE_ELIGIBLE' AS metadata_tab_name
    FROM all_current_spans
    WHERE is_eligible
),

all_current_spans_with_tab_names_and_interstate_compact_in AS (
    -- Interstate compact in cases
    SELECT
        acs.*,
        IF(slr.supervision_level_raw_text IS NOT NULL, True, False) AS metadata_interstate_compact_in,
    FROM all_current_spans_with_tab_names acs
    LEFT JOIN `{{project_id}}.sessions.supervision_level_raw_text_sessions_materialized` slr
    ON acs.person_id = slr.person_id
        AND acs.state_code = slr.state_code
        AND slr.end_date_exclusive IS NULL
        AND REGEXP_CONTAINS(slr.supervision_level_raw_text, 'COMPACT IN')
),

case_notes_cte AS (
    -- Latest LS/RNR's score
    SELECT 
        peid.external_id,
        'Latest LS/RNR' AS criteria,
        'Score' AS note_title,
        CONCAT( 
            SAFE_CAST(ote.tot_score AS STRING), -- Score
            ' - ',
            IFNULL( CONCAT( UPPER(ote.override_eval_desc), ' (Override)' ), UPPER(ote.eval_desc)), -- Level, override if available
            IFNULL( CONCAT(' - ', ote.override_rsn), '') -- Override reason if available
        ) AS note_body,
        SAFE_CAST(LEFT(ote.updt_dt, 10) AS DATE) AS event_date,
    FROM `{{project_id}}.us_ut_raw_data_up_to_date_views.ofndr_tst_eval_latest` ote
    LEFT JOIN `{{project_id}}.us_ut_raw_data_up_to_date_views.ofndr_tst_latest` ot
    USING (ofndr_tst_id)
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
    ON peid.state_code = 'US_UT'
        AND peid.external_id = ot.ofndr_num
        AND id_type = 'US_UT_DOC'
    WHERE ote.eval_desc IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY peid.state_code, peid.person_id ORDER BY event_date DESC) = 1
    
    UNION ALL

    -- Latest LS/RNR's summary of findings
    # TODO(#40921) Ingest LS/RNR summary of findings into assessment_metadata
    SELECT 
        ot.ofndr_num AS external_id,
        'Latest LS/RNR' AS criteria,
        'Summary of findings' AS note_title,
        cmt AS note_body,
        SAFE_CAST(LEFT(ot.tst_dt, 10) AS DATE) AS event_date,
    FROM `{{project_id}}.us_ut_raw_data_up_to_date_views.ofndr_tst_latest` ot
    LEFT JOIN `{{project_id}}.us_ut_raw_data_up_to_date_views.tst_qstn_rspns_latest`
        USING(ofndr_tst_id, assess_tst_id)
    LEFT JOIN `{{project_id}}.us_ut_raw_data_up_to_date_views.assess_qstn_latest` q
        USING(assess_tst_id, assess_qstn_num, tst_sctn_num)
    LEFT JOIN `{{project_id}}.us_ut_raw_data_up_to_date_views.assess_qstn_choice_latest` c
        USING(assess_tst_id, assess_qstn_num,qstn_choice_num,tst_sctn_num)
    WHERE cmt IS NOT NULL
        AND ot.assess_tst_id IN ('48', '84') -- LS/RNR
        AND q.assess_qstn = 'Summary of Findings'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ot.ofndr_num ORDER BY event_date DESC) = 1

    UNION ALL 

    -- Accomplishments in the past year
    # TODO(#40923) Ingest accomplishments somwhere
    SELECT 
        peid.external_id,
        'Accomplishments (in the past year)' AS criteria,
        rcc.rim_category_title AS note_title,
        rac.accomplishment_desc AS note_body,
        SAFE_CAST(LEFT(oa.accomplishment_dt, 10) AS DATE) AS event_date,
    FROM `{{project_id}}.us_ut_raw_data_up_to_date_views.rim_ofndr_accomplishment_latest` oa
    LEFT JOIN `{{project_id}}.us_ut_raw_data_up_to_date_views.rim_accomplishment_cd_latest` rac
        USING(rim_accomplishment_id)
    LEFT JOIN `{{project_id}}.us_ut_raw_data_up_to_date_views.rim_category_cd_latest` rcc
    USING(rim_category_id)
    INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` peid
        ON peid.external_id = oa.ofndr_num
            AND peid.state_code = 'US_UT'
            AND peid.id_type = 'US_UT_DOC'
    -- In the past year
    WHERE DATE_DIFF(CURRENT_DATE('US/Eastern'), SAFE_CAST(LEFT(oa.accomplishment_dt, 10) AS DATE), MONTH)<12

    UNION ALL

    -- Probation conditions
    # TODO(#40922) Ingest probation conditions into SupervisionPeriod.conditions
    SELECT 
        ofndr_num AS external_id,
        'Probation conditions' AS criteria,
        '' AS note_title,
        s.cond_desc AS note_body,
        SAFE_CAST(LEFT(s.strt_dt, 10) AS DATE) AS event_date,
    FROM `{{project_id}}.us_ut_raw_data_up_to_date_views.p_p_agrmnt_latest` p
    LEFT JOIN `{{project_id}}.us_ut_raw_data_up_to_date_views.p_p_spcl_cond_latest` s
        USING(p_p_agrmnt_id)
    -- Only include conditions that are active today
    WHERE CURRENT_DATE('US/Eastern') BETWEEN SAFE_CAST(LEFT(s.strt_dt, 10) AS DATE) AND IFNULL(SAFE_CAST(LEFT(s.end_dt, 10) AS DATE), '9999-12-31')

    UNION ALL

    -- Address checks
    SELECT 
        peid.external_id,
        "Recent Address Checks" AS criteria,
        contact_reason_raw_text AS note_title,
        CONCAT(
            address,
            ' - ',
            'Successful Contacts: ', successful_counts, ' of ', attempted_counts + successful_counts, ' attempts'
        ) AS note_body,
        event_date
    FROM (
        SELECT 
            state_code,
            person_id,
            ssc.contact_reason_raw_text,
            NULLIF(JSON_VALUE(supervision_contact_metadata, '$.ADDRESS'), '') AS address,
            SUM(IF(status_raw_text = 'ATTEMPTED', 1, 0)) AS attempted_counts,
            SUM(IF(status_raw_text = 'SUCCESSFUL', 1, 0)) AS successful_counts,
            MAX(contact_date) AS event_date,
        FROM `{{project_id}}.us_ut_state.state_supervision_contact` ssc
        INNER JOIN `{{project_id}}.sessions.prioritized_supervision_sessions_materialized` pss
            USING(state_code, person_id)
        WHERE ssc.state_code = 'US_UT'
            # Only include contacts with non-null addresses
            AND JSON_VALUE(supervision_contact_metadata, '$.ADDRESS') != ""
            # Only pull latest supervision super sessions
            AND CURRENT_DATE('US/Pacific') BETWEEN pss.start_date AND {nonnull_end_date_exclusive_clause('pss.end_date_exclusive')}
            # Contact date within a supervision super session
            AND ssc.contact_date BETWEEN pss.start_date AND {nonnull_end_date_exclusive_clause('pss.end_date_exclusive')}
        GROUP BY 1,2,3,4
    )
    INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` peid
        USING(state_code, person_id)

    UNION ALL

    -- Most recent drug screen
    SELECT
        peid.external_id,
        "Drug Screens" AS criteria,
        "Latest screen" AS note_title,
        CONCAT(
        'Result: ', IF(dsp.is_positive_result, 'Positive', 'Negative'),
        ' - ',
        'Sample type: ', dsp.sample_type,
        ' - ',
        'Substance: ', dsp.substance_detected
        ) AS note_body,
        dsp.drug_screen_date AS event_date
    FROM `{{project_id}}.sessions.drug_screens_preprocessed_materialized` dsp
    INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` peid
        USING(state_code, person_id)
    WHERE dsp.state_code = 'US_UT'
    AND peid.id_type = 'US_UT_DOC'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY peid.external_id ORDER BY event_date DESC) = 1

    UNION ALL 

    -- LS/RNR drug screen questions
    SELECT 
        peid.external_id,
        "Latest LS/RNR" AS criteria,
        "Drug questions" AS note_title,
        REGEXP_REPLACE(  
            CONCAT(
                -- Drug problem currently
                JSON_VALUE(ass.assessment_metadata, '$.LSRNR_Q31'),
                ": ",
                JSON_VALUE(ass.assessment_metadata, '$.LSRNR_Q31_ANSWER'),
                ",",
                JSON_VALUE(ass.assessment_metadata, '$.LSRNR_Q31_CMT'),
                " - ",
                -- Drug problem, ever
                JSON_VALUE(ass.assessment_metadata, '$.LSRNR_Q29'),
                ": ",
                JSON_VALUE(ass.assessment_metadata, '$.LSRNR_Q29_ANSWER')
            ), r'[;.]', '') AS note_body,
        ass.assessment_date AS event_date
    FROM `{{project_id}}.sessions.assessment_score_sessions_materialized` ass
    INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` peid
        USING(state_code, person_id)
    WHERE ass.state_code = 'US_UT'
    AND ass.assessment_class = 'RISK'
    AND ass.assessment_type = 'LS_RNR'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY peid.external_id ORDER BY ass.assessment_date DESC) = 1

    UNION ALL 

    -- LS/RNR alcohol screen questions
    SELECT 
        peid.external_id,
        "Latest LS/RNR" AS criteria,
        "Alcohol questions" AS note_title,
        REGEXP_REPLACE(  
            CONCAT(
                -- Alcohol problem currently
                JSON_VALUE(ass.assessment_metadata, '$.LSRNR_Q30'),
                ": ",
                JSON_VALUE(ass.assessment_metadata, '$.LSRNR_Q30_ANSWER'),
                ",",
                JSON_VALUE(ass.assessment_metadata, '$.LSRNR_Q30_CMT'),
                " - ",   
                -- Alcohol problem, ever
                JSON_VALUE(ass.assessment_metadata, '$.LSRNR_Q28'),
                ": ",
                JSON_VALUE(ass.assessment_metadata, '$.LSRNR_Q28_ANSWER')
            ), r'[;.]', '') AS note_body,
        ass.assessment_date AS event_date
    FROM `{{project_id}}.sessions.assessment_score_sessions_materialized` ass
    INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` peid
        USING(state_code, person_id)
    WHERE ass.state_code = 'US_UT'
    AND ass.assessment_class = 'RISK'
    AND ass.assessment_type = 'LS_RNR'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY peid.external_id ORDER BY ass.assessment_date DESC) = 1
),

array_case_notes_cte AS (
{array_agg_case_notes_by_external_id(from_cte="all_current_spans_with_tab_names", left_join_cte="case_notes_cte")}
)

{opportunity_query_final_select_with_case_notes(
    from_cte="all_current_spans_with_tab_names_and_interstate_compact_in",
    additional_columns="metadata_tab_name, metadata_interstate_compact_in",)}
"""

US_UT_EARLY_TERMINATION_FROM_SUPERVISION_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_UT_EARLY_TERMINATION_FROM_SUPERVISION_RECORD_VIEW_NAME,
    view_query_template=US_UT_EARLY_TERMINATION_FROM_SUPERVISION_RECORD_QUERY_TEMPLATE,
    description=__doc__,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_UT
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_UT_EARLY_TERMINATION_FROM_SUPERVISION_RECORD_VIEW_BUILDER.build_and_print()
