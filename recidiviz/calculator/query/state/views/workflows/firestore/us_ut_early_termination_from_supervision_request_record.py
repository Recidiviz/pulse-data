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

case_notes_cte AS (
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
),

array_case_notes_cte AS (
{array_agg_case_notes_by_external_id(from_cte="all_current_spans_with_tab_names", left_join_cte="case_notes_cte")}
)

{opportunity_query_final_select_with_case_notes(
    from_cte="all_current_spans_with_tab_names",
    additional_columns="metadata_tab_name",)}
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
