# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Queries information needed to fill out the furlough release forms
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    array_agg_case_notes_by_external_id,
    current_snooze,
    join_current_task_eligibility_spans_with_external_id,
    opportunity_query_final_select_with_case_notes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.task_eligibility.collapsed_task_eligibility_spans import (
    build_collapsed_tes_spans_view_materialized_address,
)
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.eligibility_spans.us_me.furlough_release_form import (
    VIEW_BUILDER as TES_VIEW_BUILDER,
)
from recidiviz.task_eligibility.utils.us_me_query_fragments import (
    FURLOUGH_NOTE_TX_REGEX,
    PROGRAM_ENROLLMENT_NOTE_TX_REGEX,
    cis_201_case_plan_case_notes,
    cis_204_notes_cte,
    cis_300_relevant_property_case_notes,
    cis_425_program_enrollment_notes,
    me_time_left_in_sentence_in_categories,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_FURLOUGH_NOTE_TITLE = "Furlough policy"

US_ME_COMPLETE_FURLOUGH_RELEASE_RECORD_VIEW_NAME = "us_me_furlough_release_form_record"

US_ME_COMPLETE_FURLOUGH_RELEASE_RECORD_DESCRIPTION = """
    Queries information needed to fill out the furlough release forms
    """

_COLLAPSED_TES_SPANS_ADDRESS = build_collapsed_tes_spans_view_materialized_address(
    TES_VIEW_BUILDER
)

US_ME_COMPLETE_FURLOUGH_RELEASE_RECORD_QUERY_TEMPLATE = f"""

WITH eligible_and_almost_eligible AS (
{join_current_task_eligibility_spans_with_external_id(
    state_code= "'US_ME'", 
    tes_task_query_view = 'furlough_release_form_materialized',
    id_type = "'US_ME_DOC'",
    eligible_and_almost_eligible_only=True,
    tes_collapsed_view_for_eligible_date=_COLLAPSED_TES_SPANS_ADDRESS
)}
),

case_notes_cte AS (
-- Get together all case_notes

    -- Program enrollment
    {cis_425_program_enrollment_notes()}

    UNION ALL

    -- Program-related notes
    {cis_204_notes_cte("Notes: Program Enrollment")}
    WHERE ncd.Note_Type_Cd = '2'
        AND cncd.Contact_Mode_Cd = '20'
        AND (n.Short_Note_Tx IS NOT NULL OR n.Note_Tx IS NOT NULL)
        AND (REGEXP_CONTAINS(UPPER(n.Short_Note_Tx), r'{PROGRAM_ENROLLMENT_NOTE_TX_REGEX}')
        OR REGEXP_CONTAINS(UPPER(n.Note_Tx), r'{PROGRAM_ENROLLMENT_NOTE_TX_REGEX}'))
    GROUP BY 1,2,3,4,5    

    UNION ALL 

    -- Case Plan Goals
    {cis_201_case_plan_case_notes()}

    UNION ALL 

    -- Furlough-release related notes
    {cis_204_notes_cte("Notes: Furlough")}
    WHERE 
        REGEXP_CONTAINS(UPPER(n.Short_Note_Tx), r'{FURLOUGH_NOTE_TX_REGEX}') 
        OR REGEXP_CONTAINS(UPPER(n.Note_Tx), r'{FURLOUGH_NOTE_TX_REGEX}')
    GROUP BY 1,2,3,4,5

    UNION ALL
    -- Furlough pass frequency
    SELECT 
        external_id,
        "{_FURLOUGH_NOTE_TITLE}" AS criteria,
        "Passes (up to 6h)" AS note_title,
        CASE time_left
            WHEN "3y to 6mo" THEN "One pass a week. This person is between 3 years and 6 months away from expected release date."
            WHEN "6mo to 30days" THEN "Two passes a week. This person is less than 6 months away from expected release date."
            WHEN "30days or less" THEN "Two passes a week. This person is less than 6 months away from expected release date."
        END AS note_body,
        SAFE_CAST(NULL AS DATE) AS event_date,
    FROM ({me_time_left_in_sentence_in_categories()}
    )

    UNION ALL
    
    -- Furlough leave frequency
    SELECT 
        external_id,
        "{_FURLOUGH_NOTE_TITLE}" AS criteria,
        "Leaves (6h to 72h)" AS note_title,
        CASE time_left
            WHEN "3y to 6mo" THEN "One leave every 60 days. This person is between 3 years and 6 months away from expected release date."
            WHEN "6mo to 30days" THEN "One leave per month. This person is between 6 months and 30 days away from expected release date."
            WHEN "30days or less" THEN "Two leaves. This person is less than 30 days away from expected release date."
        END AS note_body,
        SAFE_CAST(NULL AS DATE) AS event_date,
    FROM ({me_time_left_in_sentence_in_categories()}
    )

    UNION ALL

    -- Relevant property
    {cis_300_relevant_property_case_notes()}
),

array_case_notes_cte AS (
  {array_agg_case_notes_by_external_id()}
),

-- Get most recent furlough release snoozes per person

snooze_cte AS (
{current_snooze(
    state_code= "US_ME",
    opportunity_type= "usMeFurloughRelease",
)}
),

add_snooze_info AS (
    SELECT *
    FROM array_case_notes_cte
    LEFT JOIN snooze_cte USING(external_id)
)

{opportunity_query_final_select_with_case_notes(
    left_join_cte="add_snooze_info", 
    additional_columns="metadata_denial",
    include_eligible_date=True,
)}
"""

US_ME_COMPLETE_FURLOUGH_RELEASE_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_ME_COMPLETE_FURLOUGH_RELEASE_RECORD_VIEW_NAME,
    view_query_template=US_ME_COMPLETE_FURLOUGH_RELEASE_RECORD_QUERY_TEMPLATE,
    description=US_ME_COMPLETE_FURLOUGH_RELEASE_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    supplemental_dataset=SUPPLEMENTAL_DATA_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_ME
    ),
    analyst_dataset=ANALYST_VIEWS_DATASET,
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_COMPLETE_FURLOUGH_RELEASE_RECORD_VIEW_BUILDER.build_and_print()
