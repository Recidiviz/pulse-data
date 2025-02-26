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
"""Queries information needed to fill out the work release forms
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
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.almost_eligible_query_fragments import (
    clients_eligible,
    json_to_array_cte,
    one_criteria_away_from_eligibility,
)
from recidiviz.task_eligibility.utils.raw_table_import import (
    PROGRAM_ENROLLMENT_NOTE_TX_REGEX,
    cis_201_case_plan_case_notes,
    cis_204_notes_cte,
    cis_425_program_enrollment_notes,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ME_COMPLETE_WORK_RELEASE_RECORD_VIEW_NAME = "us_me_work_release_form_record"

US_ME_COMPLETE_WORK_RELEASE_RECORD_DESCRIPTION = """
    Queries information needed to fill out the work release forms
    """

US_ME_COMPLETE_WORK_RELEASE_RECORD_QUERY_TEMPLATE = f"""

WITH current_incarceration_pop_cte AS (
{join_current_task_eligibility_spans_with_external_id(state_code= "'US_ME'", 
    tes_task_query_view = 'work_release_form_materialized',
    id_type = "'US_ME_DOC'")}
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
), 

{json_to_array_cte('current_incarceration_pop_cte')}, 

eligible_and_almost_eligible AS (
    -- ELIGIBLE
    {clients_eligible(from_cte = 'current_incarceration_pop_cte')}

    UNION ALL

    -- ALMOST ELIGIBLE (missing the right custody_level)
    {one_criteria_away_from_eligibility('US_ME_CUSTODY_LEVEL_IS_COMMUNITY')}
),

array_case_notes_cte AS (
  {array_agg_case_notes_by_external_id()}
)

{opportunity_query_final_select_with_case_notes()}
"""

US_ME_COMPLETE_WORK_RELEASE_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_ME_COMPLETE_WORK_RELEASE_RECORD_VIEW_NAME,
    view_query_template=US_ME_COMPLETE_WORK_RELEASE_RECORD_QUERY_TEMPLATE,
    description=US_ME_COMPLETE_WORK_RELEASE_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_ME
    ),
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_COMPLETE_WORK_RELEASE_RECORD_VIEW_BUILDER.build_and_print()
