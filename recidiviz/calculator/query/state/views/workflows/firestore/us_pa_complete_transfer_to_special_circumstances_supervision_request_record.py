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
"""Query for relevant metadata needed to support special circumstances supervision opportunity in PA
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
    x_time_away_from_eligibility,
)
from recidiviz.task_eligibility.utils.us_pa_query_fragments import violations_helper
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_VIEW_NAME = (
    "us_pa_complete_transfer_to_special_circumstances_supervision_request_record"
)

US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_DESCRIPTION = """
Query for relevant metadata needed to support special circumstances supervision opportunity in PA
"""

US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_QUERY_TEMPLATE = f"""
WITH current_supervision_pop_cte AS (
{join_current_task_eligibility_spans_with_external_id(state_code="'US_PA'",
    tes_task_query_view='complete_transfer_to_special_circumstances_supervision_request_materialized',
    id_type="'US_PA_PBPP'")} 
),

json_to_array_cte AS (
    {json_to_array_cte('current_supervision_pop_cte')}
),

eligible_and_almost_eligible AS (

    -- ELIGIBLE
    {clients_eligible(from_cte = 'current_supervision_pop_cte')}

    UNION ALL 

    -- ALMOST ELIGIBLE (<6 months remaining before time served criteria is met)
    {x_time_away_from_eligibility(time_interval= 6, date_part= 'MONTH',
        criteria_name= 'US_PA_MEETS_SPECIAL_CIRCUMSTANCES_CRITERIA_FOR_TIME_SERVED',
        from_cte_table_name = "json_to_array_cte")}
),

case_notes_cte AS (
{violations_helper()}
),

array_case_notes_cte AS (
{array_agg_case_notes_by_external_id()}
)

{opportunity_query_final_select_with_case_notes()}
"""

US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_VIEW_NAME,
    view_query_template=US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_QUERY_TEMPLATE,
    description=US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_DESCRIPTION,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_PA
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.build_and_print()
