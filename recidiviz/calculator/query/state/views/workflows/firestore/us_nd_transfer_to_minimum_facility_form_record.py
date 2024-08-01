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
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.almost_eligible_query_fragments import (
    clients_eligible,
    json_to_array_cte,
)
from recidiviz.task_eligibility.utils.us_nd_query_fragments import (
    get_infractions_as_case_notes,
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

WITH current_incarceration_pop_cte AS (
    {join_current_task_eligibility_spans_with_external_id(state_code= "'US_ND'", 
    tes_task_query_view = 'transfer_to_minimum_facility_form_materialized',
    id_type = "'US_ND_ELITE'")}
),

case_notes_cte AS (
-- Get together all case_notes

    -- Positive Behavior Reports (PBR)
    {get_positive_behavior_reports_as_case_notes()}

    UNION ALL

    -- Mental Health Assignments
    {get_program_assignments_as_case_notes(
        additional_where_clause="REGEXP_CONTAINS(spa.program_id, r'MENTAL HEALTH')", 
        criteria='Mental Health')}

    UNION ALL

    -- Assignments (this includes programming, career readiness and jobs)
    {get_program_assignments_as_case_notes(
        additional_where_clause="NOT REGEXP_CONTAINS(spa.program_id, r'MENTAL HEALTH')")}

    UNION ALL

    ({get_infractions_as_case_notes()})
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
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_TRANSFER_TO_MINIMUM_FACILITY_VIEW_BUILDER.build_and_print()
