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

case_notes_cte AS (
    # TODO(#38490): This is a placeholder for now. We need to update this to the actual case notes table
    SELECT 
    '' AS external_id, '' AS criteria, '' AS note_title, '' AS note_body, '' AS event_date
    FROM `{{project_id}}.{{task_eligibility_dataset}}.early_termination_from_supervision_request_materialized` tes
    WHERE False
),

array_case_notes_cte AS (
{array_agg_case_notes_by_external_id(from_cte="all_current_spans", left_join_cte="case_notes_cte")}
)

{opportunity_query_final_select_with_case_notes(from_cte="all_current_spans")}
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
