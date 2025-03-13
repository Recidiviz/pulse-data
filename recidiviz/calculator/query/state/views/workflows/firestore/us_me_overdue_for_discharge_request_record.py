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
"""Queries individuals eligible for discharge from supervision in Maine
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    current_snooze,
    join_current_task_eligibility_spans_with_external_id,
    opportunity_query_final_select_with_case_notes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ME_OVERDUE_FOR_DISCHARGE_VIEW_NAME = "us_me_overdue_for_discharge_record"

US_ME_OVERDUE_FOR_DISCHARGE_DESCRIPTION = """
    Query for individuals eligible for discharge from supervision in Maine
    """

US_ME_OVERDUE_FOR_DISCHARGE_QUERY_TEMPLATE = f"""

WITH all_current_spans AS (
{join_current_task_eligibility_spans_with_external_id(
    state_code= "'US_ME'",
    tes_task_query_view = 'overdue_for_discharge_request_materialized',
    id_type = "'US_ME_DOC'",
)}
),

-- Get most recent OFD snoozes per person

snooze_cte AS (
{current_snooze(
    state_code= "US_ME",
    opportunity_type= "usMeOverdueForDischarge",
)}
),

add_snooze_info AS (
     SELECT
        *,
        NULL AS case_notes
    FROM snooze_cte
)

{opportunity_query_final_select_with_case_notes(
    from_cte="all_current_spans", 
    left_join_cte="add_snooze_info", 
    additional_columns="metadata_denial"
)}
"""

US_ME_OVERDUE_FOR_DISCHARGE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_ME_OVERDUE_FOR_DISCHARGE_VIEW_NAME,
    view_query_template=US_ME_OVERDUE_FOR_DISCHARGE_QUERY_TEMPLATE,
    description=US_ME_OVERDUE_FOR_DISCHARGE_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    supplemental_dataset=SUPPLEMENTAL_DATA_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_ME
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_OVERDUE_FOR_DISCHARGE_VIEW_BUILDER.build_and_print()
