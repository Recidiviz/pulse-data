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
"""
Queries information needed to surface eligible folks to be early released on a Transition Program Release AZ.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AZ_RELEASE_TO_TPR_REQUEST_VIEW_NAME = "us_az_release_to_tpr_request_record"

US_AZ_RELEASE_TO_TPR_REQUEST_DESCRIPTION = """
    Queries information needed to surface eligible folks to be early released on a Transition Program Release AZ.
    """


US_AZ_RELEASE_TO_TPR_REQUEST_QUERY_TEMPLATE = f"""

WITH eligible_and_almost_eligible AS (
{join_current_task_eligibility_spans_with_external_id(state_code="'US_AZ'",
                                                      tes_task_query_view='release_to_tpr_request_materialized',
                                                      id_type="'US_AZ_ADC_NUMBER'",
                                                      eligible_only=True)}
)

SELECT 
    external_id,
    state_code,
    reasons,
    ineligible_criteria,
FROM 
    eligible_and_almost_eligible
"""

US_AZ_RELEASE_TO_TPR_REQUEST_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_AZ_RELEASE_TO_TPR_REQUEST_VIEW_NAME,
    view_query_template=US_AZ_RELEASE_TO_TPR_REQUEST_QUERY_TEMPLATE,
    description=US_AZ_RELEASE_TO_TPR_REQUEST_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_AZ
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_RELEASE_TO_TPR_REQUEST_VIEW_BUILDER.build_and_print()
