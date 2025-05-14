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
"""Powers report that shows US_ND residents who may be waiting for a transfer to be eligible to transfer to minimum security unit"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.us_nd_query_fragments import (
    get_minimum_housing_referals_query,
    get_recent_referrals_query,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override

US_ND_TRANSFER_TO_MIN_ELIGIBLE_WAITING_FOR_TRANSFER_VIEW_NAME = (
    "us_nd_transfer_to_min_eligible_waiting_for_transfer"
)

US_ND_TRANSFER_TO_MIN_ELIGIBLE_WAITING_FOR_TRANSFER_QUERY_TEMPLATE = f"""
{get_minimum_housing_referals_query(join_with_completion_events=False, keep_latest_referral_only=True)},
{get_recent_referrals_query(evaluation_result='Approved')}"""

US_ND_TRANSFER_TO_MIN_ELIGIBLE_WAITING_FOR_TRANSFER_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.USER_METRICS_DATASET_ID,
    view_id=US_ND_TRANSFER_TO_MIN_ELIGIBLE_WAITING_FOR_TRANSFER_VIEW_NAME,
    view_query_template=US_ND_TRANSFER_TO_MIN_ELIGIBLE_WAITING_FOR_TRANSFER_QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=True,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_ND
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_PRODUCTION):
        US_ND_TRANSFER_TO_MIN_ELIGIBLE_WAITING_FOR_TRANSFER_VIEW_BUILDER.build_and_print()
