# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Defines a view that shows successful work-release approvals for 
clients in Maine. This happens when a client's custody level is lowered to 'Community'.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = """
SELECT
    state_code,
    person_id,
    SAFE_CAST(LEFT(CUSTODY_DATE, 10) AS DATE) AS completion_event_date,
# TODO(#16722): pull custody level from ingested data once it is hydrated in our schema
FROM `{project_id}.{raw_data_up_to_date_views_dataset}.CIS_112_CUSTODY_LEVEL_latest`
INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id`
    ON CIS_100_CLIENT_ID = external_id
    AND id_type = 'US_ME_DOC'
INNER JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.CIS_1017_CLIENT_SYS_latest` cl_sys
    ON CIS_1017_CLIENT_SYS_CD = CLIENT_SYS_CD
# Only include clients whose custody level was lowered to 'Community'
WHERE cl_sys.CLIENT_SYS_DESC = 'Community'
GROUP BY 1,2,3
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_ME,
        completion_event_type=TaskCompletionEventType.GRANTED_WORK_RELEASE,
        description=__doc__,
        completion_event_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_ME,
            instance=DirectIngestInstance.PRIMARY,
        ),
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
