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
"""Defines a view that shows granted credit reductions produced after a
Credit Review (CR). Note that this considers approval for any number of days as a
completion event. Realistically, there are full approvals and partial approvals which
may be important to distinguish for things like impact tracking and tool functionality,
since partial approvals could potentially be resubmitted for more time off in the
future."""
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = """
SELECT DISTINCT
    'US_NC' AS state_code,
    pei.person_id,
    DATE(cr.EVTDT) AS completion_event_date
FROM `{project_id}.{us_nc_raw_data_up_to_date_dataset}.cr_outcome_latest` cr
INNER JOIN `{project_id}.us_nc_normalized_state.state_person_external_id` pei
    ON pei.external_id = cr.OPUS
WHERE
    -- Only count reviews where credit was actually awarded. Denied reviews will read
    -- "...AWARDED NO DAYS CREDIT."
    REGEXP_CONTAINS(LOWER(cr.COMMENTS), r'\\d+\\s+day')
    AND DATE(cr.EVTDT) IS NOT NULL
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = StateSpecificTaskCompletionEventBigQueryViewBuilder(
    state_code=StateCode.US_NC,
    completion_event_type=TaskCompletionEventType.GRANTED_SUPERVISION_SENTENCE_REDUCTION,
    description=__doc__,
    completion_event_query_template=_QUERY_TEMPLATE,
    us_nc_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_NC, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
