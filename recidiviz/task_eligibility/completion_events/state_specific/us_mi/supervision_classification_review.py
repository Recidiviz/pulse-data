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
"""Defines a view that shows all classification review dates for clients in Michigan.
"""
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Defines a view that shows all classification review dates for clients in Michigan. Classification
reviews happen every 6 months after the initial classification review and should result in a supervision level 
downgrade unless there are extenuating circumstances. 
"""

_QUERY_TEMPLATE = """
SELECT DISTINCT
    pei.state_code,
    pei.person_id,
    CAST(SAFE_CAST(item_complete_date AS DATETIME) AS DATE) AS completion_event_date,
FROM `{project_id}.{raw_data_up_to_date_views_dataset}.ADH_OFFENDER_SCHEDULE_latest` schedule 
LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.ADH_REFERENCE_CODE_latest` ref1 
    ON schedule.schedule_type_id = ref1.reference_code_id
LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.ADH_REFERENCE_CODE_latest` ref2
    ON schedule.schedule_reason_id = ref2.reference_code_id
INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
    ON pei.state_code = 'US_MI'
    AND pei.id_type = 'US_MI_DOC_BOOK'
    AND pei.external_id = schedule.offender_booking_id
WHERE ref1.description = 'Classification Review'
AND item_complete_date IS NOT NULL
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_MI,
        completion_event_type=TaskCompletionEventType.SUPERVISION_CLASSIFICATION_REVIEW,
        description=_DESCRIPTION,
        completion_event_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_MI,
            instance=DirectIngestInstance.PRIMARY,
        ),
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
