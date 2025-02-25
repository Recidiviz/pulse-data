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
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MI_SUPERVISION_CLASSIFICATION_REVIEW_VIEW_NAME = (
    "us_mi_supervision_classification_review"
)

US_MI_SUPERVISION_CLASSIFICATION_REVIEW_VIEW_DESCRIPTION = """Defines a view that shows all classification review dates for clients in Michigan. Classification
reviews happen every 6 months after the initial classification review and should result in a supervision level 
downgrade unless there are extenuating circumstances. 
"""

US_MI_SUPERVISION_CLASSIFICATION_REVIEW_QUERY_TEMPLATE = """
/* OMNI */ 
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
-- this filter shouldn't actually do anything because the data should have been frozen by 2023-08-14
AND CAST(SAFE_CAST(item_complete_date AS DATETIME) AS DATE) < "2023-08-14"

UNION ALL 

/* COMS */ 
SELECT DISTINCT
    pei.state_code,
    pei.person_id,
    CAST(SAFE_CAST(Completed_date AS DATETIME) AS DATE) AS completion_event_date,
FROM `{project_id}.{raw_data_up_to_date_views_dataset}.COMS_Supervision_Schedule_Activities_latest` schedule 
INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
    ON pei.state_code = 'US_MI'
    AND pei.id_type = 'US_MI_DOC'
    AND pei.external_id = LTRIM(schedule.Offender_Number, '0')
WHERE schedule.Activity = "Classification Review"
AND Completed_date IS NOT NULL
AND CAST(SAFE_CAST(Completed_date AS DATETIME) AS DATE) >= "2023-08-14"
"""

US_MI_SUPERVISION_CLASSIFICATION_REVIEW_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_MI_SUPERVISION_CLASSIFICATION_REVIEW_VIEW_NAME,
    description=US_MI_SUPERVISION_CLASSIFICATION_REVIEW_VIEW_DESCRIPTION,
    view_query_template=US_MI_SUPERVISION_CLASSIFICATION_REVIEW_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MI,
        instance=DirectIngestInstance.PRIMARY,
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_SUPERVISION_CLASSIFICATION_REVIEW_VIEW_BUILDER.build_and_print()
