"""Defines a view that shows all security committee classification review dates for residents in Michigan.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    NORMALIZED_STATE_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MI_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_NAME = (
    "us_mi_security_classification_committee_review"
)

US_MI_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_DESCRIPTION = """Defines a view that shows all security
committee classification review dates for residents in solitary confinement in Michigan.
"""

US_MI_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_QUERY_TEMPLATE = """
/* OMNI */ 
SELECT DISTINCT
    pei.state_code,
    pei.person_id,
    DATE(item_complete_date) AS completion_event_date,
FROM `{project_id}.{raw_data_up_to_date_views_dataset}.ADH_OFFENDER_SCHEDULE_latest` schedule 
LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.ADH_REFERENCE_CODE_latest` ref1 
    ON schedule.schedule_type_id = ref1.reference_code_id
INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
    ON pei.state_code = 'US_MI'
    AND pei.id_type = 'US_MI_DOC_BOOK'
    AND pei.external_id = schedule.offender_booking_id
WHERE ref1.description = 'SCC - Security Classification Committee'
AND item_complete_date IS NOT NULL
-- this filter shouldn't actually do anything because the data should have been frozen by 2023-08-14
AND DATE(item_complete_date) < "2023-08-14"

UNION ALL 

/* COMS */ 
SELECT DISTINCT
    pei.state_code,
    pei.person_id,
    DATE(completed_date) AS completion_event_date,
FROM `{project_id}.{raw_data_up_to_date_views_dataset}.COMS_Supervision_Schedule_Activities_latest` schedule 
INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
    ON pei.state_code = 'US_MI'
    AND pei.id_type = 'US_MI_DOC'
    AND pei.external_id = LTRIM(schedule.Offender_Number, '0')
WHERE schedule.Activity IN ('SCC - Security Classification Committee','SCC – ADD – 12 Month Review','SCC – Warden – 6 Month Review')
AND completed_date IS NOT NULL
AND DATE(completed_date) >= "2023-08-14"
"""

US_MI_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_MI_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_NAME,
    description=US_MI_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_DESCRIPTION,
    view_query_template=US_MI_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MI,
        instance=DirectIngestInstance.PRIMARY,
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_BUILDER.build_and_print()
