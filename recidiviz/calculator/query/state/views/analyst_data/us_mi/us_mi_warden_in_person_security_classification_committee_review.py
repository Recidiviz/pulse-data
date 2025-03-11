"""Defines a view that shows all security committee classification review dates for which the warden was in person for
residents in solitary confinement in Michigan.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MI_WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_NAME = (
    "us_mi_warden_in_person_security_classification_committee_review"
)

US_MI_WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_DESCRIPTION = """Defines a view that shows all security
committee classification review dates for which the warden was in person for residents in solitary confinement in Michigan.
"""

US_MI_WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_QUERY_TEMPLATE = """
/* COMS */ 
SELECT DISTINCT
    pei.state_code,
    pei.person_id,
    DATE(completed_date) AS completion_event_date,
FROM `{project_id}.{raw_data_up_to_date_views_dataset}.COMS_Supervision_Schedule_Activities_latest` schedule 
INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
    ON pei.state_code = 'US_MI'
    AND pei.id_type = 'US_MI_DOC'
    AND pei.external_id = schedule.Offender_Number
WHERE schedule.Activity = 'SCC – Warden – 6 Month Review'
AND completed_date IS NOT NULL
"""

US_MI_WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_MI_WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_NAME,
    description=US_MI_WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_DESCRIPTION,
    view_query_template=US_MI_WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MI,
        instance=DirectIngestInstance.PRIMARY,
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_BUILDER.build_and_print()
