# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query for relevant metadata needed to support supervision level downgrade opportunity in Tennessee
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_NAME = (
    "us_tn_supervision_level_downgrade_record"
)

US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_DESCRIPTION = """
    Query for relevant metadata needed to support supervision level downgrade opportunity in Tennessee 
    """
US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_QUERY_TEMPLATE = f"""
WITH base_query AS (
    SELECT 
       pei.external_id, 
       tes.state_code,
       ANY_VALUE(tes.reasons) AS reasons,
       ARRAY_AGG(STRUCT(CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS violation_date, ContactNoteType AS violation_code)) AS metadata_violations
    FROM `{{project_id}}.{{task_eligibility_dataset}}.supervision_level_downgrade_materialized`  tes
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      USING(person_id)
    /*
    In TN, we may see supervision levels higher than assessment levels for valid reasons. While these reasons are not captured
    in the data, having certain contact codes indicating a violation or recent arrest since the most recent assessment date
    may suggest a valid override, so we want to surface that information
    */
    LEFT JOIN `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.ContactNoteType_latest` contact
        ON pei.external_id = contact.OffenderID
        AND CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) >= CAST(JSON_VALUE(reasons[0].reason.latest_assessment_date) AS DATE)
        AND ContactNoteType IN ('PWAR','VWAR','VRPT','ARRP')
    WHERE CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND {nonnull_end_date_exclusive_clause('tes.end_date')}
        AND tes.is_eligible
        AND tes.state_code = 'US_TN'
    GROUP BY 1,2
)

SELECT
    external_id,
    state_code,
    reasons,
    IF(metadata_violations[offset(0)].violation_date IS NULL, [], metadata_violations) AS metadata_violations
    FROM base_query
"""

US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_NAME,
    view_query_template=US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_QUERY_TEMPLATE,
    description=US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_TN
    ),
    should_materialize=True,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.build_and_print()
