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
"""Defines a view that shows furlough releases for clients in Maine.
"""
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Defines a view that shows furlough releases for clients in Maine.
"""
# TODO(#23182) Make sure were using the right dataset to catch furloughs

_QUERY_TEMPLATE = """
SELECT 
  peid.state_code,
  peid.person_id,
  SAFE_CAST(LEFT(mv.Movement_Date, 10) AS DATE) AS completion_event_date,
FROM `{project_id}.{raw_data_up_to_date_views_dataset}.CIS_309_MOVEMENT_latest` mv
INNER JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.CIS_3090_MOVEMENT_TYPE_latest` mvty
  ON mv.Cis_3090_Movement_Type_Cd = mvty.Movement_Type_Cd
    AND mvty.E_Movement_Type_Desc IN ('Furlough')
INNER JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.CIS_3093_MVMT_STATUS_latest` mvst
  ON mv.Cis_3093_Mvmt_Status_Cd = mvst.Mvmt_Status_Cd
    AND mvst.E_Mvmt_Status_Desc IN ('Complete')
INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` peid
  ON peid.external_id = mv.Cis_Client_Id
    AND peid.state_code = 'US_ME'
    AND id_type = 'US_ME_DOC'
GROUP BY 1,2,3
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_ME,
        completion_event_type=TaskCompletionEventType.GRANTED_FURLOUGH,
        description=_DESCRIPTION,
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
