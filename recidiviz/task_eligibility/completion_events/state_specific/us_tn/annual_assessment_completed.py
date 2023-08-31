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
"""Defines a view that shows when hearings that were scheduled have occurred, regardless
of whether they were on time.
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

_DESCRIPTION = """Defines a view that shows when hearings that were scheduled have occurred, regardless
of whether they were on time.
"""

_QUERY_TEMPLATE = """
    -- TODO(#23526): Pull from state_assessment once ClassificationDecisionDate is in metadata
    SELECT state_code,
           person_id, 
           DATE(ClassificationDecisionDate) AS completion_event_date, 
    FROM
        `{project_id}.{raw_data_up_to_date_views_dataset}.Classification_latest` classification
    INNER JOIN
        `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
    ON
        classification.OffenderID = pei.external_id
    AND
        pei.state_code = 'US_TN'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderId, CAFDate ORDER BY CAST(ClassificationSequenceNumber AS INT64) DESC) = 1
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        completion_event_type=TaskCompletionEventType.ANNUAL_ASSESSMENT_COMPLETED,
        description=_DESCRIPTION,
        completion_event_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
        ),
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
