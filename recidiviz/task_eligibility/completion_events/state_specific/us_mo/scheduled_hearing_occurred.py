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
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.task_eligibility.utils.us_mo_query_fragments import hearings_dedup_cte
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Defines a view that shows when hearings that were scheduled have occurred, regardless
of whether they were on time.
"""

_QUERY_TEMPLATE = f"""
    WITH {hearings_dedup_cte()}
    ,
    hearings_with_review_dates AS (
        SELECT
            state_code,
            person_id,
            hearing_date,
            LEAD(hearing_date) OVER hearing_window AS next_hearing_date,
            next_review_date
        FROM hearings
        WHERE next_review_date IS NOT NULL
        WINDOW hearing_window AS (
            PARTITION BY state_code, person_id
            ORDER BY hearing_date ASC
        )
    )
    SELECT
        state_code,
        person_id,
        next_hearing_date AS completion_event_date,
    FROM hearings_with_review_dates
    WHERE next_hearing_date IS NOT NULL
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_MO,
        completion_event_type=TaskCompletionEventType.SCHEDULED_HEARING_OCCURRED,
        description=_DESCRIPTION,
        completion_event_query_template=_QUERY_TEMPLATE,
        us_mo_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_MO, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
