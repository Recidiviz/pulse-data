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
"""Identifies (at the person level) when clients in MI have been discharged early
from supervision.
"""
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
WITH early_discharge_movements AS (
    # TODO(#57799): migrate to a COMS movement source when available
    SELECT
        state_code,
        person_id,
        DATE(movement_date) AS discharge_date,
    FROM
        `{project_id}.{us_mi_raw_data_up_to_date_dataset}.ADH_OFFENDER_EXTERNAL_MOVEMENT_latest` movements
    LEFT JOIN
        `{project_id}.normalized_state.state_person_external_id` pei
    ON
        pei.external_id = movements.offender_booking_id
        AND pei.id_type = 'US_MI_DOC_BOOK'
    WHERE
        -- Only select early discharge movement end reasons
        -- "30" (Early Discharge from Parole)
        -- "125" (Early Discharge from Probation)
        movements.movement_reason_id IN ("30", "125")
)
SELECT DISTINCT
    eds.state_code,
    eds.person_id,
    sessions.end_date_exclusive AS completion_event_date,
FROM early_discharge_movements eds
INNER JOIN
    `{project_id}.sessions.compartment_sessions_materialized` sessions
ON
    eds.state_code = sessions.state_code
    AND eds.person_id = sessions.person_id
    -- Count any discharge that was marked within 7 days of the session transition to liberty
    AND ABS(DATE_DIFF(discharge_date, sessions.end_date_exclusive, DAY)) <= 7
WHERE
    sessions.compartment_level_1 IN ("SUPERVISION", "SUPERVISION_OUT_OF_STATE")
    AND sessions.outflow_to_level_1 = "LIBERTY"
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_MI,
        completion_event_type=TaskCompletionEventType.EARLY_DISCHARGE,
        description=__doc__,
        completion_event_query_template=_QUERY_TEMPLATE,
        us_mi_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_MI, instance=DirectIngestInstance.PRIMARY
        ),
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
