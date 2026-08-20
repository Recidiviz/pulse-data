# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Identify when clients in TN have been granted Suspension of Direct Supervision (SDS).

NB: this completion event only covers transfers to SDS for parole (and does not include
transfers to the parallel version for probation, called Judicial Suspension of Direct
Supervision [JSS]).
"""

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.task_eligibility.utils.us_tn_query_fragments import (
    SDS_SUPERVISION_LEVELS_RAW_TEXT,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = f"""
    SELECT
        state_code,
        person_id,
        start_date AS completion_event_date,
    FROM `{{project_id}}.{{sessions_dataset}}.supervision_level_raw_text_sessions_materialized`
    WHERE state_code = 'US_TN'
        AND supervision_level_raw_text IN ({list_to_query_string(SDS_SUPERVISION_LEVELS_RAW_TEXT, quoted=True)})
        /* Filter out SDS supervision-level session starts that are coming immediately
        after an existing SDS session. This is done to handle the switch from the
        previous to the current supervision-level codes (because, for example, we don't
        want to count the start of a session with '9SD' as a new start if the session
        before it was 'SDS', meaning that the person was already on SDS and the new
        session start just reflects the change in supervision-level codes). */
        AND COALESCE(previous_supervision_level_raw_text, 'UNKNOWN') NOT IN ({list_to_query_string(SDS_SUPERVISION_LEVELS_RAW_TEXT, quoted=True)})
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        completion_event_type=TaskCompletionEventType.TRANSFER_TO_NO_CONTACT_PAROLE,
        description=__doc__,
        completion_event_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
