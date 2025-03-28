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
"""Identify when clients in TN have been granted Suspension of Direct Supervision (SDS).

NB: this completion event only covers transfers to SDS for parole (and does not include
transfers to the parallel version for probation, called Judicial Suspension of Direct
Supervision [JSS]).

In TN, changes to supervision levels are generally backdated or postdated to the 1st of
the month. To work around the backdating (which can make it appear in the data as if a
client's level was changed before they actually experienced that change in real life),
we use an analyst view here which incorporates data from `SupervisionPlan` to infer the
date on which an officer actually initiated the transfer in cases of backdating. We
don't try to work around postdating because we are interested in tracking when the
client actually experienced the transition, and in cases of postdating, the client still
wouldn't experience the transition until the later date.
"""

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
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

# TODO(#40144): If we decide to undo the backdating workarounds for TN, we'll need to
# revise this completion event.
_QUERY_TEMPLATE = f"""
    WITH supervision_level_raw_text_sessions AS (
        SELECT
            state_code,
            person_id,
            start_date,
            supervision_level_raw_text,
            LAG(supervision_level_raw_text) OVER continuous_session_window AS previous_supervision_level_raw_text_within_continuous_session,
        FROM `{{project_id}}.{{analyst_dataset}}.us_tn_supervision_level_raw_text_sessions_inferred_materialized`
        WINDOW continuous_session_window AS (
            PARTITION BY state_code, person_id, date_gap_id
            ORDER BY start_date
        )
    )
    SELECT
        state_code,
        person_id,
        start_date AS completion_event_date,
    FROM supervision_level_raw_text_sessions
    WHERE supervision_level_raw_text IN ({list_to_query_string(SDS_SUPERVISION_LEVELS_RAW_TEXT, quoted=True)})
        /* Filter out SDS supervision-level session starts that are coming immediately
        after an existing SDS session. This is done to handle the switch from the
        previous to the current supervision-level codes (because, for example, we don't
        want to count the start of a session with '9SD' as a new start if the session
        before it was 'SDS', meaning that the person was already on SDS and the new
        session start just reflects the change in supervision-level codes). */
        AND COALESCE(previous_supervision_level_raw_text_within_continuous_session, 'UNKNOWN') NOT IN ({list_to_query_string(SDS_SUPERVISION_LEVELS_RAW_TEXT, quoted=True)})
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        completion_event_type=TaskCompletionEventType.TRANSFER_TO_NO_CONTACT_PAROLE,
        description=__doc__,
        completion_event_query_template=_QUERY_TEMPLATE,
        analyst_dataset=ANALYST_VIEWS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
