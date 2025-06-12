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
"""Identifies (at the person level) when clients in IA have been discharged early
from supervision.
"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.views.sentence_sessions.person_projected_date_sessions import (
    PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sessions import (
    COMPARTMENT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = f"""
-- Classify a transition from supervision (in state & out of state) to liberty as an early
-- discharge in Iowa (at the person-level granularity) if the discharge date is more than
-- 30 days before the projected full term release date.
SELECT DISTINCT
    sessions.state_code,
    sessions.person_id,
    sessions.end_date_exclusive AS completion_event_date,
FROM
    `{{project_id}}.{COMPARTMENT_SESSIONS_VIEW_BUILDER.table_for_query.to_str()}` sessions
INNER JOIN
    `{{project_id}}.{PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER.table_for_query.to_str()}` sentences
ON
    sessions.state_code = sentences.state_code
    AND sessions.person_id = sentences.person_id
    -- Pull the sentence spans that were active on the day the session ended (inclusive) because that
    -- is the span most likely to have the dates that were effective on the discharge date, this
    -- can still produce duplicate rows then there is a single day sentence span starting on the
    -- session inclusive end date. In the case of duplicates, count the transition as an early discharge
    -- if either span has a projected date date more than 30 days after the actual discharge date.
    AND DATE_SUB(sessions.end_date_exclusive, INTERVAL 1 DAY)
        BETWEEN sentences.start_date AND {nonnull_end_date_clause("sentences.end_date_exclusive")}
WHERE
    sessions.state_code = "US_IA"
    -- Count transitions from supervision -> liberty, including out-of-state (IC-OUT)
    AND compartment_level_1 IN ("SUPERVISION", "SUPERVISION_OUT_OF_STATE")
    AND outflow_to_level_1 = "LIBERTY"
    -- A discharge is considered "early" in IA if it is completed more than 30 days before the full term release date (TDD/SDD)
    AND sessions.end_date_exclusive < DATE_SUB(sentences.group_projected_full_term_release_date_max, INTERVAL 30 DAY)
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_IA,
        completion_event_type=TaskCompletionEventType.EARLY_DISCHARGE,
        description=__doc__,
        completion_event_query_template=_QUERY_TEMPLATE,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
