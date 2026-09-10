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
"""Defines a view that shows when an initial custody classification decision has
been made for someone in Michigan, under the 2026 classification policy.

TODO(MI-7783): This currently never fires for MI, since it joins to
custody_classification_assessment_dates, which has no MI source yet.
Non-functional for MI until that data source exists.

TODO(MI-7750): Once MI has a source in custody_classification_assessment_dates,
this will mark every initial classification decision as an event under the 2026
policy, since MI's classification data doesn't yet distinguish which policy
version a decision was made under (unlike TN, whose equivalent completion event
additionally filters on assessment_type = "DCAF" to isolate 2026-policy intake
decisions). Add an equivalent filter here once MI's data supports it, so this
doesn't fire for pre-2026-policy initial classifications.
"""
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import TASK_ELIGIBILITY_CRITERIA_GENERAL
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = """
    SELECT
        c.state_code,
        c.person_id,
        c.classification_decision_date AS completion_event_date,
    FROM `{project_id}.{analyst_views_dataset}.custody_classification_assessment_dates_materialized` c
    -- This criteria only exists for spans where someone has received their first
    -- classification after starting or re-starting state-prison custody, so joining
    -- here limits to only intake completion events
    INNER JOIN
        `{project_id}.{task_eligibility_criteria_general_dataset}.has_initial_classification_in_state_prison_custody_materialized` i
        ON c.person_id = i.person_id
        AND c.state_code = i.state_code
        AND c.classification_decision_date = i.start_date
    WHERE c.state_code = 'US_MI'
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = StateSpecificTaskCompletionEventBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    completion_event_type=TaskCompletionEventType.INCARCERATION_INTAKE_ASSESSMENT_2026_POLICY_COMPLETED,
    description=__doc__,
    completion_event_query_template=_QUERY_TEMPLATE,
    analyst_views_dataset=ANALYST_VIEWS_DATASET,
    task_eligibility_criteria_general_dataset=TASK_ELIGIBILITY_CRITERIA_GENERAL,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
