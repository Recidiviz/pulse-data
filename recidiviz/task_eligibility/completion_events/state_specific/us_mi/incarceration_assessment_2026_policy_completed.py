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
"""Defines a view that shows when a subsequent (non-intake) classification review has
occurred for someone in Michigan, under the 2026 classification policy.

TODO(MI-7750): This currently marks every non-intake classification decision as an
event under the 2026 policy, since MI's classification data doesn't yet distinguish
which policy version a decision was made under (unlike TN, whose equivalent completion
event additionally filters on assessment_type = "RCAF" to isolate 2026-policy
reclassification decisions). Add an equivalent filter here once MI's data supports it,
so this doesn't fire for pre-2026-policy reclassifications. Same root cause as the
identical caveat on incarceration_intake_assessment_2026_policy_completed.py.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_completion_event_builders import (
    non_intake_classification_decision_completed_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    non_intake_classification_decision_completed_view_builder(
        state_code=StateCode.US_MI,
        description=__doc__,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
