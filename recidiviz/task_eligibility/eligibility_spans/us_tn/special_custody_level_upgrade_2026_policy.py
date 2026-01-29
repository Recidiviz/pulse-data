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
"""Builder for a task eligibility spans view that shows the spans of time during which
someone in TN is eligible for a special upgrade, under the 2026 classification policy.
Someone is eligible if they are assessed at a higher level under the new policy than
under the old policy, and have not yet been re-classified under the new policy.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population_state_prison_exclude_safekeeping,
)
from recidiviz.task_eligibility.completion_events.general import custody_level_upgrade
from recidiviz.task_eligibility.criteria.general import (
    custody_level_lower_than_recommended,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    custody_level_lower_than_recommended_2026_policy,
    has_not_been_classified_under_2026_policy,
    ineligible_for_annual_reclassification,
    ineligible_for_initial_classification,
)
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

custody_level_not_lower_than_recommended_previous_policy_view_builder = (
    StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
        sub_criteria=custody_level_lower_than_recommended.VIEW_BUILDER
    )
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY",
    description=__doc__,
    candidate_population_view_builder=incarceration_population_state_prison_exclude_safekeeping.VIEW_BUILDER,
    criteria_spans_view_builders=[
        ineligible_for_annual_reclassification.VIEW_BUILDER,
        ineligible_for_initial_classification.VIEW_BUILDER,
        custody_level_lower_than_recommended_2026_policy.VIEW_BUILDER,
        custody_level_not_lower_than_recommended_previous_policy_view_builder,
        has_not_been_classified_under_2026_policy.VIEW_BUILDER,
    ],
    completion_event_builder=custody_level_upgrade.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
