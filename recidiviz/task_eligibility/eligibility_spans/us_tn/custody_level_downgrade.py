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
"""Builder for a task eligibility spans view that shows the spans of time during which
someone in TN is eligible for a custody level downgrade.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population_state_prison_exclude_safekeeping,
)
from recidiviz.task_eligibility.completion_events.general import custody_level_downgrade
from recidiviz.task_eligibility.criteria.general import (
    custody_level_higher_than_recommended,
    custody_level_is_not_max,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    ineligible_for_annual_reclassification,
    ineligible_for_initial_classification,
    latest_caf_assessment_not_override,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="CUSTODY_LEVEL_DOWNGRADE",
    description=__doc__,
    candidate_population_view_builder=incarceration_population_state_prison_exclude_safekeeping.VIEW_BUILDER,
    criteria_spans_view_builders=[
        latest_caf_assessment_not_override.VIEW_BUILDER,
        ineligible_for_annual_reclassification.VIEW_BUILDER,
        ineligible_for_initial_classification.VIEW_BUILDER,
        custody_level_higher_than_recommended.VIEW_BUILDER,
        custody_level_is_not_max.VIEW_BUILDER,
    ],
    completion_event_builder=custody_level_downgrade.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
