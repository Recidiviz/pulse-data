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
"""Builder for a task eligiblity spans view that shows the spans of time during which
someone in TN is eligible for a custody level downgrade.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population_facility_filter,
)
from recidiviz.task_eligibility.completion_events.general import custody_level_downgrade
from recidiviz.task_eligibility.criteria.general import (
    custody_level_higher_than_recommended,
    custody_level_is_not_max,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    at_least_6_months_since_most_recent_incarceration_incident,
    has_had_at_least_1_incarceration_incident_past_year,
    ineligible_for_annual_reclassification,
    latest_caf_assessment_not_override,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Builder for a task eligiblity spans view that shows the spans of time during which
someone in TN is eligible for a custody level downgrade.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="CUSTODY_LEVEL_DOWNGRADE",
    description=_DESCRIPTION,
    candidate_population_view_builder=general_incarceration_population_facility_filter.VIEW_BUILDER,
    criteria_spans_view_builders=[
        latest_caf_assessment_not_override.VIEW_BUILDER,
        ineligible_for_annual_reclassification.VIEW_BUILDER,
        custody_level_higher_than_recommended.VIEW_BUILDER,
        custody_level_is_not_max.VIEW_BUILDER,
        # TODO(#23614): Remove these two criteria as part of widening the funnel for special downgrades
        has_had_at_least_1_incarceration_incident_past_year.VIEW_BUILDER,
        at_least_6_months_since_most_recent_incarceration_incident.VIEW_BUILDER,
    ],
    completion_event_builder=custody_level_downgrade.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
