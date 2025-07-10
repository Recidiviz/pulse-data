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
"""Task eligibility spans view that shows the spans of time when someone in MO is
eligible for work release.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import granted_work_release
from recidiviz.task_eligibility.criteria.general import (
    no_contraband_incarceration_incident_within_2_years,
    not_in_work_release,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mo import (
    educational_score_1_while_incarcerated,
    institutional_risk_score_1_while_incarcerated,
    mental_health_score_3_or_below_while_incarcerated,
    no_current_or_prior_excluded_offenses_work_release,
    no_escape_in_10_years_or_current_sentence,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

WORK_RELEASE_AND_OUTSIDE_CLEARANCE_SHARED_CRITERIA: list[
    TaskCriteriaBigQueryViewBuilder
] = [
    no_contraband_incarceration_incident_within_2_years.VIEW_BUILDER,
    # For outside clearance, we check that someone is not eligible for work release,
    # because if eligible, they should show up in the work-release opportunity instead
    # of outside clearance. We include `NOT_IN_WORK_RELEASE` as a shared criterion with
    # outside clearance here because if someone is already on work release, they won't
    # be eligible for work release but could therefore show up in the outside-clearance
    # eligibility pool, but we don't want to surface them there if they're already on
    # work release.
    not_in_work_release.VIEW_BUILDER,
    educational_score_1_while_incarcerated.VIEW_BUILDER,
    institutional_risk_score_1_while_incarcerated.VIEW_BUILDER,
    mental_health_score_3_or_below_while_incarcerated.VIEW_BUILDER,
    no_escape_in_10_years_or_current_sentence.VIEW_BUILDER,
]

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MO,
    task_name="WORK_RELEASE",
    description=__doc__,
    # TODO(#43388): Ensure that this is the correct candidate population.
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    # TODO(#42982): Finish adding in criteria and filling in stubs.
    criteria_spans_view_builders=[
        *WORK_RELEASE_AND_OUTSIDE_CLEARANCE_SHARED_CRITERIA,
        no_current_or_prior_excluded_offenses_work_release.VIEW_BUILDER,
    ],
    # TODO(#43358): Make sure this completion event is pulling in the proper data from
    # upstream to capture work-release events appropriately.
    completion_event_builder=granted_work_release.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
