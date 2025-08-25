# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
a resident in MI is eligible for a custody level downgrade because they have gone 6 months without a
class I or II misconduct and meet other eligibility criteria.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import custody_level_downgrade
from recidiviz.task_eligibility.criteria.state_specific.us_mi import (
    management_level_greater_than_confinement_level,
    management_level_within_six_points_of_lower_level,
    no_class_i_or_ii_misconduct_in_six_months_and_no_security_assessment,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    task_name="CUSTODY_LEVEL_DOWNGRADE",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        management_level_greater_than_confinement_level.VIEW_BUILDER,
        management_level_within_six_points_of_lower_level.VIEW_BUILDER,
        no_class_i_or_ii_misconduct_in_six_months_and_no_security_assessment.VIEW_BUILDER,
    ],
    completion_event_builder=custody_level_downgrade.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
