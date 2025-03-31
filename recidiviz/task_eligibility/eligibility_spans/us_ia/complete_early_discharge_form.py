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
"""Shows the spans of time during which someone in IA is eligible for early discharge from supervision."""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    active_supervision_and_supervision_out_of_state_population,
)
from recidiviz.task_eligibility.completion_events.general import early_discharge
from recidiviz.task_eligibility.criteria.general import (
    no_supervision_violation_within_6_months,
    supervision_level_is_medium_or_lower,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ia import (
    completed_mandated_programs,
    no_open_supervision_modifiers,
    no_pending_charges,
    no_sex_offender_specialty,
    not_serving_ineligible_offense_for_early_discharge,
    supervision_fees_paid,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which someone in IA is eligible for early discharge from
supervision."""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IA,
    task_name="COMPLETE_EARLY_DISCHARGE_FORM",
    description=_DESCRIPTION,
    candidate_population_view_builder=active_supervision_and_supervision_out_of_state_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        supervision_level_is_medium_or_lower.VIEW_BUILDER,  # TODO(#39493) Address IC-OUT cases with missing supervision levels
        no_pending_charges.VIEW_BUILDER,
        no_supervision_violation_within_6_months.VIEW_BUILDER,
        no_open_supervision_modifiers.VIEW_BUILDER,
        no_sex_offender_specialty.VIEW_BUILDER,
        supervision_fees_paid.VIEW_BUILDER,
        completed_mandated_programs.VIEW_BUILDER,
        not_serving_ineligible_offense_for_early_discharge.VIEW_BUILDER,
    ],
    completion_event_builder=early_discharge.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
