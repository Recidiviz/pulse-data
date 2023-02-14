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
someone in MI is eligible for early discharge from parole or dual supervision.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.state_specific.us_mi import (
    parole_dual_supervision_population_not_high_or_sai,
)
from recidiviz.task_eligibility.completion_events import early_discharge
from recidiviz.task_eligibility.criteria.general import (
    serving_at_least_one_year_on_parole_supervision,
    supervision_not_past_full_term_completion_date,
    supervision_past_half_full_term_release_date,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mi import (
    no_active_ppo,
    no_new_ineligible_offenses_for_early_discharge_from_supervision,
    no_owi_violation_on_parole_dual_supervision,
    not_serving_ineligible_offenses_for_early_discharge_from_parole_dual_supervision,
    parole_dual_supervision_past_early_discharge_date,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which someone in MI is eligible
to request early discharge from supervision through the parole board.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    task_name="COMPLETE_DISCHARGE_EARLY_FROM_PAROLE_DUAL_SUPERVISION_REQUEST",
    description=_DESCRIPTION,
    candidate_population_view_builder=parole_dual_supervision_population_not_high_or_sai.VIEW_BUILDER,
    criteria_spans_view_builders=[
        supervision_not_past_full_term_completion_date.VIEW_BUILDER,
        serving_at_least_one_year_on_parole_supervision.VIEW_BUILDER,
        parole_dual_supervision_past_early_discharge_date.VIEW_BUILDER,
        not_serving_ineligible_offenses_for_early_discharge_from_parole_dual_supervision.VIEW_BUILDER,
        no_new_ineligible_offenses_for_early_discharge_from_supervision.VIEW_BUILDER,
        no_active_ppo.VIEW_BUILDER,
        no_owi_violation_on_parole_dual_supervision.VIEW_BUILDER,
        supervision_past_half_full_term_release_date.VIEW_BUILDER,
    ],
    completion_event_builder=early_discharge.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
