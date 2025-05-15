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
    no_supervision_violation_report_within_6_months_using_response_date,
    not_serving_a_life_sentence_on_supervision_or_supervision_out_of_state,
    supervision_case_type_is_not_sex_offense,
    supervision_past_full_term_completion_date_or_upcoming_30_days,
    supervision_type_is_not_investigation,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ia import (
    no_open_supervision_modifiers,
    not_excluded_from_early_discharge_by_parole_condition,
    not_serving_ineligible_offense_for_early_discharge,
    serving_supervision_case_at_least_90_days,
    supervision_fees_paid,
    supervision_level_is_0_not_available_1_2_or_3,
)
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder,
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
        supervision_level_is_0_not_available_1_2_or_3.VIEW_BUILDER,
        no_supervision_violation_report_within_6_months_using_response_date.VIEW_BUILDER,
        no_open_supervision_modifiers.VIEW_BUILDER,
        supervision_case_type_is_not_sex_offense.VIEW_BUILDER,
        supervision_fees_paid.VIEW_BUILDER,
        not_serving_ineligible_offense_for_early_discharge.VIEW_BUILDER,
        StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
            sub_criteria=supervision_past_full_term_completion_date_or_upcoming_30_days.VIEW_BUILDER,
        ),
        not_serving_a_life_sentence_on_supervision_or_supervision_out_of_state.VIEW_BUILDER,
        supervision_type_is_not_investigation.VIEW_BUILDER,
        serving_supervision_case_at_least_90_days.VIEW_BUILDER,
        not_excluded_from_early_discharge_by_parole_condition.VIEW_BUILDER,
    ],
    completion_event_builder=early_discharge.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
