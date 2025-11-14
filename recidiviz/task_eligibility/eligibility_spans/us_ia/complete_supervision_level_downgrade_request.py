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
"""Shows the spans of time during which someone in IA is eligible for a supervision level downgrade."""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    transfer_to_limited_supervision_from_minimum,
)
from recidiviz.task_eligibility.criteria.general import (
    no_supervision_level_downgrade_within_6_months,
    no_supervision_violation_report_within_6_months_using_response_date,
    not_serving_a_life_sentence_on_supervision,
    supervision_case_type_is_not_sex_offense,
    supervision_level_is_medium_or_minimum,
    supervision_level_is_minimum,
    supervision_level_is_not_residential_program,
    supervision_type_is_not_investigation,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ia import (
    marked_ineligible_for_early_discharge,
    no_open_supervision_modifiers,
    not_eligible_for_early_discharge,
    not_serving_ineligible_offense_for_early_discharge,
    serving_supervision_case_at_least_90_days,
)
from recidiviz.task_eligibility.criteria_condition import NotEligibleCriteriaCondition
from recidiviz.task_eligibility.eligibility_spans.us_ia.complete_early_discharge_form import (
    not_supervision_past_group_full_term_completion_date_or_upcoming_30_days_view_builder,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

us_ia_not_eligible_or_marked_ineligible_for_early_discharge_view_builder = (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
        logic_type=TaskCriteriaGroupLogicType.OR,
        criteria_name="US_IA_NOT_ELIGIBLE_OR_MARKED_INELIGIBLE_FOR_EARLY_DISCHARGE",
        sub_criteria_list=[
            not_eligible_for_early_discharge.VIEW_BUILDER,
            marked_ineligible_for_early_discharge.VIEW_BUILDER,
        ],
    )
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IA,
    task_name="COMPLETE_SUPERVISION_LEVEL_DOWNGRADE_REQUEST",
    description=__doc__,
    candidate_population_view_builder=active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        supervision_level_is_medium_or_minimum.VIEW_BUILDER,
        no_supervision_violation_report_within_6_months_using_response_date.VIEW_BUILDER,
        no_open_supervision_modifiers.VIEW_BUILDER,
        supervision_case_type_is_not_sex_offense.VIEW_BUILDER,
        not_serving_ineligible_offense_for_early_discharge.VIEW_BUILDER,
        not_supervision_past_group_full_term_completion_date_or_upcoming_30_days_view_builder,
        supervision_type_is_not_investigation.VIEW_BUILDER,
        serving_supervision_case_at_least_90_days.VIEW_BUILDER,
        supervision_level_is_not_residential_program.VIEW_BUILDER,
        no_supervision_level_downgrade_within_6_months.VIEW_BUILDER,
        us_ia_not_eligible_or_marked_ineligible_for_early_discharge_view_builder,
        not_serving_a_life_sentence_on_supervision.VIEW_BUILDER,
        # TODO(#48523): remove this once we are allowed to show L3 eligible clients again
        supervision_level_is_minimum.VIEW_BUILDER,
    ],
    # TODO(#48523): update this once we are allowed to show L3 eligible clients again
    completion_event_builder=transfer_to_limited_supervision_from_minimum.VIEW_BUILDER,
    almost_eligible_condition=NotEligibleCriteriaCondition(
        criteria=us_ia_not_eligible_or_marked_ineligible_for_early_discharge_view_builder,
        description="If eligible for early discharge, mark as almost eligible for supervision level downgrade."
        "This will be used to populate a pending eligibility tab.",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
