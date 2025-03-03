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
"""
Shows the spans of time during which someone in UT is eligible
for an Early Termination from Supevision
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.general import early_discharge
from recidiviz.task_eligibility.criteria.general import (
    at_least_3_months_since_most_recent_positive_drug_test,
    on_supervision_at_least_6_months,
    supervision_continuous_employment_for_3_months,
    supervision_housing_is_permanent_or_temporary_for_3_months,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ut import (
    has_completed_ordered_assessments,
    no_medhigh_supervision_violation_within_3_months,
    no_risk_level_increase_of_15_percent,
    risk_level_reduction_of_one_or_more,
    risk_level_stayed_moderate_or_low,
    risk_score_reduction_5_percent_or_more,
)
from recidiviz.task_eligibility.criteria_condition import (
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    OrTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_UT,
    task_name="EARLY_TERMINATION_FROM_SUPERVISION_REQUEST",
    description=__doc__,
    candidate_population_view_builder=active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        # 1. Completion of ordered assessments and any recommended treatment or programming
        has_completed_ordered_assessments.VIEW_BUILDER,
        # 2. Risk reduction criteria
        OrTaskCriteriaGroup(
            criteria_name="US_UT_RISK_REDUCTION_FOR_ET",
            sub_criteria_list=[
                risk_level_reduction_of_one_or_more.VIEW_BUILDER,
                risk_level_stayed_moderate_or_low.VIEW_BUILDER,
                risk_score_reduction_5_percent_or_more.VIEW_BUILDER,
            ],
            allowed_duplicate_reasons_keys=[
                "assessment_level_raw_text",
                "first_assessment_level_raw_text",
            ],
        ),
        # 3. Compliance and stability
        supervision_housing_is_permanent_or_temporary_for_3_months.VIEW_BUILDER,
        supervision_continuous_employment_for_3_months.VIEW_BUILDER,
        no_medhigh_supervision_violation_within_3_months.VIEW_BUILDER,
        on_supervision_at_least_6_months.VIEW_BUILDER,
        at_least_3_months_since_most_recent_positive_drug_test.VIEW_BUILDER,
        no_risk_level_increase_of_15_percent.VIEW_BUILDER,
        # Past ET Review date/ half-time date
        # TODO(#38964) - Add this criteria back after the data is flowing through sessions
        # supervision_or_supervision_out_of_state_past_half_full_term_release_date.VIEW_BUILDER,
    ],
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            NotEligibleCriteriaCondition(
                criteria=has_completed_ordered_assessments.VIEW_BUILDER,
                description="Only missing the completion of ordered assessments/treatment/programming criteria",
            ),
            NotEligibleCriteriaCondition(
                criteria=supervision_continuous_employment_for_3_months.VIEW_BUILDER,
                description="Only missing the continuous employment for 3 months criteria",
            ),
            # TODO(#38964) - Add this criteria back after the data is flowing through sessions
            # NotEligibleCriteriaCondition(
            #     criteria=supervision_or_supervision_out_of_state_past_half_full_term_release_date.VIEW_BUILDER,
            #     description="Only missing the past ET Review date/half-time date criteria",
            # ),
        ],
        at_most_n_conditions_true=1,
    ),
    completion_event_builder=early_discharge.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
