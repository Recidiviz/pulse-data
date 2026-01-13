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
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.general import early_discharge
from recidiviz.task_eligibility.criteria.general import (
    at_least_6_months_since_most_recent_positive_drug_test,
    continuous_employment_or_student_or_alt_for_3_months,
    on_supervision_at_least_6_months,
    supervision_housing_is_permanent_for_3_months,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ut import (
    has_completed_ordered_assessments,
    no_medhigh_supervision_violation_within_12_months,
    no_request_for_termination_in_current_supervision_super_session,
    no_risk_level_increase_of_5_percent,
    risk_level_reduction_of_one_or_more,
    risk_level_stayed_moderate_or_low,
    risk_score_reduction_5_percent_or_more,
    supervision_or_supervision_out_of_state_past_half_full_term_release_date,
)
from recidiviz.task_eligibility.criteria_condition import (
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

RISK_REDUCTION_FOR_ET_VIEW_BUILDER = (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
        logic_type=TaskCriteriaGroupLogicType.OR,
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
)[0]

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_UT,
    task_name="EARLY_TERMINATION_FROM_SUPERVISION_REQUEST",
    description=__doc__,
    candidate_population_view_builder=active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        # 1. Completion of ordered assessments and any recommended treatment or programming
        has_completed_ordered_assessments.VIEW_BUILDER,
        # 2. Risk reduction criteria
        RISK_REDUCTION_FOR_ET_VIEW_BUILDER,
        # 3. Compliance and stability
        supervision_housing_is_permanent_for_3_months.VIEW_BUILDER,
        continuous_employment_or_student_or_alt_for_3_months.VIEW_BUILDER,
        no_medhigh_supervision_violation_within_12_months.VIEW_BUILDER,
        on_supervision_at_least_6_months.VIEW_BUILDER,
        at_least_6_months_since_most_recent_positive_drug_test.VIEW_BUILDER,
        no_risk_level_increase_of_5_percent.VIEW_BUILDER,
        # Past ET Review date/ half-time date
        supervision_or_supervision_out_of_state_past_half_full_term_release_date.VIEW_BUILDER,
        # There hasn't been a request for termination in their current supervision session
        no_request_for_termination_in_current_supervision_super_session.VIEW_BUILDER,
    ],
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        # There are two separate ways to become almost eligible:
        #   1) You are missing one or both of the treatment or employment criteria
        #   2) You are within 2 years of your half-time date
        sub_conditions_list=[
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    NotEligibleCriteriaCondition(
                        criteria=has_completed_ordered_assessments.VIEW_BUILDER,
                        description="Only missing the completion of ordered assessments/treatment/programming criteria",
                    ),
                    NotEligibleCriteriaCondition(
                        criteria=continuous_employment_or_student_or_alt_for_3_months.VIEW_BUILDER,
                        description="Only missing the continuous employment for 3 months criteria",
                    ),
                ],
                at_most_n_conditions_true=2,
            ),
            TimeDependentCriteriaCondition(
                criteria=supervision_or_supervision_out_of_state_past_half_full_term_release_date.VIEW_BUILDER,
                reasons_date_field="half_full_term_release_date",
                interval_length=2,
                interval_date_part=BigQueryDateInterval.YEAR,
                description="Within 2 years of 1/2 full term release date",
            ),
        ],
        at_most_n_conditions_true=1,
    ),
    completion_event_builder=early_discharge.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
