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
"""Builder for a task eligibility spans view that shows the spans of time during which
a resident in MI is eligible for a security assessment to facilitate a custody level downgrade because they have gone 6
months without a class I or II misconduct and meet other eligibility criteria.
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_housing_unit_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import custody_level_downgrade
from recidiviz.task_eligibility.criteria.state_specific.us_mi import (
    has_assessment_since_latest_class_i_or_ii_misconduct,
    management_level_greater_than_confinement_level,
    management_level_less_than_or_equal_to_custody_level,
    management_level_within_six_points_of_lower_level,
    no_class_i_or_ii_misconduct_in_current_supersession,
    no_class_i_or_ii_misconduct_in_six_months_and_no_security_assessment,
)
from recidiviz.task_eligibility.criteria_condition import (
    LessThanOrEqualCriteriaCondition,
    PickNCompositeCriteriaCondition,
    TimeDependentCriteriaCondition,
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

_NO_CLASS_I_OR_II_MISCONDUCT_OR_ASSESSMENT_AFTER_MISCONDUCT = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.OR,
    criteria_name="US_MI_NO_CLASS_I_OR_II_MISCONDUCT_OR_ASSESSMENT_AFTER_MISCONDUCT",
    sub_criteria_list=[
        no_class_i_or_ii_misconduct_in_current_supersession.VIEW_BUILDER,
        has_assessment_since_latest_class_i_or_ii_misconduct.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[],
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    task_name="CUSTODY_LEVEL_DOWNGRADE_NEEDS_ASSESSMENT",
    description=__doc__,
    candidate_population_view_builder=general_housing_unit_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        management_level_greater_than_confinement_level.VIEW_BUILDER,
        management_level_within_six_points_of_lower_level.VIEW_BUILDER,
        management_level_less_than_or_equal_to_custody_level.VIEW_BUILDER,
        no_class_i_or_ii_misconduct_in_six_months_and_no_security_assessment.VIEW_BUILDER,
        _NO_CLASS_I_OR_II_MISCONDUCT_OR_ASSESSMENT_AFTER_MISCONDUCT,
    ],
    completion_event_builder=custody_level_downgrade.VIEW_BUILDER,
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            TimeDependentCriteriaCondition(
                criteria=no_class_i_or_ii_misconduct_in_six_months_and_no_security_assessment.VIEW_BUILDER,
                reasons_date_field="six_month_misconduct_free_date",
                interval_length=1,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Within one month of becoming six months misconduct-free",
            ),
            LessThanOrEqualCriteriaCondition(
                criteria=no_class_i_or_ii_misconduct_in_six_months_and_no_security_assessment.VIEW_BUILDER,
                reasons_numerical_field="assessed_after_six_months_misconduct_free_date",
                # Value 0 indicates the resident still needs to be re-screened
                value=0,
                description="Has not been re-screened since becoming six months misconduct-free",
            ),
        ],
        at_least_n_conditions_true=2,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
