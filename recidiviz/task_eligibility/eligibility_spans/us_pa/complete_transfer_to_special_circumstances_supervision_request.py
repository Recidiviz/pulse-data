# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Shows the spans of time during which
someone in PA is eligible for transfer to Special Circumstances supervision.
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_pa import (
    transfer_to_special_circumstances_supervision,
)
from recidiviz.task_eligibility.criteria.general import (
    supervision_level_is_not_electronic_monitoring,
    supervision_level_is_not_limited,
)
from recidiviz.task_eligibility.criteria.state_specific.us_pa import (
    marked_ineligible_for_admin_supervision,
    meets_special_circumstances_criteria_for_time_served,
    no_high_sanctions_in_past_year,
    no_medium_sanctions_in_past_year,
    not_assigned_ineligible_stat_code,
    not_eligible_for_admin_supervision,
    not_on_sex_offense_protocol,
    serving_special_case,
)
from recidiviz.task_eligibility.criteria_condition import TimeDependentCriteriaCondition
from recidiviz.task_eligibility.eligibility_spans.us_pa.complete_transfer_to_administrative_supervision_request import (
    not_supervision_past_full_term_completion_date_or_upcoming_90_days_view_builder,
)
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateSpecificInvertedTaskCriteriaBigQueryViewBuilder,
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

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_PA,
    task_name="COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST",
    description=__doc__,
    candidate_population_view_builder=probation_parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        meets_special_circumstances_criteria_for_time_served.VIEW_BUILDER,
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.OR,
            criteria_name="US_PA_MEETS_SPECIAL_CIRCUMSTANCES_CRITERIA_FOR_SANCTIONS",
            sub_criteria_list=[
                StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
                    logic_type=TaskCriteriaGroupLogicType.AND,
                    criteria_name="US_PA_MEETS_SANCTION_CRITERIA_FOR_SPECIAL_CASE",
                    sub_criteria_list=[
                        serving_special_case.VIEW_BUILDER,
                        no_medium_sanctions_in_past_year.VIEW_BUILDER,
                        no_high_sanctions_in_past_year.VIEW_BUILDER,
                    ],
                    allowed_duplicate_reasons_keys=["latest_sanction_date"],
                ),
                StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
                    logic_type=TaskCriteriaGroupLogicType.AND,
                    criteria_name="US_PA_MEETS_SANCTION_CRITERIA_FOR_NON_SPECIAL_CASES",
                    sub_criteria_list=[
                        StateSpecificInvertedTaskCriteriaBigQueryViewBuilder(
                            sub_criteria=serving_special_case.VIEW_BUILDER,
                        ),
                        no_high_sanctions_in_past_year.VIEW_BUILDER,
                    ],
                    allowed_duplicate_reasons_keys=[],
                ),
            ],
            allowed_duplicate_reasons_keys=[
                "case_type",
                "sanction_type",
                "latest_sanction_date",
            ],
        ),
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.OR,
            criteria_name="US_PA_NOT_ELIGIBLE_OR_MARKED_INELIGIBLE_FOR_ADMIN_SUPERVISION",
            sub_criteria_list=[
                not_eligible_for_admin_supervision.VIEW_BUILDER,
                marked_ineligible_for_admin_supervision.VIEW_BUILDER,
            ],
            allowed_duplicate_reasons_keys=[],
        ),
        supervision_level_is_not_limited.VIEW_BUILDER,
        not_on_sex_offense_protocol.VIEW_BUILDER,
        not_assigned_ineligible_stat_code.VIEW_BUILDER,
        not_supervision_past_full_term_completion_date_or_upcoming_90_days_view_builder,
        supervision_level_is_not_electronic_monitoring.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_special_circumstances_supervision.VIEW_BUILDER,
    almost_eligible_condition=TimeDependentCriteriaCondition(
        criteria=meets_special_circumstances_criteria_for_time_served.VIEW_BUILDER,
        reasons_date_field="eligible_date",
        interval_length=6,
        interval_date_part=BigQueryDateInterval.MONTH,
        description="Within 6 months of meeting the time served requirement",
    ),
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
