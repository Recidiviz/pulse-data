# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
someone in MI is eligible for an annual classification review, under the 2026
classification policy.
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population_state_prison_exclude_safekeeping,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_mi import (
    incarceration_assessment_2026_policy_completed,
)
from recidiviz.task_eligibility.criteria.general import (
    has_initial_classification_in_state_prison_custody,
    in_state_prison_custody_at_least_4_months,
    not_has_initial_classification_in_state_prison_custody,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mi import (
    at_least_12_months_since_latest_assessment,
    custody_level_compared_to_recommended_2026_policy,
)
from recidiviz.task_eligibility.criteria_condition import TimeDependentCriteriaCondition
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateAgnosticTaskCriteriaGroupBigQueryViewBuilder,
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# A resident is due for an annual classification review either 12 months after their
# latest assessment in their current state prison custody stay, or, if they have had no
# assessment yet in that stay, once they reach 4 months in custody. The initial
# classification review covers their first 4 months.
US_MI_ANNUAL_CLASSIFICATION_REVIEW_ELIGIBILITY_CRITERIA_VIEW_BUILDER = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.OR,
    criteria_name="US_MI_ANNUAL_CLASSIFICATION_REVIEW_2026_POLICY_ELIGIBILITY",
    sub_criteria_list=[
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.AND,
            criteria_name="US_MI_AT_LEAST_12_MONTHS_SINCE_LATEST_ASSESSMENT_IN_STATE_PRISON_CUSTODY",
            sub_criteria_list=[
                at_least_12_months_since_latest_assessment.VIEW_BUILDER,
                has_initial_classification_in_state_prison_custody.VIEW_BUILDER,
            ],
        ),
        StateAgnosticTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.AND,
            criteria_name="IN_STATE_PRISON_CUSTODY_AT_LEAST_4_MONTHS_WITHOUT_INITIAL_CLASSIFICATION",
            sub_criteria_list=[
                in_state_prison_custody_at_least_4_months.VIEW_BUILDER,
                not_has_initial_classification_in_state_prison_custody.VIEW_BUILDER,
            ],
        ),
    ],
    # Both paths include has_initial_classification_in_state_prison_custody, directly or
    # inverted, so both carry its reasons fields.
    allowed_duplicate_reasons_keys=[
        "initial_assessment_date",
        "initial_classification_date",
        "initial_classification_decision_date",
        "most_recent_prison_custody_start_date",
    ],
)

US_MI_ANNUAL_CLASSIFICATION_REVIEW_CRITERIA_VIEW_BUILDERS: list[
    TaskCriteriaBigQueryViewBuilder
] = [
    US_MI_ANNUAL_CLASSIFICATION_REVIEW_ELIGIBILITY_CRITERIA_VIEW_BUILDER,
    # This criteria is used to add the current and recommended custody levels into the
    # reasons blob for easier access to the fields on the front end, standardizing across
    # all MI classification opportunities, including custody level downgrade. For annual
    # classification review, everyone with a span meets the criteria.
    custody_level_compared_to_recommended_2026_policy.VIEW_BUILDER,
]

# A resident due 12 months after their latest assessment is almost eligible starting 1
# month before their assessment is due, and becomes eligible 2 weeks before their
# assessment is due (see the eligible_date_clause in
# at_least_12_months_since_latest_assessment.VIEW_BUILDER). A resident due at 4 months
# in custody has no assessment_due_date, so they are never almost eligible.
US_MI_ANNUAL_CLASSIFICATION_REVIEW_ALMOST_ELIGIBLE_CONDITION = (
    TimeDependentCriteriaCondition(
        criteria=US_MI_ANNUAL_CLASSIFICATION_REVIEW_ELIGIBILITY_CRITERIA_VIEW_BUILDER,
        reasons_date_field="assessment_due_date",
        interval_length=1,
        interval_date_part=BigQueryDateInterval.MONTH,
        description="Within 1 month of assessment due date",
    )
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    task_name="ANNUAL_CLASSIFICATION_REVIEW_2026_POLICY",
    description=__doc__,
    candidate_population_view_builder=incarceration_population_state_prison_exclude_safekeeping.VIEW_BUILDER,
    criteria_spans_view_builders=US_MI_ANNUAL_CLASSIFICATION_REVIEW_CRITERIA_VIEW_BUILDERS,
    completion_event_builder=incarceration_assessment_2026_policy_completed.VIEW_BUILDER,
    almost_eligible_condition=US_MI_ANNUAL_CLASSIFICATION_REVIEW_ALMOST_ELIGIBLE_CONDITION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
