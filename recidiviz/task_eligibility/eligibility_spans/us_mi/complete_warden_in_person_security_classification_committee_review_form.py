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
someone in MI is eligible for an in person security committee classification review from the warden
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_mi import (
    warden_in_person_security_classification_committee_review,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mi import (
    expected_number_of_warden_in_person_security_classification_committee_reviews_greater_than_observed,
    in_solitary_confinement_at_least_six_months,
    six_months_past_last_warden_in_person_security_classification_committee_review_date,
)
from recidiviz.task_eligibility.criteria_condition import (
    EligibleCriteriaCondition,
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

_DESCRIPTION = """Shows the spans of time during which someone in MI is eligible for an in person
 security classification committee review from the warden"""

_PAST_WARDEN_IN_PERSON_REVIEW_DATE_CRITERIA_VIEW_BUILDER = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.OR,
    criteria_name="US_MI_PAST_WARDEN_IN_PERSON_REVIEW_FOR_SCC_DATE",
    sub_criteria_list=[
        six_months_past_last_warden_in_person_security_classification_committee_review_date.VIEW_BUILDER,
        expected_number_of_warden_in_person_security_classification_committee_reviews_greater_than_observed.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=["next_scc_date"],
    reasons_aggregate_function_override={"next_scc_date": "MIN"},
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    task_name="COMPLETE_WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM",
    description=_DESCRIPTION,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        _PAST_WARDEN_IN_PERSON_REVIEW_DATE_CRITERIA_VIEW_BUILDER,
        in_solitary_confinement_at_least_six_months.VIEW_BUILDER,
    ],
    completion_event_builder=warden_in_person_security_classification_committee_review.VIEW_BUILDER,
    # Residents are almost eligible for the in person security classification committee review from the warden if
    # they are within 60 days from being fully eligible, which means both the next SCC date AND the six months
    # in solitary date must be at most 60 days away.
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    TimeDependentCriteriaCondition(
                        criteria=_PAST_WARDEN_IN_PERSON_REVIEW_DATE_CRITERIA_VIEW_BUILDER,
                        reasons_date_field="next_scc_date",
                        interval_length=60,
                        interval_date_part=BigQueryDateInterval.DAY,
                        description="Within 60 days of the next warden in person security classification committee"
                        " review due date",
                    ),
                    EligibleCriteriaCondition(
                        criteria=_PAST_WARDEN_IN_PERSON_REVIEW_DATE_CRITERIA_VIEW_BUILDER,
                        description="Past the next warden in person security classification committee review due date",
                    ),
                ],
                at_least_n_conditions_true=1,
            ),
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    TimeDependentCriteriaCondition(
                        criteria=in_solitary_confinement_at_least_six_months.VIEW_BUILDER,
                        reasons_date_field="six_months_in_solitary_date",
                        interval_length=60,
                        interval_date_part=BigQueryDateInterval.DAY,
                        description="Within 60 days of being in solitary confinement at least six months",
                    ),
                    EligibleCriteriaCondition(
                        criteria=in_solitary_confinement_at_least_six_months.VIEW_BUILDER,
                        description="In solitary confinement for at least 6 months",
                    ),
                ],
                at_least_n_conditions_true=1,
            ),
        ],
        at_least_n_conditions_true=2,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
