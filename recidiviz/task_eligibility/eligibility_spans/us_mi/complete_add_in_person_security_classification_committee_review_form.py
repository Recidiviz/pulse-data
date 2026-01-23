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
"""Shows the spans of time during which someone in MI is eligible for an in person
 security classification committee review from the ADD"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.state_specific.us_mi import (
    scc_solitary_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_mi import (
    add_in_person_security_classification_committee_review,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mi import (
    in_solitary_confinement_at_least_one_year,
    past_add_in_person_review_for_scc_date,
)
from recidiviz.task_eligibility.criteria_condition import (
    EligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    task_name="COMPLETE_ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM",
    description=__doc__,
    candidate_population_view_builder=scc_solitary_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        past_add_in_person_review_for_scc_date.VIEW_BUILDER,
        in_solitary_confinement_at_least_one_year.VIEW_BUILDER,
    ],
    completion_event_builder=add_in_person_security_classification_committee_review.VIEW_BUILDER,
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    TimeDependentCriteriaCondition(
                        criteria=past_add_in_person_review_for_scc_date.VIEW_BUILDER,
                        reasons_date_field="next_scc_date",
                        interval_length=60,
                        interval_date_part=BigQueryDateInterval.DAY,
                        description="Within 60 days of the next add in person security classification committee review due date",
                    ),
                    EligibleCriteriaCondition(
                        criteria=past_add_in_person_review_for_scc_date.VIEW_BUILDER,
                        description="Past the next add in person security classification committee review due date",
                    ),
                ],
                at_least_n_conditions_true=1,
            ),
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    TimeDependentCriteriaCondition(
                        criteria=in_solitary_confinement_at_least_one_year.VIEW_BUILDER,
                        reasons_date_field="one_year_in_solitary_date",
                        interval_length=60,
                        interval_date_part=BigQueryDateInterval.DAY,
                        description="Within 60 days of being in solitary confinement at least 1 year",
                    ),
                    EligibleCriteriaCondition(
                        criteria=in_solitary_confinement_at_least_one_year.VIEW_BUILDER,
                        description="In solitary confinement for at least 1 year",
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
