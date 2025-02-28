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
"""Shows the eligibility spans for residents in AZ who are eligible for a Transition 
Program Release (TPR) release according to our (Recidiviz) calculations. If someone is also
eligible or almost eligible for a Drug Transition Program (DTP) release, they will not be 
considered eligible or almost eligible for this opportunity.  
"""

from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_az import (
    early_release_to_community_confinement_supervision_not_overdue,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az import (
    at_least_24_months_since_last_csed,
    eligible_or_almost_eligible_for_overdue_for_recidiviz_dtp,
    meets_functional_literacy_tpr,
    no_active_felony_detainers,
    no_ineligible_tpr_offense_convictions,
    no_tpr_denial_in_current_incarceration,
    no_tpr_removals_from_self_improvement_programs,
    no_transition_release_in_current_sentence_span,
    within_6_months_of_recidiviz_tpr_date,
)
from recidiviz.task_eligibility.criteria_condition import (
    EligibleCriteriaCondition,
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.eligibility_spans.us_az.overdue_for_recidiviz_dtp_request import (
    COMMON_CRITERIA_ACROSS_TPR_AND_DTP,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    AndTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_AZ,
    task_name="OVERDUE_FOR_RECIDIVIZ_TPR_REQUEST",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        ### Criteria shared in both TPR and DTP
        *COMMON_CRITERIA_ACROSS_TPR_AND_DTP,  # type: ignore
        ### TPR-specific criteria
        # a. Functional literacy
        meets_functional_literacy_tpr.VIEW_BUILDER,
        # b. Offenses
        no_ineligible_tpr_offense_convictions.VIEW_BUILDER,
        # c. No TPR denials in current incarceration, TPRs on current sentence and no ACIS TPR date
        AndTaskCriteriaGroup(
            criteria_name="US_AZ_NO_TPR_DENIAL_OR_RELEASE_IN_CURRENT_INCARCERATION",
            sub_criteria_list=[
                no_tpr_denial_in_current_incarceration.VIEW_BUILDER,
                no_transition_release_in_current_sentence_span.VIEW_BUILDER,
            ],
            allowed_duplicate_reasons_keys=[],
        ),
        # d. Self improvement programs
        no_tpr_removals_from_self_improvement_programs.VIEW_BUILDER,
        # e. Time
        at_least_24_months_since_last_csed.VIEW_BUILDER,
        within_6_months_of_recidiviz_tpr_date.VIEW_BUILDER,
        # f. Not already eligible for overdue_for_recidiviz_dtp
        eligible_or_almost_eligible_for_overdue_for_recidiviz_dtp.VIEW_BUILDER,
    ],
    completion_event_builder=early_release_to_community_confinement_supervision_not_overdue.VIEW_BUILDER,
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            # Only missing mandatory literacy
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    NotEligibleCriteriaCondition(
                        criteria=meets_functional_literacy_tpr.VIEW_BUILDER,
                        description="Missing Mandatory Literacy criteria",
                    ),
                    EligibleCriteriaCondition(
                        criteria=within_6_months_of_recidiviz_tpr_date.VIEW_BUILDER,
                        description="Within 6 months of projected TPR eligibility date",
                    ),
                    EligibleCriteriaCondition(
                        criteria=no_active_felony_detainers.VIEW_BUILDER,
                        description="No active felony detainers",
                    ),
                ],
                at_least_n_conditions_true=3,
            ),
            # Projected TPR Date Between 6 - 12 Months AND (missing mandatory literacy XOR felony detainers)
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    TimeDependentCriteriaCondition(
                        criteria=within_6_months_of_recidiviz_tpr_date.VIEW_BUILDER,
                        reasons_date_field="recidiviz_tpr_date",
                        interval_length=12,
                        interval_date_part=BigQueryDateInterval.MONTH,
                        description="Within 6-12 months from Recidiviz projected TPR date",
                    ),
                    # Felony Detainer XOR Mandatory Literacy (only 1 met)
                    PickNCompositeCriteriaCondition(
                        sub_conditions_list=[
                            NotEligibleCriteriaCondition(
                                criteria=no_active_felony_detainers.VIEW_BUILDER,
                                description="Missing Felony Detainer criteria",
                            ),
                            NotEligibleCriteriaCondition(
                                criteria=meets_functional_literacy_tpr.VIEW_BUILDER,
                                description="Missing Mandatory Literacy criteria",
                            ),
                        ],
                        at_most_n_conditions_true=1,
                    ),
                    # Felony Detainer AND Mandatory Literacy are both met
                    PickNCompositeCriteriaCondition(
                        sub_conditions_list=[
                            EligibleCriteriaCondition(
                                criteria=no_active_felony_detainers.VIEW_BUILDER,
                                description="Missing Felony Detainer criteria",
                            ),
                            EligibleCriteriaCondition(
                                criteria=meets_functional_literacy_tpr.VIEW_BUILDER,
                                description="Missing Mandatory Literacy criteria",
                            ),
                        ],
                        at_least_n_conditions_true=2,
                    ),
                ],
                at_least_n_conditions_true=2,
            ),
        ],
        at_least_n_conditions_true=1,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
