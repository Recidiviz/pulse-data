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
"""Shows the eligibility spans for residents in AZ who are eligible for a Drug Transition 
Program (DTP) release according to our (Recidiviz) calculations. 
"""

from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_az import (
    early_release_to_drug_program_not_overdue,
)
from recidiviz.task_eligibility.criteria.general import (
    custody_level_is_minimum_or_medium,
    no_nonviolent_incarceration_violation_within_6_months,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az import (
    acis_dtp_date_not_set,
    acis_tpr_date_not_set,
    enrolled_in_functional_literacy,
    is_us_citizen_or_legal_permanent_resident,
    meets_functional_literacy_dtp,
    no_active_felony_detainers,
    no_dtp_denial_or_previous_dtp_release,
    no_dtp_removals_from_self_improvement_programs,
    no_ineligible_dtp_offense_convictions,
    no_major_violent_violation_during_incarceration,
    no_unsatisfactory_program_ratings_within_3_months,
    not_serving_flat_sentence,
    only_drug_offense_convictions,
    within_7_days_of_recidiviz_dtp_date,
)
from recidiviz.task_eligibility.criteria_condition import (
    EligibleCriteriaCondition,
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Criteria shared in both TPR and DTP
COMMON_CRITERIA_ACROSS_TPR_AND_DTP: list[TaskCriteriaBigQueryViewBuilder] = [
    no_active_felony_detainers.VIEW_BUILDER,
    custody_level_is_minimum_or_medium.VIEW_BUILDER,
    no_unsatisfactory_program_ratings_within_3_months.VIEW_BUILDER,
    not_serving_flat_sentence.VIEW_BUILDER,
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
        logic_type=TaskCriteriaGroupLogicType.AND,
        criteria_name="US_AZ_NO_VIOLATIONS_AND_ELIGIBLE_LEGAL_STATUS",
        sub_criteria_list=[
            no_nonviolent_incarceration_violation_within_6_months.VIEW_BUILDER,
            no_major_violent_violation_during_incarceration.VIEW_BUILDER,
            is_us_citizen_or_legal_permanent_resident.VIEW_BUILDER,
        ],
        allowed_duplicate_reasons_keys=[],
    ),
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
        logic_type=TaskCriteriaGroupLogicType.AND,
        criteria_name="US_AZ_NO_ACIS_DTP_OR_TPR_DATE_SET",
        sub_criteria_list=[
            acis_dtp_date_not_set.VIEW_BUILDER,
            acis_tpr_date_not_set.VIEW_BUILDER,
        ],
        allowed_duplicate_reasons_keys=[],
    ),
]

_FUNCTIONAL_LITERACY_CRITERIA = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.OR,
    criteria_name="US_AZ_ENROLLED_IN_OR_MEETS_MANDATORY_LITERACY",
    sub_criteria_list=[
        meets_functional_literacy_dtp.VIEW_BUILDER,
        enrolled_in_functional_literacy.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[],
)


VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_AZ,
    task_name="OVERDUE_FOR_RECIDIVIZ_DTP_REQUEST",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        ### Criteria shared in both TPR and DTP
        *COMMON_CRITERIA_ACROSS_TPR_AND_DTP,
        ### DTP-specific criteria
        # a. Functional literacy
        _FUNCTIONAL_LITERACY_CRITERIA,
        # b. Offenses
        only_drug_offense_convictions.VIEW_BUILDER,
        no_ineligible_dtp_offense_convictions.VIEW_BUILDER,
        # c. No DTP denials in current incarceration, DTPs in the past
        no_dtp_denial_or_previous_dtp_release.VIEW_BUILDER,
        # d. Self improvement programs
        no_dtp_removals_from_self_improvement_programs.VIEW_BUILDER,
        # e. Time
        within_7_days_of_recidiviz_dtp_date.VIEW_BUILDER,
    ],
    completion_event_builder=early_release_to_drug_program_not_overdue.VIEW_BUILDER,
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            # Only missing mandatory literacy
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    NotEligibleCriteriaCondition(
                        criteria=_FUNCTIONAL_LITERACY_CRITERIA,
                        description="Missing Mandatory Literacy criteria",
                    ),
                    EligibleCriteriaCondition(
                        criteria=within_7_days_of_recidiviz_dtp_date.VIEW_BUILDER,
                        description="Within 7 days of projected TPR eligibility date",
                    ),
                    EligibleCriteriaCondition(
                        criteria=no_active_felony_detainers.VIEW_BUILDER,
                        description="No active felony detainers",
                    ),
                ],
                at_least_n_conditions_true=3,
            ),
            # Projected DTP Date Between 7 days - 12 Months AND (missing mandatory literacy XOR felony detainers)
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    # TODO(#42817): update this condition to "date within 6-12 months"
                    TimeDependentCriteriaCondition(
                        criteria=within_7_days_of_recidiviz_dtp_date.VIEW_BUILDER,
                        reasons_date_field="recidiviz_dtp_date",
                        interval_length=12,
                        interval_date_part=BigQueryDateInterval.MONTH,
                        description="Within 7 days to 12 months from Recidiviz projected DTP date",
                    ),
                    # Felony Detainer XOR Mandatory Literacy (only 1 met)
                    PickNCompositeCriteriaCondition(
                        sub_conditions_list=[
                            NotEligibleCriteriaCondition(
                                criteria=no_active_felony_detainers.VIEW_BUILDER,
                                description="Missing Felony Detainer criteria",
                            ),
                            NotEligibleCriteriaCondition(
                                criteria=_FUNCTIONAL_LITERACY_CRITERIA,
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
                                description="Has Felony Detainer criteria",
                            ),
                            EligibleCriteriaCondition(
                                criteria=_FUNCTIONAL_LITERACY_CRITERIA,
                                description="Has Mandatory Literacy criteria",
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
