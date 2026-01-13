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
"""
Shows the spans of time during which someone in ND is eligible
for the Adult Training Program (ATP).
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import granted_work_release
from recidiviz.task_eligibility.criteria.general import (
    custody_level_is_minimum,
    incarcerated_at_least_30_days_in_same_facility,
    incarcerated_at_least_90_days,
    no_escape_in_current_incarceration,
    not_in_work_release,
)
from recidiviz.task_eligibility.criteria.state_specific.us_nd import (
    has_facility_restrictions,
    incarceration_within_1_year_of_ftcd_or_prd_or_cpp_release,
    no_detainers_or_warrants,
    not_enrolled_in_relevant_program,
    not_serving_ineligible_offense_for_atp_work_release,
    not_within_1_month_of_parole_start_date,
    work_release_committee_requirements,
)
from recidiviz.task_eligibility.criteria_condition import (
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.eligibility_spans.us_nd.transfer_to_minimum_facility_form import (
    HOUSING_UNIT_TYPE_IS_NOT_SOLITARY_CONFINEMENT,
    INCARCERATION_NOT_WITHIN_3_MONTHS_OF_FTCD,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_ND,
    task_name="TRANSFER_TO_ATP_FORM",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        custody_level_is_minimum.VIEW_BUILDER,
        incarceration_within_1_year_of_ftcd_or_prd_or_cpp_release.VIEW_BUILDER,
        work_release_committee_requirements.VIEW_BUILDER,
        not_in_work_release.VIEW_BUILDER,
        not_serving_ineligible_offense_for_atp_work_release.VIEW_BUILDER,
        incarcerated_at_least_90_days.VIEW_BUILDER,
        incarcerated_at_least_30_days_in_same_facility.VIEW_BUILDER,
        no_detainers_or_warrants.VIEW_BUILDER,
        INCARCERATION_NOT_WITHIN_3_MONTHS_OF_FTCD,
        no_escape_in_current_incarceration.VIEW_BUILDER,
        not_enrolled_in_relevant_program.VIEW_BUILDER,
        has_facility_restrictions.VIEW_BUILDER,
        not_within_1_month_of_parole_start_date.VIEW_BUILDER,
        HOUSING_UNIT_TYPE_IS_NOT_SOLITARY_CONFINEMENT,
    ],
    completion_event_builder=granted_work_release.VIEW_BUILDER,
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            # Almost eligible if:
            # (3 months within 1 year of FTCD OR 3 months within 1 year of PRD)
            #   XOR missing 30 days in same facility
            #   XOR missing 90 days incarcerated
            #   XOR missing not enrolled in relevant program
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    TimeDependentCriteriaCondition(
                        criteria=incarceration_within_1_year_of_ftcd_or_prd_or_cpp_release.VIEW_BUILDER,
                        reasons_date_field="full_term_completion_date",
                        interval_length=15,  # 12 months + 3 months
                        interval_date_part=BigQueryDateInterval.MONTH,
                        description="Within 3 months from eligibility according to the full term completion date",
                    ),
                    TimeDependentCriteriaCondition(
                        criteria=incarceration_within_1_year_of_ftcd_or_prd_or_cpp_release.VIEW_BUILDER,
                        reasons_date_field="parole_review_date",
                        interval_length=15,  # 12 months + 3 months
                        interval_date_part=BigQueryDateInterval.MONTH,
                        description="Within 3 months away from eligibility according to the parole review date",
                    ),
                ],
                at_least_n_conditions_true=1,
            ),
            NotEligibleCriteriaCondition(
                criteria=incarcerated_at_least_30_days_in_same_facility.VIEW_BUILDER,
                description="Only missing the 30 days in the same facility criteria",
            ),
            NotEligibleCriteriaCondition(
                criteria=incarcerated_at_least_90_days.VIEW_BUILDER,
                description="Only missing the 90 days in the NDDCR criteria",
            ),
            NotEligibleCriteriaCondition(
                criteria=not_enrolled_in_relevant_program.VIEW_BUILDER,
                description="Only missing the not enrolled in relevant program criteria",
            ),
        ],
        at_most_n_conditions_true=1,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
