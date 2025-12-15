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
"""Shows the spans of time during which someone in ND is eligible
for a transfer into a minimum security facility.
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_nd import (
    transfer_to_minimum_facility,
)
from recidiviz.task_eligibility.criteria.general import (
    custody_level_is_minimum,
    housing_unit_type_is_solitary_confinement,
    incarceration_within_3_months_of_full_term_completion_date,
    incarceration_within_42_months_of_full_term_completion_date,
    no_escape_in_current_incarceration,
    not_in_work_release,
)
from recidiviz.task_eligibility.criteria.state_specific.us_nd import (
    no_detainers_or_felony_warrants,
    not_enrolled_in_relevant_program,
    not_in_an_orientation_unit,
    not_in_minimum_security_facility,
    not_in_wtru_btc,
)
from recidiviz.task_eligibility.criteria_condition import (
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_NOT_WITHIN_3_MONTHS_OF_FTCD = StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
    sub_criteria=incarceration_within_3_months_of_full_term_completion_date.VIEW_BUILDER,
)
HOUSING_UNIT_TYPE_IS_NOT_SOLITARY_CONFINEMENT = (
    StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
        sub_criteria=housing_unit_type_is_solitary_confinement.VIEW_BUILDER,
    )
)


VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_ND,
    task_name="TRANSFER_TO_MINIMUM_FACILITY_FORM",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        custody_level_is_minimum.VIEW_BUILDER,
        not_in_minimum_security_facility.VIEW_BUILDER,
        not_in_work_release.VIEW_BUILDER,
        not_in_an_orientation_unit.VIEW_BUILDER,
        not_in_wtru_btc.VIEW_BUILDER,
        incarceration_within_42_months_of_full_term_completion_date.VIEW_BUILDER,
        INCARCERATION_NOT_WITHIN_3_MONTHS_OF_FTCD,
        not_enrolled_in_relevant_program.VIEW_BUILDER,
        no_escape_in_current_incarceration.VIEW_BUILDER,
        HOUSING_UNIT_TYPE_IS_NOT_SOLITARY_CONFINEMENT,
        no_detainers_or_felony_warrants.VIEW_BUILDER,
    ],
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            TimeDependentCriteriaCondition(
                criteria=incarceration_within_42_months_of_full_term_completion_date.VIEW_BUILDER,
                reasons_date_field="full_term_completion_date",
                interval_length=43,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Within 43 months of full-term completion date",
            ),
            TimeDependentCriteriaCondition(
                criteria=not_in_an_orientation_unit.VIEW_BUILDER,
                reasons_date_field="housing_unit_start_date",
                interval_length=-2,
                interval_date_part=BigQueryDateInterval.WEEK,
                description="Currently in an orientation unit, less than a week from reaching the 3-week mark",
            ),
            NotEligibleCriteriaCondition(
                criteria=not_enrolled_in_relevant_program.VIEW_BUILDER,
                description="Enrolled in a relevant program",
            ),
        ],
        at_most_n_conditions_true=1,
    ),
    completion_event_builder=transfer_to_minimum_facility.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
