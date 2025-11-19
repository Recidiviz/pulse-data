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
"""Shows the spans of time during which someone in NE is eligible
for good time restoration.
"""
from typing import List

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import good_time_reinstated
from recidiviz.task_eligibility.criteria.general import (
    housing_unit_type_is_solitary_confinement,
    incarcerated_in_state_prison_at_least_1_year,
    no_highest_severity_incarceration_sanctions_within_1_year,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ne import (
    at_least_2_weeks_since_last_good_time_restoration,
    has_lost_restorable_good_time,
    less_than_3_udc_mrs_in_past_6_months,
    no_idc_mrs_in_past_6_months,
    not_in_custody_level_1a,
    over_4_months_from_trd,
)
from recidiviz.task_eligibility.criteria_condition import (
    BigQueryDateInterval,
    PickNCompositeCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

not_in_restrictive_housing_view_builder = (
    StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
        sub_criteria=housing_unit_type_is_solitary_confinement.VIEW_BUILDER
    )
)

US_NE_GOOD_TIME_RESTORATION_30_DAYS_CRITERIA_BUILDERS: List[
    TaskCriteriaBigQueryViewBuilder
] = [
    has_lost_restorable_good_time.VIEW_BUILDER,
    not_in_custody_level_1a.VIEW_BUILDER,
    not_in_restrictive_housing_view_builder,
    # TODO(#53595): Make sure we're accounting for all NDCS incarceration periods, in
    # case some are missing information on custodial authority.
    incarcerated_in_state_prison_at_least_1_year.VIEW_BUILDER,
    no_highest_severity_incarceration_sanctions_within_1_year.VIEW_BUILDER,
    no_idc_mrs_in_past_6_months.VIEW_BUILDER,
    less_than_3_udc_mrs_in_past_6_months.VIEW_BUILDER,
    over_4_months_from_trd.VIEW_BUILDER,
    at_least_2_weeks_since_last_good_time_restoration.VIEW_BUILDER,
]

_ALMOST_ELIGIBLE_MONTHS = 3

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_NE,
    task_name="GOOD_TIME_RESTORATION_30_DAYS",
    description=__doc__,
    candidate_population_view_builder=incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=US_NE_GOOD_TIME_RESTORATION_30_DAYS_CRITERIA_BUILDERS,
    completion_event_builder=good_time_reinstated.VIEW_BUILDER,
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            TimeDependentCriteriaCondition(
                criteria=no_highest_severity_incarceration_sanctions_within_1_year.VIEW_BUILDER,
                # Select the most recent incident date
                reasons_date_field="latest_event_date",
                interval_length=_ALMOST_ELIGIBLE_MONTHS,
                interval_date_part=BigQueryDateInterval.MONTH,
                description=f"Less than {_ALMOST_ELIGIBLE_MONTHS} months until this Class I MR is no longer disqualifying.",
            ),
            TimeDependentCriteriaCondition(
                criteria=no_idc_mrs_in_past_6_months.VIEW_BUILDER,
                # Select the most recent incident date
                reasons_date_field="latest_incident_date",
                interval_length=_ALMOST_ELIGIBLE_MONTHS,
                interval_date_part=BigQueryDateInterval.MONTH,
                description=f"Less than {_ALMOST_ELIGIBLE_MONTHS} months until this MR which went to the IDC is no longer disqualifying.",
            ),
        ],
        at_least_n_conditions_true=2,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
