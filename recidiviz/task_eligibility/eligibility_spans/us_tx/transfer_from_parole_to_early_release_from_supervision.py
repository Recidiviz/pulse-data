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
"""Shows the spans of time during which someone in TX, who is on parole or dual parole and
probation, is eligible to be transferred to Early Release from Supervision. ERS does not
fully release clients from supervision, but eliminates any required contact. This status
only affects their parole status, not their probation status.
"""
from datetime import date

from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    transfer_to_unsupervised_parole,
)
from recidiviz.task_eligibility.criteria.general import (
    no_supervision_sustained_violation_within_2_years,
    supervision_level_is_minimum_or_limited_for_3_years,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    case_type_eligible_for_ars_ers,
    no_warrant_with_sustained_violation_within_2_years,
    not_supervision_within_6_months_of_release_date,
    served_at_least_half_of_remaining_supervision_sentence,
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
    state_code=StateCode.US_TX,
    task_name="TRANSFER_FROM_PAROLE_TO_EARLY_RELEASE_FROM_SUPERVISION",
    description=__doc__,
    candidate_population_view_builder=parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        supervision_level_is_minimum_or_limited_for_3_years.VIEW_BUILDER,
        served_at_least_half_of_remaining_supervision_sentence.VIEW_BUILDER,
        no_supervision_sustained_violation_within_2_years.VIEW_BUILDER,
        no_warrant_with_sustained_violation_within_2_years.VIEW_BUILDER,
        not_supervision_within_6_months_of_release_date.VIEW_BUILDER,
        case_type_eligible_for_ars_ers.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_unsupervised_parole.VIEW_BUILDER,
    policy_start_date=date(2025, 7, 1),
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            TimeDependentCriteriaCondition(
                criteria=supervision_level_is_minimum_or_limited_for_3_years.VIEW_BUILDER,
                reasons_date_field="minimum_time_served_date",
                interval_length=1,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Within 1 month of being on minimum or limited supervision for 3 years",
            ),
            EligibleCriteriaCondition(
                criteria=supervision_level_is_minimum_or_limited_for_3_years.VIEW_BUILDER,
                description="On minimum or limited supervision for 3 years",
            ),
            TimeDependentCriteriaCondition(
                criteria=served_at_least_half_of_remaining_supervision_sentence.VIEW_BUILDER,
                reasons_date_field="served_at_least_half_of_remaining_supervision_sentence_date",
                interval_length=1,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Within 1 month of having served at least half of remaining supervision sentence",
            ),
            EligibleCriteriaCondition(
                criteria=served_at_least_half_of_remaining_supervision_sentence.VIEW_BUILDER,
                description="Served at least half of remaining supervision sentence",
            ),
            TimeDependentCriteriaCondition(
                criteria=no_supervision_sustained_violation_within_2_years.VIEW_BUILDER,
                reasons_date_field="violation_expiration_date",
                interval_length=1,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Within 1 month of not having any sustained violations in the last two years",
            ),
            EligibleCriteriaCondition(
                criteria=no_supervision_sustained_violation_within_2_years.VIEW_BUILDER,
                description="Has not sustained any violations in the last two years",
            ),
            TimeDependentCriteriaCondition(
                criteria=no_warrant_with_sustained_violation_within_2_years.VIEW_BUILDER,
                reasons_date_field="warrant_expiration_date",
                interval_length=1,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Within 1 month of not having any sustained warrants in the last two years",
            ),
            EligibleCriteriaCondition(
                criteria=no_warrant_with_sustained_violation_within_2_years.VIEW_BUILDER,
                description="Has not sustained any warrants in the last two years",
            ),
        ],
        # In the conditions above, a person wouldn't be able to meet both the
        # `EligibleCriteriaCondition` and the 'TimeDependentCriteriaCondition' for a single
        # criterion, and so each set of conditions related to a single criterion
        # acts as its own category, in which a person cannot meet more than one
        # condition in that category. This means that the below requirement
        # effectively results in a person having to be eligible or almost-
        # eligible within each of the criteria "categories" above in order to
        # hit the minimum number of conditions.
        at_least_n_conditions_true=4,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
