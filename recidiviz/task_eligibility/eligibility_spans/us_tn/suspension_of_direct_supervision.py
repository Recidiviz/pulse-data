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
"""Task eligibility spans view that shows the spans of time when someone in TN is
eligible for Suspension of Direct Supervision (SDS).

NB: this is for SDS specifically (the parole version of release from active supervision
in TN) and not for the parallel probation version, Judicial Suspension of Direct
Supervision (JSS).
"""

from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    parole_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_tn import (
    transfer_to_no_contact_parole,
)
from recidiviz.task_eligibility.criteria.general import (
    at_least_12_months_since_most_recent_positive_drug_test,
    has_fines_fees_balance_of_0,
    has_permanent_fines_fees_exemption,
    latest_drug_test_is_negative,
    no_supervision_violation_report_within_2_years,
    on_supervision_at_least_2_years_and_assessed_risk_low_while_on_supervision_at_least_2_years,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    no_arrests_in_past_2_years,
    no_supervision_sanction_within_1_year,
    no_warrant_within_2_years,
    not_in_day_reporting_center,
    not_in_programmed_supervision_unit,
    not_interstate_compact_incoming,
    not_on_community_supervision_for_life,
    not_on_suspension_of_direct_supervision,
)
from recidiviz.task_eligibility.criteria_condition import (
    EligibleCriteriaCondition,
    LessThanOrEqualCriteriaCondition,
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateAgnosticTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

FINES_FEES_CRITERIA_GROUP = StateAgnosticTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.OR,
    criteria_name="HAS_FINES_FEES_BALANCE_OF_0_OR_IS_EXEMPT",
    sub_criteria_list=[
        has_fines_fees_balance_of_0.VIEW_BUILDER,
        has_permanent_fines_fees_exemption.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[],
)

# TODO(#40144): Make SDS opportunity internally consistent with respect to backdating
# (for criteria, eligibility spans, and completion event).
# TODO(#34432): Figure out how to set up the tool for ISC-out cases, which can be
# eligible for SDS. (This may involve updating the candidate population to include
# `SUPERVISION_OUT_OF_STATE` clients and/or changes to criteria.)
VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="SUSPENSION_OF_DIRECT_SUPERVISION",
    description=__doc__,
    candidate_population_view_builder=parole_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        at_least_12_months_since_most_recent_positive_drug_test.VIEW_BUILDER,
        latest_drug_test_is_negative.VIEW_BUILDER,
        no_supervision_violation_report_within_2_years.VIEW_BUILDER,
        # TODO(#33627): Update/change time-served criterion to account for the fact that
        # if individuals are "removed from compliant reporting due to the imposition of
        # a sanction," then that time on compliant reporting does not count towards the
        # two-years-on-supervision requirement for SDS. May need to shift to using a
        # state-specific criterion, then? (If so, check that nobody has starting using
        # the general criterion before deleting it.)
        on_supervision_at_least_2_years_and_assessed_risk_low_while_on_supervision_at_least_2_years.VIEW_BUILDER,
        no_arrests_in_past_2_years.VIEW_BUILDER,
        no_supervision_sanction_within_1_year.VIEW_BUILDER,
        no_warrant_within_2_years.VIEW_BUILDER,
        # TODO(#41397): Check with TN to confirm that we're correctly handling PSU & DRC
        # clients (and time spent in those programs) when determining SDS eligibility.
        not_in_day_reporting_center.VIEW_BUILDER,
        not_in_programmed_supervision_unit.VIEW_BUILDER,
        not_interstate_compact_incoming.VIEW_BUILDER,
        not_on_community_supervision_for_life.VIEW_BUILDER,
        not_on_suspension_of_direct_supervision.VIEW_BUILDER,
        FINES_FEES_CRITERIA_GROUP,
    ],
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            # Pathway 1 for almost-eligibility: someone meets every criterion except for
            # the fines/fees criterion (so they have a fines/fees balance without any
            # permanent exemption).
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    EligibleCriteriaCondition(
                        criteria=on_supervision_at_least_2_years_and_assessed_risk_low_while_on_supervision_at_least_2_years.VIEW_BUILDER,
                        description="On supervision for at least two years, with a 'LOW' assessed risk level for at least two years",
                    ),
                    NotEligibleCriteriaCondition(
                        criteria=FINES_FEES_CRITERIA_GROUP,
                        description="Has unpaid fines/fees without a permanent exemption",
                    ),
                ],
                at_least_n_conditions_true=2,
            ),
            # Pathway 2 for almost-eligibility: someone is within a limited number of
            # days of meeting the accrued-time criteria (and they are missing both or
            # just one of them at the moment), and they either meet the fines/fees
            # criterion already or can have a limited balance.
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    EligibleCriteriaCondition(
                        criteria=on_supervision_at_least_2_years_and_assessed_risk_low_while_on_supervision_at_least_2_years.VIEW_BUILDER,
                        description="On supervision for at least two years, with a 'LOW' assessed risk level for at least two years",
                    ),
                    TimeDependentCriteriaCondition(
                        criteria=on_supervision_at_least_2_years_and_assessed_risk_low_while_on_supervision_at_least_2_years.VIEW_BUILDER,
                        reasons_date_field="combined_eligible_date",
                        interval_length=60,
                        interval_date_part=BigQueryDateInterval.DAY,
                        description="Within 60 days of having been on supervision for two years and at a 'LOW' assessed risk level for two years",
                    ),
                    EligibleCriteriaCondition(
                        criteria=FINES_FEES_CRITERIA_GROUP,
                        description="Has fully paid fines/fees or has a permanent exemption",
                    ),
                    LessThanOrEqualCriteriaCondition(
                        criteria=FINES_FEES_CRITERIA_GROUP,
                        reasons_numerical_field="amount_owed",
                        value=250,
                        description="Fines/fees balance of $250 or less",
                    ),
                ],
                # In the conditions above, a person wouldn't be able to meet both the
                # `EligibleCriteriaCondition` and the other condition for a single
                # criterion, and so each set of conditions related to a single criterion
                # acts as its own category, in which a person cannot meet more than one
                # condition in that category. This means that the below requirement
                # effectively results in a person having to be eligible or almost-
                # eligible within each of the criteria "categories" above in order to
                # hit the minimum number of conditions.
                at_least_n_conditions_true=2,
            ),
        ],
        at_least_n_conditions_true=1,
    ),
    completion_event_builder=transfer_to_no_contact_parole.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
