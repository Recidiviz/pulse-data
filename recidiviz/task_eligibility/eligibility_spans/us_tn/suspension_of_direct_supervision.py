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

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    parole_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_tn import (
    transfer_to_no_contact_parole,
)
from recidiviz.task_eligibility.criteria.general import (
    assessed_risk_low_at_least_2_years,
    at_least_12_months_since_most_recent_positive_drug_test,
    has_fines_fees_balance_of_0,
    has_permanent_fines_fees_exemption,
    latest_drug_test_is_negative,
    no_supervision_violation_report_within_2_years,
    on_supervision_at_least_2_years,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    no_arrests_in_past_2_years,
    no_supervision_sanction_within_1_year,
    no_warrant_within_2_years,
    not_interstate_compact_incoming,
    not_on_community_supervision_for_life,
    special_conditions_are_current,
)
from recidiviz.task_eligibility.criteria_condition import NotEligibleCriteriaCondition
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    OrTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

FINES_FEES_CRITERIA_GROUP = OrTaskCriteriaGroup(
    criteria_name="HAS_FINES_FEES_BALANCE_OF_0_OR_IS_EXEMPT",
    sub_criteria_list=[
        has_fines_fees_balance_of_0.VIEW_BUILDER,
        has_permanent_fines_fees_exemption.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[],
)

# TODO(#34432): Figure out how to set up the tool for ISC-out cases, which can be
# eligible for SDS. (This may involve updating the candidate population to include
# `SUPERVISION_OUT_OF_STATE` clients and/or changes to criteria.)
VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="SUSPENSION_OF_DIRECT_SUPERVISION",
    description=__doc__,
    candidate_population_view_builder=parole_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        assessed_risk_low_at_least_2_years.VIEW_BUILDER,
        at_least_12_months_since_most_recent_positive_drug_test.VIEW_BUILDER,
        latest_drug_test_is_negative.VIEW_BUILDER,
        no_supervision_violation_report_within_2_years.VIEW_BUILDER,
        # TODO(#33627): Update/change time-served criterion to account for the fact that
        # if individuals are "removed from compliant reporting due to the imposition of
        # a sanction," then that time on compliant reporting does not count towards the
        # two-years-on-supervision requirement for SDS. May need to shift to using a
        # state-specific criterion, then? (If so, check that nobody has starting using
        # the general criterion before deleting it.)
        on_supervision_at_least_2_years.VIEW_BUILDER,
        no_arrests_in_past_2_years.VIEW_BUILDER,
        no_supervision_sanction_within_1_year.VIEW_BUILDER,
        no_warrant_within_2_years.VIEW_BUILDER,
        not_interstate_compact_incoming.VIEW_BUILDER,
        not_on_community_supervision_for_life.VIEW_BUILDER,
        # TODO(#33635): Double-check that this existing state-specific criterion is
        # correct for the specific SDS requirement that individuals must have completed
        # and/or complied with all special conditions.
        special_conditions_are_current.VIEW_BUILDER,
        FINES_FEES_CRITERIA_GROUP,
    ],
    # TODO(#33636): Refine this almost-eligible condition, likely by setting an upper
    # limit on the balance a client can have to be considered almost eligible.
    almost_eligible_condition=NotEligibleCriteriaCondition(
        criteria=FINES_FEES_CRITERIA_GROUP,
        description="Has unpaid fines/fees without a permanent exemption",
    ),
    completion_event_builder=transfer_to_no_contact_parole.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
