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
eligible for Compliant Reporting under the policy implemented in 2025, for the Minimum (Low) group.
"""

from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    transfer_to_limited_supervision,
)
from recidiviz.task_eligibility.criteria.general import (
    has_permanent_fines_fees_exemption,
    latest_drug_test_is_negative_or_missing,
    no_supervision_violation_report_within_6_months,
    on_minimum_supervision_at_least_six_months,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    assessed_not_high_on_strong_r_domains,
    most_recent_fee_code_is_feep_in_last_90_days,
    no_arrests_in_past_6_months,
    no_supervision_sanction_within_3_months,
    not_in_day_reporting_center_location,
    not_on_community_supervision_for_life,
    not_serving_ineligible_cr_offense_policy_b,
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
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    OrTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# The new compliant reporting policy has 2 "pathways" to get the opportunity: Group A are people who are on
# Intake (Unassigned) for 60 days and test LOW on Vantage 2.0 and meet criteria set A;
# Group B are people who have been on LOW (new supervision level, mapped to MINIMUM internally)
# for 6+ months and meeting criteria set B. There is some criteria set
# that is applied to both groups. This TES file is for Group B, and TRANSFER_UNASSIGNED_GROUP_TO_COMPLIANT_REPORTING_2025_POLICY
# is for Group A. Both are combined in one opportunity record query.

_FEE_SCHEDULE_OR_PERMANENT_EXEMPTION = OrTaskCriteriaGroup(
    criteria_name="US_TN_FEE_SCHEDULE_OR_PERMANENT_EXEMPTION",
    sub_criteria_list=[
        most_recent_fee_code_is_feep_in_last_90_days.VIEW_BUILDER,
        has_permanent_fines_fees_exemption.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[],
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="TRANSFER_MINIMUM_GROUP_TO_COMPLIANT_REPORTING_2025_POLICY",
    description=__doc__,
    candidate_population_view_builder=probation_parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        on_minimum_supervision_at_least_six_months.VIEW_BUILDER,
        not_serving_ineligible_cr_offense_policy_b.VIEW_BUILDER,
        not_on_community_supervision_for_life.VIEW_BUILDER,
        not_in_day_reporting_center_location.VIEW_BUILDER,
        no_supervision_sanction_within_3_months.VIEW_BUILDER,
        no_arrests_in_past_6_months.VIEW_BUILDER,
        no_supervision_violation_report_within_6_months.VIEW_BUILDER,
        latest_drug_test_is_negative_or_missing.VIEW_BUILDER,
        assessed_not_high_on_strong_r_domains.VIEW_BUILDER,
        _FEE_SCHEDULE_OR_PERMANENT_EXEMPTION,
    ],
    # Clients are almost eligible if they are:
    # (30 days from time requirement OR time requirement is met)
    # AND
    # (missing at least one of [time requirement OR fines & fees OR needs])
    # The inclusion of TimeDependentCriteriaCondition in the second group is meant to catch cases where a person is
    # ONLY missing the time requirement
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    EligibleCriteriaCondition(
                        criteria=on_minimum_supervision_at_least_six_months.VIEW_BUILDER,
                        description="Eligible for supervision level time served",
                    ),
                    TimeDependentCriteriaCondition(
                        criteria=on_minimum_supervision_at_least_six_months.VIEW_BUILDER,
                        reasons_date_field="minimum_time_served_date",
                        interval_length=30,
                        interval_date_part=BigQueryDateInterval.DAY,
                        description="30 days from enough time on minimum",
                    ),
                ],
                at_least_n_conditions_true=1,
            ),
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    NotEligibleCriteriaCondition(
                        criteria=assessed_not_high_on_strong_r_domains.VIEW_BUILDER,
                        description="Don't have 3 Face to Face contacts within 60 days of intake",
                    ),
                    NotEligibleCriteriaCondition(
                        criteria=_FEE_SCHEDULE_OR_PERMANENT_EXEMPTION,
                        description="No FEEP code in last 90 days and no permanent exemption",
                    ),
                    TimeDependentCriteriaCondition(
                        criteria=on_minimum_supervision_at_least_six_months.VIEW_BUILDER,
                        reasons_date_field="minimum_time_served_date",
                        interval_length=30,
                        interval_date_part=BigQueryDateInterval.DAY,
                        description="30 days from enough time on minimum",
                    ),
                ],
                at_least_n_conditions_true=1,
            ),
        ],
        at_least_n_conditions_true=2,
    ),
    completion_event_builder=transfer_to_limited_supervision.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
