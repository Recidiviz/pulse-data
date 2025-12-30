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
eligible for Compliant Reporting under the policy implemented in 2025, for those who are
on the "Low" supervision level (which we map to 'MINIMUM').
"""

# TODO(#40868): Ideally, combine this logic into the eligibility spans for the pre-2025
# version of the CR policy, such that we have a single set of spans (if possible) for
# the old and new versions of the policy.

from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_tn import (
    transfer_to_limited_supervision_2025_policy,
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
    no_ineligible_cr_offense_2025_policy,
    no_supervision_sanction_within_3_months,
    not_in_day_reporting_center,
    not_on_community_supervision_for_life,
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
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# The new Compliant Reporting policy has 2 "pathways" to get the opportunity:
#   - Intake pathway: for people who test Low-Compliant on the STRONG-R 2.0 during
#     intake and meet the set of intake-specific criteria.
#   - "Minimum" pathway: for people who have been on "Low" supervision (mapped to
#     'MINIMUM' internally) for 6+ months and meet the set of criteria for this pathway.
# There is some criteria set that is applied to both groups. This TES file is for the
# minimum pathway, and TRANSFER_INTAKE_GROUP_TO_COMPLIANT_REPORTING_2025_POLICY is for
# the intake pathway. Both are combined in one opportunity record query.

_FEE_SCHEDULE_OR_PERMANENT_EXEMPTION = (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
        logic_type=TaskCriteriaGroupLogicType.OR,
        criteria_name="US_TN_FEE_SCHEDULE_OR_PERMANENT_EXEMPTION",
        sub_criteria_list=[
            most_recent_fee_code_is_feep_in_last_90_days.VIEW_BUILDER,
            has_permanent_fines_fees_exemption.VIEW_BUILDER,
        ],
        allowed_duplicate_reasons_keys=[],
    )
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="TRANSFER_MINIMUM_GROUP_TO_COMPLIANT_REPORTING_2025_POLICY",
    description=__doc__,
    candidate_population_view_builder=probation_parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        on_minimum_supervision_at_least_six_months.VIEW_BUILDER,
        not_on_community_supervision_for_life.VIEW_BUILDER,
        not_in_day_reporting_center.VIEW_BUILDER,
        no_supervision_sanction_within_3_months.VIEW_BUILDER,
        no_arrests_in_past_6_months.VIEW_BUILDER,
        no_ineligible_cr_offense_2025_policy.VIEW_BUILDER,
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
                        description="Assessed high on at least one StrongR domain",
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
    completion_event_builder=transfer_to_limited_supervision_2025_policy.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
