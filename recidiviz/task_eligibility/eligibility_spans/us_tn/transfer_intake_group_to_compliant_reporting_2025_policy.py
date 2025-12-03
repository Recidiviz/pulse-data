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
eligible for Compliant Reporting under the policy implemented in 2025, for those who
test Low-Compliant (on Vantage 2.0) and are eligible for Compliant Reporting following
intake.
"""

# TODO(#40868): Ideally, combine this logic into the eligibility spans for the pre-2025
# version of the CR policy, such that we have a single set of spans (if possible) for
# the old and new versions of the policy.

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_tn import (
    transfer_to_limited_supervision_2025_policy,
)
from recidiviz.task_eligibility.criteria.general import (
    no_positive_drug_screens_since_intake_supervision_level,
    no_supervision_violation_report_since_intake_supervision_level,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    assessed_risk_low_compliant_on_strong_r2_after_intake_supervision_level,
    home_visit_since_intake_supervision_level,
    no_supervision_sanction_since_intake_supervision_level,
    not_in_day_reporting_center,
    not_on_community_supervision_for_life,
    not_serving_ineligible_cr_offense_policy_b,
    three_face_to_face_contacts_within_60_days_of_intake_supervision_start,
)
from recidiviz.task_eligibility.criteria_condition import (
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
)
from recidiviz.task_eligibility.eligibility_spans.us_tn.transfer_minimum_group_to_compliant_reporting_2025_policy import (
    _FEE_SCHEDULE_OR_PERMANENT_EXEMPTION,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# The new compliant reporting policy has 2 "pathways" to get the opportunity: Group A are people who are on
# Intake and test Low-Compliant on Vantage 2.0 and meet criteria set A;
# Group B are people who have been on LOW (new supervision level, mapped to MINIMUM internally)
# for 6+ months and meeting criteria set B. There is some criteria set
# that is applied to both groups. This TES file is for Group A, and TRANSFER_MINIMUM_GROUP_TO_COMPLIANT_REPORTING_2025_POLICY
# is for Group B. Both are combined in one opportunity record query.

# TODO(#38506): Revisit "Minimum after Intake group" (i.e. "in between Groups A and B") after launch of full policy
# This criteria exists as a safeguard for TN's new Compliant Reporting policy. After the new standards are rolled
# out, people should not actually be on Low ever - they should either be moved to Compliant Reporting from Intake
# or moved to Low-Medium. However, there's a chance that with implementation errors/lags, there will be a group of people
# who were moved from Unassigned to Low (new) or Minimum (current), qualify for Compliant Reporting, but don't receive it

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="TRANSFER_INTAKE_GROUP_TO_COMPLIANT_REPORTING_2025_POLICY",
    description=__doc__,
    candidate_population_view_builder=probation_parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        home_visit_since_intake_supervision_level.VIEW_BUILDER,
        assessed_risk_low_compliant_on_strong_r2_after_intake_supervision_level.VIEW_BUILDER,
        no_supervision_sanction_since_intake_supervision_level.VIEW_BUILDER,
        no_supervision_violation_report_since_intake_supervision_level.VIEW_BUILDER,
        no_positive_drug_screens_since_intake_supervision_level.VIEW_BUILDER,
        three_face_to_face_contacts_within_60_days_of_intake_supervision_start.VIEW_BUILDER,
        not_serving_ineligible_cr_offense_policy_b.VIEW_BUILDER,
        not_on_community_supervision_for_life.VIEW_BUILDER,
        not_in_day_reporting_center.VIEW_BUILDER,
        _FEE_SCHEDULE_OR_PERMANENT_EXEMPTION,
    ],
    # Clients are almost eligible if they are: missing at least one of [fines & fees OR
    # face to face contacts].
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            NotEligibleCriteriaCondition(
                criteria=three_face_to_face_contacts_within_60_days_of_intake_supervision_start.VIEW_BUILDER,
                description="Don't have 3 Face to Face contacts within 60 days of intake",
            ),
            NotEligibleCriteriaCondition(
                criteria=_FEE_SCHEDULE_OR_PERMANENT_EXEMPTION,
                description="No FEEP code in last 90 days and no permanent exemption",
            ),
        ],
        at_least_n_conditions_true=1,
    ),
    completion_event_builder=transfer_to_limited_supervision_2025_policy.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
