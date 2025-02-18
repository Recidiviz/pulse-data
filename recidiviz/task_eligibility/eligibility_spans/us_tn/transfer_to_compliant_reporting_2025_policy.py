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
eligible for Compliant Reporting under the policy implemented in 2025.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_tn import (
    transfer_to_no_contact_parole,
)
from recidiviz.task_eligibility.criteria.general import (
    assessed_not_high_on_strong_r_domains,
    assessed_risk_low_after_intake,
    latest_drug_test_is_negative_or_missing,
    no_positive_drug_screens_since_starting_supervision,
    no_supervision_sanction_since_starting_supervision,
    no_supervision_violation_report_since_starting_supervision,
    no_supervision_violation_report_within_6_months,
    supervision_level_is_minimum,
    supervision_level_low_after_intake,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    fee_schedule_in_place,
    home_visit_since_intake,
    no_arrests_in_past_6_months,
    no_supervision_sanction_within_3_months,
    not_on_community_supervision_for_life,
    not_serving_ineligible_cr_offense_policy_b,
    three_face_contacts_within_60_days,
)
from recidiviz.task_eligibility.criteria_condition import (
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    InvertedTaskCriteriaBigQueryViewBuilder,
    OrTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_LEVEL_NOT_LOW_AFTER_INTAKE = InvertedTaskCriteriaBigQueryViewBuilder(
    sub_criteria=supervision_level_low_after_intake.VIEW_BUILDER,
)

SUPERVISION_LEVEL_NOT_MINIMUM = InvertedTaskCriteriaBigQueryViewBuilder(
    sub_criteria=supervision_level_is_minimum.VIEW_BUILDER,
)

_ASSESSED_HIGH_CRITERIA = OrTaskCriteriaGroup(
    criteria_name="US_TN_NO_STRONG_R_HIGH_NEEDS",
    sub_criteria_list=[
        assessed_not_high_on_strong_r_domains.VIEW_BUILDER,
        SUPERVISION_LEVEL_NOT_MINIMUM,
    ],
    allowed_duplicate_reasons_keys=[],
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY",
    description=__doc__,
    candidate_population_view_builder=probation_parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        # Every one eligible is either Low after intake or Low-Moderate
        OrTaskCriteriaGroup(
            criteria_name="US_TN_SUPERVISION_LEVEL_IS_LOW_OR_MINIMUM",
            sub_criteria_list=[
                # TODO(#37898): update to supervision_level_is_low_moderate_for_6_months
                #  when that is rolled out and ingested
                supervision_level_is_minimum.VIEW_BUILDER,
                supervision_level_low_after_intake.VIEW_BUILDER,
            ],
            allowed_duplicate_reasons_keys=[
                "supervision_level",
                "supervision_level_start_date",
            ],
        ),
        # Criteria that applies to either "pathway" to eligibility low after intake or low-moderate
        fee_schedule_in_place.VIEW_BUILDER,
        not_serving_ineligible_cr_offense_policy_b.VIEW_BUILDER,
        not_on_community_supervision_for_life.VIEW_BUILDER,
        # Criteria for people who tested Low at Intake
        OrTaskCriteriaGroup(
            criteria_name="US_TN_FACE_TO_FACE_CONTACTS_SINCE_INTAKE",
            sub_criteria_list=[
                three_face_contacts_within_60_days.VIEW_BUILDER,
                SUPERVISION_LEVEL_NOT_LOW_AFTER_INTAKE,
            ],
            allowed_duplicate_reasons_keys=[],
        ),
        OrTaskCriteriaGroup(
            criteria_name="US_TN_HOME_VISIT_SINCE_SUPERVISION_INTAKE",
            sub_criteria_list=[
                home_visit_since_intake.VIEW_BUILDER,
                SUPERVISION_LEVEL_NOT_LOW_AFTER_INTAKE,
            ],
            allowed_duplicate_reasons_keys=[],
        ),
        OrTaskCriteriaGroup(
            criteria_name="US_TN_ASSESSED_LOW_AFTER_INTAKE",
            sub_criteria_list=[
                assessed_risk_low_after_intake.VIEW_BUILDER,
                SUPERVISION_LEVEL_NOT_LOW_AFTER_INTAKE,
            ],
            allowed_duplicate_reasons_keys=[],
        ),
        OrTaskCriteriaGroup(
            criteria_name="US_TN_NO_SANCTION_SINCE_INTAKE",
            sub_criteria_list=[
                no_supervision_sanction_since_starting_supervision.VIEW_BUILDER,
                SUPERVISION_LEVEL_NOT_LOW_AFTER_INTAKE,
            ],
            allowed_duplicate_reasons_keys=[],
        ),
        OrTaskCriteriaGroup(
            criteria_name="US_TN_NO_VIOLATION_SINCE_INTAKE",
            sub_criteria_list=[
                no_supervision_violation_report_since_starting_supervision.VIEW_BUILDER,
                SUPERVISION_LEVEL_NOT_LOW_AFTER_INTAKE,
            ],
            allowed_duplicate_reasons_keys=[],
        ),
        OrTaskCriteriaGroup(
            criteria_name="US_TN_NO_POSITIVE_DRUG_SCREENS_SINCE_INTAKE",
            sub_criteria_list=[
                no_positive_drug_screens_since_starting_supervision.VIEW_BUILDER,
                SUPERVISION_LEVEL_NOT_LOW_AFTER_INTAKE,
            ],
            allowed_duplicate_reasons_keys=[],
        ),
        # Criteria for people on Low-Moderate for 6 months - currently "minimum"
        # TODO(#37898): As of January 2025, TN has not rolled out the new supervision levels which will
        # be needed for the new compliant reporting policy. At the moment they're applying the new
        # criteria to everyone on Minimum. Once we have new levels and they are ingested, this criteria
        # will need to be updated to supervision_level_is_low_moderate_for_6_months
        OrTaskCriteriaGroup(
            criteria_name="US_TN_SANCTIONS_PAST_3_MONTHS",
            sub_criteria_list=[
                no_supervision_sanction_within_3_months.VIEW_BUILDER,
                SUPERVISION_LEVEL_NOT_MINIMUM,
            ],
            allowed_duplicate_reasons_keys=[],
        ),
        OrTaskCriteriaGroup(
            criteria_name="US_TN_NO_ARRESTS_PAST_6_MONTHS",
            sub_criteria_list=[
                no_arrests_in_past_6_months.VIEW_BUILDER,
                SUPERVISION_LEVEL_NOT_MINIMUM,
            ],
            allowed_duplicate_reasons_keys=[],
        ),
        OrTaskCriteriaGroup(
            criteria_name="US_TN_NO_VIOLATIONS_PAST_6_MONTHS",
            sub_criteria_list=[
                no_supervision_violation_report_within_6_months.VIEW_BUILDER,
                SUPERVISION_LEVEL_NOT_MINIMUM,
            ],
            allowed_duplicate_reasons_keys=[],
        ),
        OrTaskCriteriaGroup(
            criteria_name="US_TN_LATEST_DRUG_TEST_NEGATIVE_OR_MISSING",
            sub_criteria_list=[
                latest_drug_test_is_negative_or_missing.VIEW_BUILDER,
                SUPERVISION_LEVEL_NOT_MINIMUM,
            ],
            allowed_duplicate_reasons_keys=[],
        ),
        _ASSESSED_HIGH_CRITERIA,
    ],
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            NotEligibleCriteriaCondition(
                criteria=_ASSESSED_HIGH_CRITERIA,
                description="Assessed high on at least one StrongR domain",
            ),
            NotEligibleCriteriaCondition(
                criteria=fee_schedule_in_place.VIEW_BUILDER,
                description="No FEEP code in last 90 days",
            ),
        ],
        at_most_n_conditions_true=2,
    ),
    # TODO(#37912) - update this once backdating PR merged in
    completion_event_builder=transfer_to_no_contact_parole.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
