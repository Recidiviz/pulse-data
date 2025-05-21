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
"""Shows the eligibility spans for supervision clients in AZ who are eligible
for a transfer to administrative supervision"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_az import (
    transfer_to_limited_supervision,
)
from recidiviz.task_eligibility.criteria.general import (
    no_supervision_violation_within_15_months,
    on_supervision_at_least_15_months,
    supervision_level_is_not_limited,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az import (
    not_homeless_in_release_plan,
    not_serving_ineligible_offense_for_admin_supervision,
    oras_employed_disabled_retired_or_student,
    oras_has_substance_use_issues,
    oras_risk_level_is_medium_or_lower,
    risk_release_assessment_is_completed,
    risk_release_assessment_level_is_minimum,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateAgnosticTaskCriteriaGroupBigQueryViewBuilder,
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_AZ,
    task_name="TRANSFER_TO_ADMINISTRATIVE_SUPERVISION",
    description=__doc__,
    candidate_population_view_builder=active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        supervision_level_is_not_limited.VIEW_BUILDER,
        # 1.1 ORAS score Medium or Below OR Risk Release Assessment Minimum or Below
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.OR,
            criteria_name="US_AZ_ELIGIBLE_RISK_LEVEL",
            sub_criteria_list=[
                risk_release_assessment_level_is_minimum.VIEW_BUILDER,
                oras_risk_level_is_medium_or_lower.VIEW_BUILDER,
            ],
            allowed_duplicate_reasons_keys=["assessment_score", "assessment_level"],
        ),
        # 1.3 Has completed initial intake and needs assessment
        risk_release_assessment_is_completed.VIEW_BUILDER,
        # 1.4 Not classified as homeless in their home release plan
        # TODO(#41739): Discuss capability/relevancy of alternate eligibility via other part of criteria
        not_homeless_in_release_plan.VIEW_BUILDER,
        # 1.5 Currently employed, retired, or in school, as assessed in ORAS Question 2.4
        # TODO(#41739): Discuss capability/relevancy of alternate eligibility via other part of criteria
        oras_employed_disabled_retired_or_student.VIEW_BUILDER,
        # # TODO(#42752): Re-enable mental health criteria once all assessments are available in prod
        # # 1.6 Mental Health Score of 3 or below.
        # mental_health_score_3_or_below.VIEW_BUILDER,
        # 1.7 Not currently dealing with substance use issues, as assessed in ORAS Question 5.4
        # TODO(#41739): Discuss capability/relevancy of alternate eligibility via other part of criteria
        oras_has_substance_use_issues.VIEW_BUILDER,
        # 1.8 Offenders with ineligible offenses are only eligible if they've been 15 months violation free
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.OR,
            criteria_name="US_AZ_INELIGIBLE_OFFENSES_BUT_15_MONTHS_VIOLATION_FREE",
            sub_criteria_list=[
                not_serving_ineligible_offense_for_admin_supervision.VIEW_BUILDER,
                StateAgnosticTaskCriteriaGroupBigQueryViewBuilder(
                    logic_type=TaskCriteriaGroupLogicType.AND,
                    criteria_name="15_MONTHS_ON_SUPERVISION_VIOLATION_FREE",
                    sub_criteria_list=[
                        no_supervision_violation_within_15_months.VIEW_BUILDER,
                        on_supervision_at_least_15_months.VIEW_BUILDER,
                    ],
                    allowed_duplicate_reasons_keys=[],
                ),
            ],
            allowed_duplicate_reasons_keys=[],
        ),
    ],
    completion_event_builder=transfer_to_limited_supervision.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
