# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
for a transfer to administrative supervision. This version of TES only applies eligibility to those with a completed
ORAS."""
from datetime import date

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    active_supervision_population_not_limited_level,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_az import (
    transfer_to_limited_supervision,
)
from recidiviz.task_eligibility.criteria.general import (
    oras_community_supervision_completed,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az import (
    mental_health_score_3_or_below,
    no_ineligible_offense_conviction_for_admin_supervision,
    not_in_halfway_house_or_new_freedom,
    not_serving_expanded_ineligible_offense_for_admin_supervision,
    not_severely_mentally_ill,
    oras_employed_disabled_retired_or_student,
    oras_has_substance_use_issues,
    oras_risk_level_is_medium_or_lower,
    risk_release_assessment_is_completed,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_MEET_INELIGIBLE_OFFENSE_CRITERIA = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name="US_AZ_NO_INELIGIBLE_CURRENT_OR_PRIOR_OFFENSE",
    sub_criteria_list=[
        no_ineligible_offense_conviction_for_admin_supervision.VIEW_BUILDER,
        not_serving_expanded_ineligible_offense_for_admin_supervision.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[],
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_AZ,
    task_name="TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_V2",
    description=__doc__,
    candidate_population_view_builder=active_supervision_population_not_limited_level.VIEW_BUILDER,
    criteria_spans_view_builders=[
        # 1.1 Risk and needs assessment shows a risk determination of moderate or lower
        oras_risk_level_is_medium_or_lower.VIEW_BUILDER,
        # 1.2 No current or prior convictions of a registerable sex offense or felony
        # domestic violence offense, or current convictions of felony arson or murder
        _MEET_INELIGIBLE_OFFENSE_CRITERIA,
        # 1.3 Has completed initial intake and needs assessment
        risk_release_assessment_is_completed.VIEW_BUILDER,
        # 1.5 Currently employed, retired, or in school, as assessed in ORAS Question 2.4
        oras_employed_disabled_retired_or_student.VIEW_BUILDER,
        # 1.6 Mental Health Score of 3 or below.
        mental_health_score_3_or_below.VIEW_BUILDER,
        # 1.7 Not SMI-C
        not_severely_mentally_ill.VIEW_BUILDER,
        # 1.8 Not currently dealing with substance use issues, as assessed in ORAS Question 5.4
        oras_has_substance_use_issues.VIEW_BUILDER,
        # Not in a Halfway House or New Freedom (in service of 1.4)
        not_in_halfway_house_or_new_freedom.VIEW_BUILDER,
        # Internal Criteria, created solely for configuring Maybe Eligible accurately
        oras_community_supervision_completed.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_limited_supervision.VIEW_BUILDER,
    policy_start_date=date(2026, 6, 27),
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
