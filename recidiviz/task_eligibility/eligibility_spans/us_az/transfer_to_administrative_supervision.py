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
    mental_health_score_3_or_below,
    not_serving_ineligible_offense_for_admin_supervision,
    risk_release_assessment_level_is_minimum,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    AndTaskCriteriaGroup,
    OrTaskCriteriaGroup,
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
        # 1.1 ORAS score or Risk Release Assessment
        risk_release_assessment_level_is_minimum.VIEW_BUILDER,
        # 1.6 Mental Health Score of 3 or below.
        mental_health_score_3_or_below.VIEW_BUILDER,
        # 1.8 Offenders with ineligible offenses are only eligible if they've been 15 months violation free
        OrTaskCriteriaGroup(
            criteria_name="US_AZ_INELIGIBLE_OFFENSES_BUT_15_MONTHS_VIOLATION_FREE",
            sub_criteria_list=[
                not_serving_ineligible_offense_for_admin_supervision.VIEW_BUILDER,
                AndTaskCriteriaGroup(
                    criteria_name="US_AZ_15_MONTHS_ON_SUPERVISION_VIOLATION_FREE",
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
