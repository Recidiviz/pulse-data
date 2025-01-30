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
"""
Shows the spans of time during which someone in UT is eligible
for an Early Termination from Supevision
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.general import early_discharge
from recidiviz.task_eligibility.criteria.general import (
    supervision_or_supervision_out_of_state_past_half_full_term_release_date,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ut import (
    risk_level_reduction_of_one_or_more,
    risk_level_stayed_moderate_or_low,
    risk_score_reduction_5_percent_or_more,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    OrTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_UT,
    task_name="EARLY_TERMINATION_FROM_SUPERVISION_REQUEST",
    description=__doc__,
    candidate_population_view_builder=active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        # Risk reduction criteria
        OrTaskCriteriaGroup(
            criteria_name="US_UT_RISK_REDUCTION_FOR_ET",
            sub_criteria_list=[
                risk_level_reduction_of_one_or_more.VIEW_BUILDER,
                risk_level_stayed_moderate_or_low.VIEW_BUILDER,
                risk_score_reduction_5_percent_or_more.VIEW_BUILDER,
            ],
            allowed_duplicate_reasons_keys=[
                "assessment_level_raw_text",
                "first_assessment_level_raw_text",
            ],
        ),
        # Past ET Review date/ half-time date
        supervision_or_supervision_out_of_state_past_half_full_term_release_date.VIEW_BUILDER,
    ],
    completion_event_builder=early_discharge.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
