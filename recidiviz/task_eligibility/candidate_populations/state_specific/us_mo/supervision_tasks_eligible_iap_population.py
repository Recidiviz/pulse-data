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
"""Identifies when a client is a candidate for supervision Tasks in MO under routine
contact standards applying only to clients in the Initial Assessment Period (IAP).
"""

from recidiviz.task_eligibility.candidate_populations.state_specific.us_mo import (
    supervision_tasks_eligible_population,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mo import (
    in_supervision_initial_assessment_phase,
)
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "US_MO_SUPERVISION_TASKS_ELIGIBLE_IAP_POPULATION"

# TODO(#57821): Update/refine candidate population to ensure it's correct.
_CRITERIA_GROUP = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name=_POPULATION_NAME,
    sub_criteria_list=[
        supervision_tasks_eligible_population.VIEW_BUILDER.as_criteria(
            criteria_name="US_MO_IN_SUPERVISION_TASKS_ELIGIBLE_POPULATION",
        ),
        in_supervision_initial_assessment_phase.VIEW_BUILDER,
    ],
)

VIEW_BUILDER: StateSpecificTaskCandidatePopulationBigQueryViewBuilder = (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder.from_criteria_group(
        criteria_group=_CRITERIA_GROUP,
        population_name=_POPULATION_NAME,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
