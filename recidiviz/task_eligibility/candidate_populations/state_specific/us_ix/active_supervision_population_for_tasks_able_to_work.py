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
This script selects all spans of time in which a person is a candidate for active supervision
for tasks in the state of Idaho AND is able to work. Specifically, it selects people with the
following conditions:
- Active supervision: probation, parole, or dual
- Case types: GENERAL, SEX_OFFENSE, XCRC, or MENTAL_HEALTH_COURT
- Supervision levels: MINIMUM, MEDIUM, HIGH, or XCRC
- Not marked as unable to work (meets is_able_to_work criteria)
"""

from recidiviz.task_eligibility.candidate_populations.state_specific.us_ix import (
    active_supervision_population_for_tasks,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import is_able_to_work
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "US_IX_ACTIVE_SUPERVISION_POPULATION_FOR_TASKS_ABLE_TO_WORK"

_CRITERIA_GROUP = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name=_POPULATION_NAME,
    sub_criteria_list=[
        active_supervision_population_for_tasks.VIEW_BUILDER.as_criteria(
            criteria_name="US_IX_IN_ACTIVE_SUPERVISION_POPULATION_FOR_TASKS",
        ),
        is_able_to_work.VIEW_BUILDER,
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
