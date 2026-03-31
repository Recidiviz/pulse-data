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
"""Candidate population combining prioritized supervision with IN_CUSTODY supervision status.

This population is the intersection of:
1. Prioritized supervision population (people on supervision with an assigned officer)
2. IN IN_CUSTODY supervision status

This can be used by states to ensure tasks are only considered for people
on supervised in-custody status.
"""

from recidiviz.task_eligibility.candidate_populations.general import (
    prioritized_supervision_population_with_officer,
)
from recidiviz.task_eligibility.criteria.general import supervision_level_is_in_custody
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateAgnosticTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "PRIORITIZED_SUPERVISION_POPULATION_IN_CUSTODY_WITH_OFFICER"

# Use state-agnostic criteria group to combine both criteria using AND logic
_CRITERIA_GROUP = StateAgnosticTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name=_POPULATION_NAME,
    sub_criteria_list=[
        prioritized_supervision_population_with_officer.VIEW_BUILDER.as_criteria(
            criteria_name="IN_PRIORITIZED_SUPERVISION_POPULATION_WITH_OFFICER",
        ),
        supervision_level_is_in_custody.VIEW_BUILDER,
    ],
)

VIEW_BUILDER: StateAgnosticTaskCandidatePopulationBigQueryViewBuilder = (
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder.from_criteria_group(
        criteria_group=_CRITERIA_GROUP,
        population_name=_POPULATION_NAME,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
