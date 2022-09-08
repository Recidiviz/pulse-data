# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Defines a candidate population view containing all people who are actively
supervised at any point in time.
"""
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.candidate_population_builders import (
    state_agnostic_supervision_candidate_population_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "SUPERVISION_POPULATION"

_DESCRIPTION = """Selects all spans of time in which a person is supervised as tracked
by data in our `sessions` dataset. All types of supervision (including INTERNAL_UNKNOWN)
are included.
"""

VIEW_BUILDER: StateAgnosticTaskCandidatePopulationBigQueryViewBuilder = (
    state_agnostic_supervision_candidate_population_view_builder(
        population_name=_POPULATION_NAME,
        description=_DESCRIPTION,
        additional_filters=[],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
