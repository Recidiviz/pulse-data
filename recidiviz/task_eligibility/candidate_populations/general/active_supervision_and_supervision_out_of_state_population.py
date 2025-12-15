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
"""Defines a candidate population view containing all people who are on supervision (in or out
of state) and who are actively supervised as defined by excluding certain compartments and supervision levels
such as in custody, bench warrant, absconsion, or unknown.
"""
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.candidate_population_builders import (
    state_agnostic_candidate_population_view_builder,
)
from recidiviz.task_eligibility.utils.candidate_population_query_fragments import (
    active_supervision_population_additional_filters,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "ACTIVE_SUPERVISION_AND_SUPERVISION_OUT_OF_STATE_POPULATION"

VIEW_BUILDER: StateAgnosticTaskCandidatePopulationBigQueryViewBuilder = (
    state_agnostic_candidate_population_view_builder(
        population_name=_POPULATION_NAME,
        description=__doc__,
        additional_filters=active_supervision_population_additional_filters(),
        compartment_level_1=["SUPERVISION", "SUPERVISION_OUT_OF_STATE"],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
