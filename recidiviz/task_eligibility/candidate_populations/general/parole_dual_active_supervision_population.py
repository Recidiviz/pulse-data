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
"""Selects all spans of time in which a person is on parole or dual
supervision and actively supervised as defined by excluding certain compartments and supervision levels 
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

_POPULATION_NAME = "PAROLE_DUAL_ACTIVE_SUPERVISION_POPULATION"

VIEW_BUILDER: StateAgnosticTaskCandidatePopulationBigQueryViewBuilder = (
    state_agnostic_candidate_population_view_builder(
        population_name=_POPULATION_NAME,
        description=__doc__,
        additional_filters=active_supervision_population_additional_filters(
            included_compartment_level_2="('PAROLE', 'DUAL')"
        ),
        compartment_level_1=["SUPERVISION"],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
