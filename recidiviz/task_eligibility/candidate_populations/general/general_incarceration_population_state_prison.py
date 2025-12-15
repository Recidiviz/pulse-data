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
"""Selects all spans of time in which a person is in GENERAL INCARCERATION
as tracked by data in our `sessions` dataset, as well as STATE_PRISON custodial authority"""
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.candidate_population_builders import (
    state_agnostic_candidate_population_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "GENERAL_INCARCERATION_POPULATION_STATE_PRISON"

VIEW_BUILDER: StateAgnosticTaskCandidatePopulationBigQueryViewBuilder = (
    state_agnostic_candidate_population_view_builder(
        population_name=_POPULATION_NAME,
        description=__doc__,
        additional_filters=[
            'compartment_level_2 = "GENERAL" ',
            'custodial_authority = "STATE_PRISON" ',
        ],
        compartment_level_1=["INCARCERATION"],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
