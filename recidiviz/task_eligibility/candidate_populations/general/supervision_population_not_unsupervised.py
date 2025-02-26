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
"""Defines a candidate population view containing all people who are
supervised whose compartment_level_2 does not include Absconsion or Bench Warrant, and whose
 supervision level does not include Unsupervised.
"""
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.candidate_population_builders import (
    state_agnostic_candidate_population_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "SUPERVISION_POPULATION_NOT_UNSUPERVISED"

_DESCRIPTION = """Selects all spans of time in which a person is supervised,
their compartment_level_2 does not include Absconsion or Bench Warrant, and their
 supervision level does not include Unsupervised, as tracked by data in our `sessions` dataset.
"""

VIEW_BUILDER: StateAgnosticTaskCandidatePopulationBigQueryViewBuilder = state_agnostic_candidate_population_view_builder(
    population_name=_POPULATION_NAME,
    description=_DESCRIPTION,
    additional_filters=[
        'attr.compartment_level_2 NOT IN ("INTERNAL_UNKNOWN", "ABSCONSION", "BENCH_WARRANT")',
        'attr.correctional_level NOT IN ("UNSUPERVISED")',
    ],
    compartment_level_1="SUPERVISION",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
