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
"""Defines a class that can be used to collect view builders of type
TaskCandidatePopulationBigQueryViewBuilder.
"""
from typing import List

from recidiviz.big_query.big_query_view_collector import (
    BigQueryViewCollector,
    filename_matches_view_id_validator,
)
from recidiviz.task_eligibility.candidate_populations import (
    general as general_population_module,
)
from recidiviz.task_eligibility.candidate_populations import (
    state_specific as state_specific_population_module,
)
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder,
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
    TaskCandidatePopulationBigQueryViewBuilder,
)


class TaskCandidatePopulationBigQueryViewCollector(
    BigQueryViewCollector[TaskCandidatePopulationBigQueryViewBuilder]
):
    """A class that can be used to collect view builders of type
    TaskCandidatePopulationBigQueryViewBuilder.
    """

    def collect_view_builders(self) -> List[TaskCandidatePopulationBigQueryViewBuilder]:
        """Returns a list of all defined TaskCriteriaBigQueryViewBuilder, both the
        StateAgnosticTaskCandidatePopulationBigQueryViewBuilder which contain general
        queries that can be used for any state and the
        StateSpecificTaskCandidatePopulationBigQueryViewBuilder which apply logic
        specific to a particular state.
        """
        view_builders = self.collect_view_builders_in_module(
            StateAgnosticTaskCandidatePopulationBigQueryViewBuilder,
            general_population_module,
            validate_builder_fn=filename_matches_view_id_validator,
        )

        for state_population_module in self.get_submodules(
            state_specific_population_module, submodule_name_prefix_filter=None
        ):
            view_builders.extend(
                self.collect_view_builders_in_module(
                    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
                    state_population_module,
                    validate_builder_fn=filename_matches_view_id_validator,
                )
            )

        return view_builders
