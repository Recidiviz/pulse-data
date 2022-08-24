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
"""Tests for the TaskCandidatePopulationBigQueryViewCollector."""
import unittest
from collections import defaultdict
from unittest.mock import Mock, patch

from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder,
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_candidate_population_big_query_view_collector import (
    TaskCandidatePopulationBigQueryViewCollector,
)
from recidiviz.view_registry.datasets import is_state_specific_address


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-456"))
class TestTaskCandidatePopulationBigQueryViewCollector(unittest.TestCase):
    """Tests for the TaskCandidatePopulationBigQueryViewCollector."""

    def test_collect_all(self) -> None:
        collector = TaskCandidatePopulationBigQueryViewCollector()
        all_population_builders = collector.collect_view_builders()

        # Fail if we didn't find any population
        self.assertGreater(len(all_population_builders), 0)

        for builder in all_population_builders:
            if not isinstance(
                builder, StateAgnosticTaskCandidatePopulationBigQueryViewBuilder
            ) and not isinstance(
                builder, StateSpecificTaskCandidatePopulationBigQueryViewBuilder
            ):
                raise ValueError(
                    f"Found unexpected population view builder type [{type(builder)}]: {builder}"
                )

            # Confirm that each view builds
            try:
                _ = builder.build()
            except Exception as e:
                raise ValueError(f"Failed to build view {builder.address}") from e

    def test_unique_population_names(self) -> None:
        collector = TaskCandidatePopulationBigQueryViewCollector()
        all_population_builders = collector.collect_view_builders()

        population_name_to_builders = defaultdict(list)
        for builder in all_population_builders:
            if not isinstance(
                builder, StateAgnosticTaskCandidatePopulationBigQueryViewBuilder
            ) and not isinstance(
                builder, StateSpecificTaskCandidatePopulationBigQueryViewBuilder
            ):
                raise ValueError(
                    f"Found unexpected population view builder type [{type(builder)}]: {builder}"
                )

            population_name_to_builders[builder.population_name].append(builder)

        for population_name, builders in population_name_to_builders.items():
            if len(builders) > 1:
                raise ValueError(
                    f"Found reused population name [{population_name}] for builders: "
                    f"{[b.address for b in builders]}"
                )

    def test_state_agnostic_no_raw_data_tables(self) -> None:
        collector = TaskCandidatePopulationBigQueryViewCollector()
        all_criteria_builders = collector.collect_view_builders()
        for builder in all_criteria_builders:
            if isinstance(
                builder, StateAgnosticTaskCandidatePopulationBigQueryViewBuilder
            ):
                view = builder.build()

                for parent_address in view.parent_tables:
                    if is_state_specific_address(parent_address):
                        raise ValueError(
                            f"Found state-specific address [{parent_address}] "
                            f"referenced from state-agnostic criteria view "
                            f"[{view.address}]."
                        )
