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
"""Tests for the SingleTaskEligibilityBigQueryViewCollector."""
import unittest
from collections import defaultdict
from unittest.mock import Mock, patch

from recidiviz.task_eligibility.single_task_eligibility_spans_view_collector import (
    SingleTaskEligibilityBigQueryViewCollector,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-456"))
class TestSingleTaskEligibilityBigQueryViewCollector(unittest.TestCase):
    """Tests for the SingleTaskEligibilityBigQueryViewCollector."""

    def test_collect_all(self) -> None:
        collector = SingleTaskEligibilityBigQueryViewCollector()
        all_task_eligibility_builders = collector.collect_view_builders()

        # Fail if we didn't find any criteria
        self.assertGreater(len(all_task_eligibility_builders), 0)

        for builder in all_task_eligibility_builders:
            if not isinstance(builder, SingleTaskEligibilitySpansBigQueryViewBuilder):
                raise ValueError(
                    f"Found unexpected task eligiblity view builder type "
                    f"[{type(builder)}]: {builder}"
                )

            # Confirm that each view builds
            try:
                _ = builder.build()
            except Exception as e:
                raise ValueError(f"Failed to build view {builder.address}") from e

    def test_collect_by_state(self) -> None:
        collector = SingleTaskEligibilityBigQueryViewCollector()
        all_task_eligibility_builders_by_state = (
            collector.collect_view_builders_by_state()
        )

        all_task_eligibility_builders = collector.collect_view_builders()

        flattened_builders_by_state = [
            b
            for builders in all_task_eligibility_builders_by_state.values()
            for b in builders
        ]
        self.assertCountEqual(
            all_task_eligibility_builders, flattened_builders_by_state
        )

    def test_unique_criteria_names_within_state(self) -> None:
        collector = SingleTaskEligibilityBigQueryViewCollector()
        all_task_eligibility_builders_by_state = (
            collector.collect_view_builders_by_state()
        )

        for state_code, builders in all_task_eligibility_builders_by_state.items():
            criteria_name_to_builders = defaultdict(list)
            for builder in builders:
                if not isinstance(
                    builder, SingleTaskEligibilitySpansBigQueryViewBuilder
                ):
                    raise ValueError(
                        f"Found unexpected task eligiblity view builder type "
                        f"[{type(builder)}]: {builder}"
                    )

                criteria_name_to_builders[builder.task_name].append(builder)

            for criteria_name, builders in criteria_name_to_builders.items():
                if len(builders) > 1:
                    raise ValueError(
                        f"Found reused criteria name [{criteria_name}] for "
                        f"[{state_code}] builders: {[b.address for b in builders]}"
                    )

    def test_state_codes_agree(self) -> None:
        collector = SingleTaskEligibilityBigQueryViewCollector()
        all_task_eligibility_builders = collector.collect_view_builders()

        for task_builder in all_task_eligibility_builders:
            if not isinstance(
                task_builder, SingleTaskEligibilitySpansBigQueryViewBuilder
            ):
                raise ValueError(
                    f"Found unexpected task eligiblity view builder type "
                    f"[{type(task_builder)}]: {task_builder}"
                )

            expected_state_code = task_builder.state_code

            if isinstance(
                task_builder.candidate_population_view_builder,
                StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
            ):
                self.assertEqual(
                    expected_state_code,
                    task_builder.candidate_population_view_builder.state_code,
                )
            for criteria_builder in task_builder.criteria_spans_view_builders:
                if isinstance(
                    criteria_builder,
                    StateSpecificTaskCriteriaBigQueryViewBuilder,
                ):
                    self.assertEqual(expected_state_code, criteria_builder.state_code)
