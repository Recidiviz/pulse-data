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
"""Tests for the AlmostEligibleSpansBigQueryViewCollector."""
import unittest
from collections import defaultdict
from unittest.mock import Mock, patch

from recidiviz.task_eligibility.almost_eligible_spans_big_query_view_builder import (
    AlmostEligibleSpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.almost_eligible_task_spans_big_query_view_collector import (
    AlmostEligibleSpansBigQueryViewCollector,
)
from recidiviz.task_eligibility.basic_single_task_eligibility_spans_big_query_view_collector import (
    BasicSingleTaskEligibilitySpansBigQueryViewCollector,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_collector import (
    SingleTaskEligibilityBigQueryViewCollector,
)


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-456"))
class TestSingleTaskEligibilityBigQueryViewCollector(unittest.TestCase):
    """Tests for the AlmostEligibleSpansBigQueryViewCollector."""

    def setUp(self) -> None:
        self.tes_builders = (
            SingleTaskEligibilityBigQueryViewCollector().collect_view_builders()
        )
        self.basic_tes_builders = BasicSingleTaskEligibilitySpansBigQueryViewCollector(
            self.tes_builders
        ).collect_view_builders()

    def test_collect_all(self) -> None:
        collector = AlmostEligibleSpansBigQueryViewCollector(
            self.basic_tes_builders, self.tes_builders
        )
        all_almost_eligible_builders = collector.collect_view_builders()

        # Fail if we didn't find any builders
        self.assertGreater(len(all_almost_eligible_builders), 0)

        for builder in all_almost_eligible_builders:
            if not isinstance(builder, AlmostEligibleSpansBigQueryViewBuilder):
                raise ValueError(
                    f"Found unexpected task almost eligibile view builder type "
                    f"[{type(builder)}]: {builder}"
                )

            # Confirm that each view builds
            try:
                _ = builder.build()
            except Exception as e:
                raise ValueError(f"Failed to build view {builder.address}") from e

    def test_collect_by_state(self) -> None:
        collector = AlmostEligibleSpansBigQueryViewCollector(
            self.basic_tes_builders, self.tes_builders
        )
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

    def test_unique_task_names_within_state(self) -> None:
        collector = AlmostEligibleSpansBigQueryViewCollector(
            self.basic_tes_builders, self.tes_builders
        )
        all_task_eligibility_builders_by_state = (
            collector.collect_view_builders_by_state()
        )

        for state_code, builders in all_task_eligibility_builders_by_state.items():
            task_name_to_builders = defaultdict(list)
            for builder in builders:
                if not isinstance(builder, AlmostEligibleSpansBigQueryViewBuilder):
                    raise ValueError(
                        f"Found unexpected almost eligible view builder type "
                        f"[{type(builder)}]: {builder}"
                    )

                task_name_to_builders[builder.task_name].append(builder)

            for task_name, builders in task_name_to_builders.items():
                if len(builders) > 1:
                    raise ValueError(
                        f"Found reused task name [{task_name}] for "
                        f"[{state_code}] builders: {[b.address for b in builders]}"
                    )
