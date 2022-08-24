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
"""Tests for the TaskCriteriaBigQueryViewCollector."""

import unittest
from collections import defaultdict
from unittest.mock import Mock, patch

from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_collector import (
    TaskCriteriaBigQueryViewCollector,
)
from recidiviz.view_registry.datasets import is_state_specific_address


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-456"))
class TestTaskCriteriaBigQueryViewCollector(unittest.TestCase):
    """Tests for the TaskCriteriaBigQueryViewCollector."""

    def test_collect_all(self) -> None:
        collector = TaskCriteriaBigQueryViewCollector()
        all_criteria_builders = collector.collect_view_builders()

        # Fail if we didn't find any criteria
        self.assertGreater(len(all_criteria_builders), 0)

        for builder in all_criteria_builders:
            if not isinstance(
                builder, StateSpecificTaskCriteriaBigQueryViewBuilder
            ) and not isinstance(builder, StateAgnosticTaskCriteriaBigQueryViewBuilder):
                raise ValueError(
                    f"Found unexpected criteria view builder type [{type(builder)}]: {builder}"
                )

            # Confirm that each view builds
            try:
                _ = builder.build()
            except Exception as e:
                raise ValueError(f"Failed to build view {builder.address}") from e

    def test_unique_criteria_names(self) -> None:
        collector = TaskCriteriaBigQueryViewCollector()
        all_criteria_builders = collector.collect_view_builders()

        criteria_name_to_builders = defaultdict(list)
        for builder in all_criteria_builders:
            if not isinstance(
                builder, StateSpecificTaskCriteriaBigQueryViewBuilder
            ) and not isinstance(builder, StateAgnosticTaskCriteriaBigQueryViewBuilder):
                raise ValueError(
                    f"Found unexpected criteria view builder type [{type(builder)}]: {builder}"
                )

            criteria_name_to_builders[builder.criteria_name].append(builder)

        for criteria_name, builders in criteria_name_to_builders.items():
            if len(builders) > 1:
                raise ValueError(
                    f"Found reused criteria name [{criteria_name}] for builders: "
                    f"{[b.address for b in builders]}"
                )

    def test_state_agnostic_no_raw_data_tables(self) -> None:
        collector = TaskCriteriaBigQueryViewCollector()
        all_criteria_builders = collector.collect_view_builders()
        for builder in all_criteria_builders:
            if isinstance(builder, StateAgnosticTaskCriteriaBigQueryViewBuilder):
                view = builder.build()

                for parent_address in view.parent_tables:
                    if is_state_specific_address(parent_address):
                        raise ValueError(
                            f"Found state-specific address [{parent_address}] "
                            f"referenced from state-agnostic criteria view "
                            f"[{view.address}]."
                        )
