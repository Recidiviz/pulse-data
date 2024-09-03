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
"""Tests for the TaskCompletionEventBigQueryViewCollector."""
import unittest
from collections import defaultdict
from typing import Dict, List, Tuple
from unittest.mock import Mock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateAgnosticTaskCompletionEventBigQueryViewBuilder,
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.task_eligibility.task_completion_event_big_query_view_collector import (
    TaskCompletionEventBigQueryViewCollector,
)


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-456"))
class TestTaskCompletionEventBigQueryViewCollector(unittest.TestCase):
    """Tests for the TaskCompletionEventBigQueryViewCollector."""

    def test_collect_all(self) -> None:
        collector = TaskCompletionEventBigQueryViewCollector()
        all_completion_event_builders = collector.collect_view_builders()

        # Fail if we didn't find any completion event
        self.assertGreater(len(all_completion_event_builders), 0)

        for builder in all_completion_event_builders:
            if not isinstance(
                builder, StateSpecificTaskCompletionEventBigQueryViewBuilder
            ) and not isinstance(
                builder, StateAgnosticTaskCompletionEventBigQueryViewBuilder
            ):
                raise ValueError(
                    f"Found unexpected completion event view builder type [{type(builder)}]: {builder}"
                )

            # Confirm that each view builds
            try:
                _ = builder.build()
            except Exception as e:
                raise ValueError(f"Failed to build view {builder.address}") from e

    def test_unique_state_agnostic_completion_event_types(self) -> None:
        """Checks that each StateAgnosticTaskCompletionEventBigQueryViewBuilder has a
        distinct completion event type.
        """
        collector = TaskCompletionEventBigQueryViewCollector()
        all_completion_event_builders = collector.collect_view_builders()

        event_type_to_builders: Dict[
            TaskCompletionEventType,
            List[StateAgnosticTaskCompletionEventBigQueryViewBuilder],
        ] = defaultdict(list)
        for builder in all_completion_event_builders:
            if isinstance(builder, StateSpecificTaskCompletionEventBigQueryViewBuilder):
                continue
            if not isinstance(
                builder, StateAgnosticTaskCompletionEventBigQueryViewBuilder
            ):
                raise ValueError(
                    f"Found unexpected completion event view builder type [{type(builder)}]: {builder}"
                )

            event_type_to_builders[builder.completion_event_type].append(builder)

        for completion_event_name, builders in event_type_to_builders.items():
            if len(builders) > 1:
                raise ValueError(
                    f"Found reused completion event type [{completion_event_name}] for "
                    f"state-agnostic builders: {[b.address for b in builders]}"
                )

    def test_unique_state_specific_completion_event_types(self) -> None:
        """Checks that each StateSpecificTaskCompletionEventBigQueryViewBuilder has a
        distinct completion event type within a given state.
        """
        collector = TaskCompletionEventBigQueryViewCollector()
        all_completion_event_builders = collector.collect_view_builders()

        event_type_to_builders: Dict[
            Tuple[TaskCompletionEventType, StateCode],
            List[StateSpecificTaskCompletionEventBigQueryViewBuilder],
        ] = defaultdict(list)
        for builder in all_completion_event_builders:
            if isinstance(builder, StateAgnosticTaskCompletionEventBigQueryViewBuilder):
                continue
            if not isinstance(
                builder, StateSpecificTaskCompletionEventBigQueryViewBuilder
            ):
                raise ValueError(
                    f"Found unexpected completion event view builder type [{type(builder)}]: {builder}"
                )

            event_type_to_builders[
                (builder.completion_event_type, builder.state_code)
            ].append(builder)

        for (
            completion_event_name,
            state_code,
        ), builders in event_type_to_builders.items():
            if len(builders) > 1:
                raise ValueError(
                    f"Found reused completion event type [{completion_event_name}] for "
                    f"state-specific builders for state [{state_code.value}]: "
                    f"{[b.address for b in builders]}"
                )

    def test_state_agnostic_no_raw_data_tables(self) -> None:
        collector = TaskCompletionEventBigQueryViewCollector()
        all_completion_event_builders = collector.collect_view_builders()
        for builder in all_completion_event_builders:
            if isinstance(builder, StateAgnosticTaskCompletionEventBigQueryViewBuilder):
                view = builder.build()

                for parent_address in view.parent_tables:
                    if parent_address.is_state_specific_address():
                        raise ValueError(
                            f"Found state-specific address [{parent_address}] "
                            f"referenced from state-agnostic completion event view "
                            f"[{view.address}]."
                        )

    def test_system_type_for_all_completion_event_types(self) -> None:
        collector = TaskCompletionEventBigQueryViewCollector()
        all_completion_event_builders = collector.collect_view_builders()
        for builder in all_completion_event_builders:
            _ = builder.completion_event_type.system_type
