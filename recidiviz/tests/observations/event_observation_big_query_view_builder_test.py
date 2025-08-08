# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for EventObservationBigQueryViewBuilder."""
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
class EventObservationBigQueryViewBuilderTest(unittest.TestCase):
    """Tests for EventObservationBigQueryViewBuilder."""

    def test_build_view_direct_address_sql_source(self) -> None:
        view_builder = EventObservationBigQueryViewBuilder(
            event_type=EventType.TRANSITIONS_TO_LIBERTY_ALL,
            description="My description",
            sql_source=BigQueryAddress.from_str("dataset.source_table"),
            attribute_cols=[
                "attribute_1",
                "attribute_2",
            ],
            event_date_col="my_date_col",
        )

        view = view_builder.build(sandbox_context=None)
        self.assertEqual(
            BigQueryAddress.from_str(
                "observations__person_event.transitions_to_liberty_all"
            ),
            view.address,
        )

        expected_view_query = """
SELECT DISTINCT
    person_id,
    state_code,
    DATE(my_date_col) AS event_date,
    CAST(attribute_1 AS STRING) AS attribute_1,
    CAST(attribute_2 AS STRING) AS attribute_2
FROM `test-project.dataset.source_table`
"""
        self.assertEqual(expected_view_query, view.view_query)

        address_overrides = (
            BigQueryAddressOverrides.Builder("input_prefix")
            .register_sandbox_override_for_entire_dataset("dataset")
            .build()
        )
        sandbox_context = BigQueryViewSandboxContext(
            parent_address_overrides=address_overrides,
            parent_address_formatter_provider=None,
            output_sandbox_dataset_prefix="my_prefix",
            state_code_filter=None,
        )
        view_with_overrides = view_builder.build(sandbox_context=sandbox_context)

        expected_view_query = """
SELECT DISTINCT
    person_id,
    state_code,
    DATE(my_date_col) AS event_date,
    CAST(attribute_1 AS STRING) AS attribute_1,
    CAST(attribute_2 AS STRING) AS attribute_2
FROM `test-project.input_prefix_dataset.source_table`
"""
        self.assertEqual(expected_view_query, view_with_overrides.view_query)

    def test_build_view_custom_sql_source(self) -> None:
        sql_source = """
        
SELECT *
FROM `{project_id}.another_dataset.table`;
        """

        view_builder = EventObservationBigQueryViewBuilder(
            event_type=EventType.WORKFLOWS_ACTIVE_USAGE_EVENT,
            description="My description",
            sql_source=sql_source,
            attribute_cols=[
                "attribute_1",
                "attribute_2",
            ],
            event_date_col="my_date_col",
        )

        view = view_builder.build(sandbox_context=None)

        self.assertEqual(
            BigQueryAddress.from_str(
                "observations__workflows_primary_user_event.workflows_active_usage_event"
            ),
            view.address,
        )

        expected_view_query = """
SELECT DISTINCT
    email_address,
    state_code,
    DATE(my_date_col) AS event_date,
    CAST(attribute_1 AS STRING) AS attribute_1,
    CAST(attribute_2 AS STRING) AS attribute_2
FROM (
    SELECT *
    FROM `test-project.another_dataset.table`
)
"""
        self.assertEqual(expected_view_query, view.view_query)

        address_overrides = (
            BigQueryAddressOverrides.Builder("input_prefix")
            .register_sandbox_override_for_entire_dataset("another_dataset")
            .build()
        )
        sandbox_context = BigQueryViewSandboxContext(
            parent_address_overrides=address_overrides,
            parent_address_formatter_provider=None,
            output_sandbox_dataset_prefix="my_prefix",
            state_code_filter=None,
        )
        view_with_overrides = view_builder.build(sandbox_context=sandbox_context)

        expected_view_query = """
SELECT DISTINCT
    email_address,
    state_code,
    DATE(my_date_col) AS event_date,
    CAST(attribute_1 AS STRING) AS attribute_1,
    CAST(attribute_2 AS STRING) AS attribute_2
FROM (
    SELECT *
    FROM `test-project.input_prefix_another_dataset.table`
)
"""
        self.assertEqual(expected_view_query, view_with_overrides.view_query)
