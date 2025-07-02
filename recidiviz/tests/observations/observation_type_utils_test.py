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
"""Tests for helpers in observation_type_utils.py"""
import unittest

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.observations.event_type import EventType
from recidiviz.observations.observation_type_utils import (
    attributes_column_name_for_observation_type,
    date_column_names_for_observation_type,
    materialized_view_address_for_observation,
    observation_attribute_value_clause,
)
from recidiviz.observations.span_type import SpanType


class TestObservationTypeUtils(unittest.TestCase):
    """Tests for helpers in observation_type_utils.py"""

    def test_date_column_names_for_observation_type(self) -> None:
        self.assertEqual(
            ["event_date"],
            date_column_names_for_observation_type(EventType.DRUG_SCREEN),
        )
        self.assertEqual(
            ["start_date", "end_date"],
            date_column_names_for_observation_type(SpanType.SENTENCE_SPAN),
        )

    def test_attributes_column_name_for_observation_type(self) -> None:
        self.assertEqual(
            "event_attributes",
            attributes_column_name_for_observation_type(EventType.DRUG_SCREEN),
        )
        self.assertEqual(
            "span_attributes",
            attributes_column_name_for_observation_type(SpanType.SENTENCE_SPAN),
        )

    def test_observation_attribute_value_clause(self) -> None:
        self.assertEqual(
            'JSON_EXTRACT_SCALAR(event_attributes, "$.is_positive_result")',
            observation_attribute_value_clause(
                observation_type=EventType.DRUG_SCREEN,
                attribute="is_positive_result",
                read_attributes_from_json=True,
            ),
        )
        self.assertEqual(
            "is_positive_result",
            observation_attribute_value_clause(
                observation_type=EventType.DRUG_SCREEN,
                attribute="is_positive_result",
                read_attributes_from_json=False,
            ),
        )
        self.assertEqual(
            'JSON_EXTRACT_SCALAR(span_attributes, "$.effective_date")',
            observation_attribute_value_clause(
                observation_type=SpanType.SENTENCE_SPAN,
                attribute="effective_date",
                read_attributes_from_json=True,
            ),
        )
        self.assertEqual(
            "effective_date",
            observation_attribute_value_clause(
                observation_type=SpanType.SENTENCE_SPAN,
                attribute="effective_date",
                read_attributes_from_json=False,
            ),
        )

    def test_materialized_view_address_for_observation(self) -> None:
        self.assertEqual(
            BigQueryAddress.from_str(
                "observations__person_event.drug_screen_materialized"
            ),
            materialized_view_address_for_observation(EventType.DRUG_SCREEN),
        )
        self.assertEqual(
            BigQueryAddress.from_str(
                "observations__workflows_primary_user_event.workflows_active_usage_event_materialized"
            ),
            materialized_view_address_for_observation(
                EventType.WORKFLOWS_ACTIVE_USAGE_EVENT
            ),
        )
        self.assertEqual(
            BigQueryAddress.from_str(
                "observations__person_span.sentence_span_materialized"
            ),
            materialized_view_address_for_observation(SpanType.SENTENCE_SPAN),
        )
