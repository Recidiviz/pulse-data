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
"""Tests for ObservationSelector"""
import unittest

from recidiviz.observations.event_selector import EventSelector
from recidiviz.observations.event_type import EventType
from recidiviz.observations.span_selector import SpanSelector
from recidiviz.observations.span_type import SpanType


class TestObservationSelector(unittest.TestCase):
    """Tests for ObservationSelector"""

    def test_generate_observation_conditions_query_fragment_span_no_conditions(
        self,
    ) -> None:
        selector = SpanSelector(
            span_type=SpanType.COMPARTMENT_SESSION,
            span_conditions_dict={},
        )

        self.assertEqual(
            'span = "COMPARTMENT_SESSION"',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=True, read_attributes_from_json=True
            ),
        )
        self.assertEqual(
            "TRUE",
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False, read_attributes_from_json=True
            ),
        )
        self.assertEqual(
            "TRUE",
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False, read_attributes_from_json=False
            ),
        )

    def test_generate_observation_conditions_query_fragment_event_no_conditions(
        self,
    ) -> None:
        selector = EventSelector(
            event_type=EventType.VIOLATION_RESPONSE,
            event_conditions_dict={},
        )

        self.assertEqual(
            'event = "VIOLATION_RESPONSE"',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=True, read_attributes_from_json=True
            ),
        )
        self.assertEqual(
            "TRUE",
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False, read_attributes_from_json=True
            ),
        )
        self.assertEqual(
            "TRUE",
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False, read_attributes_from_json=False
            ),
        )

    def test_generate_observation_conditions_query_fragment_span_one_condition(
        self,
    ) -> None:
        selector = SpanSelector(
            span_type=SpanType.WORKFLOWS_USER_REGISTRATION_SESSION,
            span_conditions_dict={"system_type": ["SUPERVISION"]},
        )

        self.assertEqual(
            'span = "WORKFLOWS_USER_REGISTRATION_SESSION"\n'
            '        AND JSON_EXTRACT_SCALAR(span_attributes, "$.system_type") IN ("SUPERVISION")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=True, read_attributes_from_json=True
            ),
        )
        self.assertEqual(
            'JSON_EXTRACT_SCALAR(span_attributes, "$.system_type") IN ("SUPERVISION")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False, read_attributes_from_json=True
            ),
        )
        self.assertEqual(
            'system_type IN ("SUPERVISION")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False, read_attributes_from_json=False
            ),
        )

    def test_generate_observation_conditions_query_fragment_event_one_condition(
        self,
    ) -> None:
        selector = EventSelector(
            event_type=EventType.INCARCERATION_RELEASE,
            event_conditions_dict={"outflow_to_level_1": ["LIBERTY", "SUPERVISION"]},
        )

        self.assertEqual(
            'event = "INCARCERATION_RELEASE"\n'
            '        AND JSON_EXTRACT_SCALAR(event_attributes, "$.outflow_to_level_1") IN ("LIBERTY", "SUPERVISION")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=True, read_attributes_from_json=True
            ),
        )
        self.assertEqual(
            'JSON_EXTRACT_SCALAR(event_attributes, "$.outflow_to_level_1") IN ("LIBERTY", "SUPERVISION")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False, read_attributes_from_json=True
            ),
        )
        self.assertEqual(
            'outflow_to_level_1 IN ("LIBERTY", "SUPERVISION")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False, read_attributes_from_json=False
            ),
        )

    def test_generate_observation_conditions_query_fragment_span_multiple_conditions(
        self,
    ) -> None:
        selector = SpanSelector(
            span_type=SpanType.COMPARTMENT_SESSION,
            span_conditions_dict={
                "compartment_level_1": ["SUPERVISION"],
                "case_type_start": """NOT IN (
    "GENERAL", "DOMESTIC_VIOLENCE"
)""",
            },
        )

        self.assertEqual(
            'span = "COMPARTMENT_SESSION"\n'
            '        AND JSON_EXTRACT_SCALAR(span_attributes, "$.compartment_level_1") IN ("SUPERVISION")\n'
            '        AND JSON_EXTRACT_SCALAR(span_attributes, "$.case_type_start") NOT IN (\n'
            '    "GENERAL", "DOMESTIC_VIOLENCE"\n'
            ")",
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=True, read_attributes_from_json=True
            ),
        )
        self.assertEqual(
            'JSON_EXTRACT_SCALAR(span_attributes, "$.compartment_level_1") IN ("SUPERVISION")\n'
            '        AND JSON_EXTRACT_SCALAR(span_attributes, "$.case_type_start") NOT IN (\n'
            '    "GENERAL", "DOMESTIC_VIOLENCE"\n'
            ")",
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False, read_attributes_from_json=True
            ),
        )
        self.assertEqual(
            'compartment_level_1 IN ("SUPERVISION")\n'
            "        AND case_type_start NOT IN (\n"
            '    "GENERAL", "DOMESTIC_VIOLENCE"\n'
            ")",
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False, read_attributes_from_json=False
            ),
        )

    def test_generate_observation_conditions_query_fragment_event_multiple_conditions(
        self,
    ) -> None:
        selector = EventSelector(
            event_type=EventType.SUPERVISION_CONTACT,
            event_conditions_dict={
                "status": ["COMPLETED"],
                "contact_type": ["DIRECT", "BOTH_COLLATERAL_AND_DIRECT"],
            },
        )

        self.assertEqual(
            'event = "SUPERVISION_CONTACT"\n'
            '        AND JSON_EXTRACT_SCALAR(event_attributes, "$.status") IN ("COMPLETED")\n'
            '        AND JSON_EXTRACT_SCALAR(event_attributes, "$.contact_type") IN ("DIRECT", "BOTH_COLLATERAL_AND_DIRECT")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=True, read_attributes_from_json=True
            ),
        )
        self.assertEqual(
            'JSON_EXTRACT_SCALAR(event_attributes, "$.status") IN ("COMPLETED")\n'
            '        AND JSON_EXTRACT_SCALAR(event_attributes, "$.contact_type") IN ("DIRECT", "BOTH_COLLATERAL_AND_DIRECT")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False, read_attributes_from_json=True
            ),
        )
        self.assertEqual(
            'status IN ("COMPLETED")\n'
            '        AND contact_type IN ("DIRECT", "BOTH_COLLATERAL_AND_DIRECT")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False, read_attributes_from_json=False
            ),
        )
