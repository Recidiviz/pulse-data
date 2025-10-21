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
from recidiviz.observations.observation_selector import ObservationSelector
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
                filter_by_observation_type=True,
                read_attributes_from_json=True,
                strip_newlines=False,
            ),
        )
        self.assertEqual(
            "TRUE",
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False,
                read_attributes_from_json=True,
                strip_newlines=False,
            ),
        )
        self.assertEqual(
            "TRUE",
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False,
                read_attributes_from_json=False,
                strip_newlines=False,
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
                filter_by_observation_type=True,
                read_attributes_from_json=True,
                strip_newlines=False,
            ),
        )
        self.assertEqual(
            "TRUE",
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False,
                read_attributes_from_json=True,
                strip_newlines=False,
            ),
        )
        self.assertEqual(
            "TRUE",
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False,
                read_attributes_from_json=False,
                strip_newlines=False,
            ),
        )

    def test_generate_observation_conditions_query_fragment_span_one_condition(
        self,
    ) -> None:
        selector = SpanSelector(
            span_type=SpanType.WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSION,
            span_conditions_dict={"system_type": ["SUPERVISION"]},
        )

        self.assertEqual(
            'span = "WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSION"\n'
            '        AND JSON_EXTRACT_SCALAR(span_attributes, "$.system_type") IN ("SUPERVISION")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=True,
                read_attributes_from_json=True,
                strip_newlines=False,
            ),
        )
        self.assertEqual(
            'JSON_EXTRACT_SCALAR(span_attributes, "$.system_type") IN ("SUPERVISION")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False,
                read_attributes_from_json=True,
                strip_newlines=False,
            ),
        )
        self.assertEqual(
            'system_type IN ("SUPERVISION")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False,
                read_attributes_from_json=False,
                strip_newlines=False,
            ),
        )

    def test_generate_observation_conditions_query_fragment_event_one_condition(
        self,
    ) -> None:
        selector = EventSelector(
            event_type=EventType.SUPERVISION_CONTACT,
            event_conditions_dict={"status": ["ATTEMPTED", "COMPLETED"]},
        )

        self.assertEqual(
            'event = "SUPERVISION_CONTACT"\n'
            '        AND JSON_EXTRACT_SCALAR(event_attributes, "$.status") IN ("ATTEMPTED", "COMPLETED")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=True,
                read_attributes_from_json=True,
                strip_newlines=False,
            ),
        )
        self.assertEqual(
            'JSON_EXTRACT_SCALAR(event_attributes, "$.status") IN ("ATTEMPTED", "COMPLETED")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False,
                read_attributes_from_json=True,
                strip_newlines=False,
            ),
        )
        self.assertEqual(
            'status IN ("ATTEMPTED", "COMPLETED")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False,
                read_attributes_from_json=False,
                strip_newlines=False,
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
                filter_by_observation_type=True,
                read_attributes_from_json=True,
                strip_newlines=False,
            ),
        )
        self.assertEqual(
            'JSON_EXTRACT_SCALAR(span_attributes, "$.compartment_level_1") IN ("SUPERVISION")\n'
            '        AND JSON_EXTRACT_SCALAR(span_attributes, "$.case_type_start") NOT IN (\n'
            '    "GENERAL", "DOMESTIC_VIOLENCE"\n'
            ")",
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False,
                read_attributes_from_json=True,
                strip_newlines=False,
            ),
        )
        self.assertEqual(
            'compartment_level_1 IN ("SUPERVISION")\n'
            "        AND case_type_start NOT IN (\n"
            '    "GENERAL", "DOMESTIC_VIOLENCE"\n'
            ")",
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False,
                read_attributes_from_json=False,
                strip_newlines=False,
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
                filter_by_observation_type=True,
                read_attributes_from_json=True,
                strip_newlines=False,
            ),
        )
        self.assertEqual(
            'JSON_EXTRACT_SCALAR(event_attributes, "$.status") IN ("COMPLETED")\n'
            '        AND JSON_EXTRACT_SCALAR(event_attributes, "$.contact_type") IN ("DIRECT", "BOTH_COLLATERAL_AND_DIRECT")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False,
                read_attributes_from_json=True,
                strip_newlines=False,
            ),
        )
        self.assertEqual(
            'status IN ("COMPLETED")\n'
            '        AND contact_type IN ("DIRECT", "BOTH_COLLATERAL_AND_DIRECT")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False,
                read_attributes_from_json=False,
                strip_newlines=False,
            ),
        )
        self.assertEqual(
            'status IN ("COMPLETED") '
            'AND contact_type IN ("DIRECT", "BOTH_COLLATERAL_AND_DIRECT")',
            selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=False,
                read_attributes_from_json=False,
                strip_newlines=True,
            ),
        )

    def test_build_selected_observations_query_no_attributes(self) -> None:
        query = ObservationSelector.build_selected_observations_query_template(
            observation_type=SpanType.COMPARTMENT_SESSION,
            observation_selectors=[
                SpanSelector(
                    span_type=SpanType.COMPARTMENT_SESSION,
                    span_conditions_dict={},
                )
            ],
            output_attribute_columns=[],
        )

        expected_query = """
SELECT
    person_id,
    state_code,
    start_date,
    end_date
FROM 
    `{project_id}.observations__person_span.compartment_session_materialized`
WHERE
    TRUE
"""
        self.assertEqual(expected_query, query)

    def test_build_selected_observations_query_simple_span(self) -> None:
        query = ObservationSelector.build_selected_observations_query_template(
            observation_type=SpanType.COMPARTMENT_SESSION,
            observation_selectors=[
                SpanSelector(
                    span_type=SpanType.COMPARTMENT_SESSION,
                    span_conditions_dict={
                        "compartment_level_1": ["INCARCERATION", "SUPERVISION"],
                        "compartment_level_2": ["COMMUNITY_CONFINEMENT"],
                    },
                )
            ],
            output_attribute_columns=["compartment_level_1", "case_type_start"],
        )

        expected_query = """
SELECT
    person_id,
    state_code,
    start_date,
    end_date,
    case_type_start,
    compartment_level_1
FROM 
    `{project_id}.observations__person_span.compartment_session_materialized`
WHERE
    compartment_level_1 IN ("INCARCERATION", "SUPERVISION") AND compartment_level_2 IN ("COMMUNITY_CONFINEMENT")
"""
        self.assertEqual(expected_query, query)

    def test_build_selected_observations_query_simple_event(self) -> None:
        query = ObservationSelector.build_selected_observations_query_template(
            observation_type=EventType.CUSTODY_LEVEL_CHANGE,
            observation_selectors=[
                EventSelector(
                    event_type=EventType.CUSTODY_LEVEL_CHANGE,
                    event_conditions_dict={"change_type": ["DOWNGRADE"]},
                )
            ],
            output_attribute_columns=["change_type", "previous_custody_level"],
        )

        expected_query = """
SELECT
    person_id,
    state_code,
    event_date,
    change_type,
    previous_custody_level
FROM 
    `{project_id}.observations__person_event.custody_level_change_materialized`
WHERE
    change_type IN ("DOWNGRADE")
"""
        self.assertEqual(expected_query, query)

    def test_build_selected_observations_query_multiple_events(self) -> None:
        query = ObservationSelector.build_selected_observations_query_template(
            observation_type=EventType.CUSTODY_LEVEL_CHANGE,
            observation_selectors=[
                EventSelector(
                    event_type=EventType.CUSTODY_LEVEL_CHANGE,
                    event_conditions_dict={"change_type": ["DOWNGRADE"]},
                ),
                EventSelector(
                    event_type=EventType.CUSTODY_LEVEL_CHANGE,
                    event_conditions_dict={"change_type": ["UPGRADE"]},
                ),
                EventSelector(
                    event_type=EventType.CUSTODY_LEVEL_CHANGE,
                    event_conditions_dict={
                        "change_type": ["DOWNGRADE"],
                        "new_custody_level": ["MINIMUM"],
                    },
                ),
            ],
            output_attribute_columns=["change_type", "previous_custody_level"],
        )

        expected_query = """
SELECT
    person_id,
    state_code,
    event_date,
    change_type,
    previous_custody_level
FROM 
    `{project_id}.observations__person_event.custody_level_change_materialized`
WHERE
    ( change_type IN ("DOWNGRADE") )
    OR ( change_type IN ("UPGRADE") )
    OR ( change_type IN ("DOWNGRADE") AND new_custody_level IN ("MINIMUM") )
"""
        self.assertEqual(expected_query, query)
