# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests functionality of EventSelector functions"""

import unittest

from recidiviz.calculator.query.state.views.analyst_data.models.event_selector import (
    EventSelector,
)
from recidiviz.calculator.query.state.views.analyst_data.models.event_type import (
    EventType,
)


class EventSelectorTest(unittest.TestCase):
    """
    Tests EventSelector functions
    """

    # Check that generate_event_selector_query returns intended output for attribute filters with list conditions
    def test_generate_event_selector_query_attribute_lists(self) -> None:
        my_events = EventSelector(
            event_type=EventType.COMPARTMENT_LEVEL_2_START,
            event_conditions_dict={
                "compartment_level_1": ["XXX"],
                "start_reason": ["YYY"],
            },
        )
        query_string = my_events.generate_event_selector_query()
        expected_query_string = """
SELECT *, end_date AS end_date_exclusive
FROM `{project_id}.analyst_data.person_events_materialized`
WHERE event = "COMPARTMENT_LEVEL_2_START"
        AND JSON_EXTRACT_SCALAR(event_attributes, "$.compartment_level_1") IN ("XXX")
        AND JSON_EXTRACT_SCALAR(event_attributes, "$.start_reason") IN ("YYY")
"""
        self.assertEqual(query_string, expected_query_string)

    # Check that generate_event_selector_query returns intended output for the officer level population query
    def test_generate_event_selector_query_attribute_lists_officer(self) -> None:
        my_events = EventSelector(
            event_type=EventType.WORKFLOWS_USER_CLIENT_STATUS_UPDATE,
            event_conditions_dict={
                "violation_type": ["TECHNICAL"],
            },
        )
        query_string = my_events.generate_event_selector_query()
        expected_query_string = """
SELECT *, end_date AS end_date_exclusive
FROM `{project_id}.analyst_data.workflows_user_events_materialized`
WHERE event = "WORKFLOWS_USER_CLIENT_STATUS_UPDATE"
        AND JSON_EXTRACT_SCALAR(event_attributes, "$.violation_type") IN ("TECHNICAL")
"""
        self.assertEqual(query_string, expected_query_string)

    # Check that generate_event_selector_query returns intended output for attribute filters with string conditions
    def test_generate_event_selector_query_strings(self) -> None:
        my_events = EventSelector(
            event_type=EventType.SENTENCES_IMPOSED,
            event_conditions_dict={
                "most_severe_description": 'LIKE "%DRUG%"',
                "any_is_violent_uniform": 'IN ("False")',
            },
        )
        query_string = my_events.generate_event_selector_query()
        expected_query_string = """
SELECT *, end_date AS end_date_exclusive
FROM `{project_id}.analyst_data.person_events_materialized`
WHERE event = "SENTENCES_IMPOSED"
        AND JSON_EXTRACT_SCALAR(event_attributes, "$.most_severe_description") LIKE "%DRUG%"
        AND JSON_EXTRACT_SCALAR(event_attributes, "$.any_is_violent_uniform") IN ("False")
"""
        self.assertEqual(query_string, expected_query_string)

    # Test query construction for events with empty attribute filter dicts
    def test_generate_event_selector_query_empty_attribute_filters(self) -> None:
        my_events = EventSelector(
            event_type=EventType.SENTENCES_IMPOSED,
            event_conditions_dict={},
        )
        query_string = my_events.generate_event_selector_query()

        expected_query_string = """
SELECT *, end_date AS end_date_exclusive
FROM `{project_id}.analyst_data.person_events_materialized`
WHERE event = "SENTENCES_IMPOSED"
"""
        self.assertEqual(query_string, expected_query_string)
