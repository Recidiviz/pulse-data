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
"""Tests the `sessionize_submitted_start_and_end_events` helper function"""
from datetime import date, datetime
from typing import Any, Dict, List

from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.analyst_data.all_task_type_marked_submitted_spans import (
    sessionize_submitted_start_and_end_events,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

START_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id="analyst_data",
    view_id="start_events",
    view_query_template="SELECT * FROM `{project_id}.test_dataset.foo`;",
    description="Testing dataset of start events",
)
END_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id="analyst_data",
    view_id="end_events",
    view_query_template="SELECT * FROM `{project_id}.test_dataset.foo`;",
    description="Testing dataset of end events",
)


class TestSessionizeStartEndEvents(BigQueryEmulatorTestCase):
    """Tests for the sessionize_submitted_start_and_end_events helper function
    for assembling the Marked Submitted spans view"""

    def _load_data(
        self,
        start_data: List[Dict[str, Any]],
        end_data: List[Dict[str, Any]],
    ) -> None:
        self.create_mock_table(
            address=START_VIEW_BUILDER.table_for_query,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("opportunity_type", str),
                # TODO(#36803) Use datetime
                schema_field_for_type("timestamp", date),
            ],
        )
        self.create_mock_table(
            address=END_VIEW_BUILDER.table_for_query,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("opportunity_type", str),
                schema_field_for_type("timestamp", date),
            ],
        )
        self.load_rows_into_table(
            address=START_VIEW_BUILDER.table_for_query,
            data=start_data,
        )
        self.load_rows_into_table(
            address=END_VIEW_BUILDER.table_for_query,
            data=end_data,
        )

    def test_start_with_no_end(self) -> None:
        """
        1 person with a start event but no end event,
        which should be replaced with null in the result
        """
        self._load_data(
            start_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 1, 1),
                },
            ],
            end_data=[],
        )

        self.run_query_test(
            query_str=sessionize_submitted_start_and_end_events(
                start_event_dataset=START_VIEW_BUILDER.address.to_str(),
                end_event_dataset=END_VIEW_BUILDER.address.to_str(),
            ),
            expected_result=[
                {
                    "person_id": 1,
                    "state_code": "US_XX",
                    "opportunity_type": "us_xx_opportunity",
                    "start_date": datetime(2024, 1, 1),
                    "end_date": None,
                },
            ],
        )

    def test_multiple_starts_and_ends(self) -> None:
        """
        1 person with 4 start-end spans (4 events of each type, alternating)
        """
        self._load_data(
            start_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 1, 1),
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 3, 3),
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 5, 5),
                },
            ],
            end_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 2, 2),
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 4, 4),
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 6, 6),
                },
            ],
        )

        self.run_query_test(
            query_str=sessionize_submitted_start_and_end_events(
                start_event_dataset=START_VIEW_BUILDER.address.to_str(),
                end_event_dataset=END_VIEW_BUILDER.address.to_str(),
            ),
            expected_result=[
                {
                    "person_id": 1,
                    "state_code": "US_XX",
                    "opportunity_type": "us_xx_opportunity",
                    "start_date": datetime(2024, 1, 1),
                    "end_date": datetime(2024, 2, 2),
                },
                {
                    "person_id": 1,
                    "state_code": "US_XX",
                    "opportunity_type": "us_xx_opportunity",
                    "start_date": datetime(2024, 3, 3),
                    "end_date": datetime(2024, 4, 4),
                },
                {
                    "person_id": 1,
                    "state_code": "US_XX",
                    "opportunity_type": "us_xx_opportunity",
                    "start_date": datetime(2024, 5, 5),
                    "end_date": datetime(2024, 6, 6),
                },
            ],
        )

    def test_multiple_starts_with_no_end(self) -> None:
        """
        1 person with 3 start-end spans and one start event with no corresponding end
        """

        self._load_data(
            start_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 1, 1),
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 3, 3),
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 5, 5),
                },
            ],
            end_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 2, 2),
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 4, 4),
                },
            ],
        )

        self.run_query_test(
            query_str=sessionize_submitted_start_and_end_events(
                start_event_dataset=START_VIEW_BUILDER.address.to_str(),
                end_event_dataset=END_VIEW_BUILDER.address.to_str(),
            ),
            expected_result=[
                {
                    "person_id": 1,
                    "state_code": "US_XX",
                    "opportunity_type": "us_xx_opportunity",
                    "start_date": datetime(2024, 1, 1),
                    "end_date": datetime(2024, 2, 2),
                },
                {
                    "person_id": 1,
                    "state_code": "US_XX",
                    "opportunity_type": "us_xx_opportunity",
                    "start_date": datetime(2024, 3, 3),
                    "end_date": datetime(2024, 4, 4),
                },
                {
                    "person_id": 1,
                    "state_code": "US_XX",
                    "opportunity_type": "us_xx_opportunity",
                    "start_date": datetime(2024, 5, 5),
                    "end_date": None,
                },
            ],
        )

    def test_one_person_many_opportunities(self) -> None:
        """
        1 person with start and end events for 3 opportunities
        """
        self._load_data(
            start_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity_1",
                    "timestamp": date(2024, 1, 1),
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity_2",
                    "timestamp": date(2024, 2, 2),
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity_3",
                    "timestamp": date(2024, 3, 3),
                },
            ],
            end_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity_1",
                    "timestamp": date(2024, 7, 7),
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity_2",
                    "timestamp": date(2024, 8, 8),
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity_3",
                    "timestamp": date(2024, 9, 9),
                },
            ],
        )

        self.run_query_test(
            query_str=sessionize_submitted_start_and_end_events(
                start_event_dataset=START_VIEW_BUILDER.address.to_str(),
                end_event_dataset=END_VIEW_BUILDER.address.to_str(),
            ),
            expected_result=[
                {
                    "person_id": 1,
                    "state_code": "US_XX",
                    "opportunity_type": "us_xx_opportunity_1",
                    "start_date": datetime(2024, 1, 1),
                    "end_date": datetime(2024, 7, 7),
                },
                {
                    "person_id": 1,
                    "state_code": "US_XX",
                    "opportunity_type": "us_xx_opportunity_2",
                    "start_date": datetime(2024, 2, 2),
                    "end_date": datetime(2024, 8, 8),
                },
                {
                    "person_id": 1,
                    "state_code": "US_XX",
                    "opportunity_type": "us_xx_opportunity_3",
                    "start_date": datetime(2024, 3, 3),
                    "end_date": datetime(2024, 9, 9),
                },
            ],
        )

    def test_many_people_one_opportunity(self) -> None:
        """
        3 people, all of whom have a start and end event for the same opportunity
        """
        self._load_data(
            start_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 1, 1),
                },
                {
                    "state_code": "US_XX",
                    "person_id": 2,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 2, 2),
                },
                {
                    "state_code": "US_XX",
                    "person_id": 3,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 3, 3),
                },
            ],
            end_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 7, 7),
                },
                {
                    "state_code": "US_XX",
                    "person_id": 2,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 8, 8),
                },
                {
                    "state_code": "US_XX",
                    "person_id": 3,
                    "opportunity_type": "us_xx_opportunity",
                    "timestamp": date(2024, 9, 9),
                },
            ],
        )

        self.run_query_test(
            query_str=sessionize_submitted_start_and_end_events(
                start_event_dataset=START_VIEW_BUILDER.address.to_str(),
                end_event_dataset=END_VIEW_BUILDER.address.to_str(),
            ),
            expected_result=[
                {
                    "person_id": 1,
                    "state_code": "US_XX",
                    "opportunity_type": "us_xx_opportunity",
                    "start_date": datetime(2024, 1, 1),
                    "end_date": datetime(2024, 7, 7),
                },
                {
                    "person_id": 2,
                    "state_code": "US_XX",
                    "opportunity_type": "us_xx_opportunity",
                    "start_date": datetime(2024, 2, 2),
                    "end_date": datetime(2024, 8, 8),
                },
                {
                    "person_id": 3,
                    "state_code": "US_XX",
                    "opportunity_type": "us_xx_opportunity",
                    "start_date": datetime(2024, 3, 3),
                    "end_date": datetime(2024, 9, 9),
                },
            ],
        )
