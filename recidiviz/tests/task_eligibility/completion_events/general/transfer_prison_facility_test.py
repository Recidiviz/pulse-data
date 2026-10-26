# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests the TRANSFER_PRISON_FACILITY completion event."""
from datetime import date

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.sessions.housing_unit_sessions import (
    HOUSING_UNIT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.task_eligibility.completion_events.general.transfer_prison_facility import (
    VIEW_BUILDER,
)
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
    TableRow,
)


class TransferPrisonFacilityTest(SimpleBigQueryViewBuilderTestCase):
    """Tests the TRANSFER_PRISON_FACILITY completion event."""

    housing_unit_sessions_address = HOUSING_UNIT_SESSIONS_VIEW_BUILDER.table_for_query

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return VIEW_BUILDER

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        return {
            self.housing_unit_sessions_address: [
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("start_date", date),
                schema_field_for_type("end_date_exclusive", date),
                schema_field_for_type("housing_unit", str),
                schema_field_for_type("facility", str),
            ],
        }

    def _run(
        self, *, housing_unit_sessions: list[TableRow], expected_events: list[TableRow]
    ) -> None:
        self.run_simple_view_builder_query_test_from_data(
            input_data={self.housing_unit_sessions_address: housing_unit_sessions},
            expected_result=expected_events,
            enforce_order=False,
        )

    def test_move_between_facilities_is_a_transfer(self) -> None:
        self._run(
            housing_unit_sessions=[
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "start_date": date(2024, 1, 5),
                    "end_date_exclusive": date(2024, 3, 10),
                    "facility": "FACILITY_A",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "start_date": date(2024, 3, 10),
                    "end_date_exclusive": None,
                    "facility": "FACILITY_B",
                },
            ],
            expected_events=[
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "completion_event_date": date(2024, 3, 10),
                },
            ],
        )

    def test_first_incarceration_session_is_not_a_transfer(self) -> None:
        self._run(
            housing_unit_sessions=[
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "start_date": date(2024, 3, 10),
                    "end_date_exclusive": None,
                    "facility": "FACILITY_A",
                },
            ],
            expected_events=[],
        )

    def test_incarceration_after_a_date_gap_is_not_a_transfer(self) -> None:
        self._run(
            housing_unit_sessions=[
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "start_date": date(2024, 1, 5),
                    "end_date_exclusive": date(2024, 3, 10),
                    "facility": "FACILITY_A",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "start_date": date(2024, 4, 1),
                    "end_date_exclusive": None,
                    "facility": "FACILITY_B",
                },
            ],
            expected_events=[],
        )

    def test_housing_unit_change_within_the_same_facility_is_not_a_transfer(
        self,
    ) -> None:
        self._run(
            housing_unit_sessions=[
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "start_date": date(2024, 1, 5),
                    "end_date_exclusive": date(2024, 3, 10),
                    "housing_unit": "UNIT_1",
                    "facility": "FACILITY_A",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "start_date": date(2024, 3, 10),
                    "end_date_exclusive": None,
                    "housing_unit": "UNIT_2",
                    "facility": "FACILITY_A",
                },
            ],
            expected_events=[],
        )

    def test_moves_next_to_a_missing_facility_are_not_transfers(self) -> None:
        self._run(
            housing_unit_sessions=[
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "start_date": date(2024, 1, 5),
                    "end_date_exclusive": date(2024, 3, 10),
                    "facility": "FACILITY_A",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "start_date": date(2024, 3, 10),
                    "end_date_exclusive": date(2024, 3, 20),
                    "facility": None,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "start_date": date(2024, 3, 20),
                    "end_date_exclusive": None,
                    "facility": "FACILITY_B",
                },
            ],
            expected_events=[],
        )

    def test_each_person_is_compared_only_with_their_own_sessions(self) -> None:
        self._run(
            housing_unit_sessions=[
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "start_date": date(2024, 1, 5),
                    "end_date_exclusive": date(2024, 3, 10),
                    "facility": "FACILITY_A",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 2,
                    "start_date": date(2024, 3, 10),
                    "end_date_exclusive": None,
                    "facility": "FACILITY_B",
                },
            ],
            expected_events=[],
        )
