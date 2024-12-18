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
"""Tests the `aggregate_adjacent_spans` function in session_query_fragments.py"""
from datetime import date
from typing import Any, Dict, List

from google.cloud import bigquery

from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

PRE_AGGREGATED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id="sessions",
    view_id="pre_aggregated_table",
    view_query_template="SELECT * FROM `{project_id}.test_dataset.foo`;",
    description="Dataset of pre-aggregated spans",
)


class TestAggregateAdjacentSpansQueryHelper(BigQueryEmulatorTestCase):
    """Tests for the aggregate_adjacent_spans_function"""

    def _load_data_to_pre_aggregated_view(
        self,
        data: List[Dict[str, Any]],
    ) -> None:
        self.create_mock_table(
            address=PRE_AGGREGATED_VIEW_BUILDER.table_for_query,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("start_date", date),
                schema_field_for_type("end_date", date),
                schema_field_for_type("end_date_exclusive", date),
                schema_field_for_type("string_attribute", str),
                schema_field_for_type("int_attribute", int),
                bigquery.SchemaField(
                    "struct_attribute",
                    field_type=bigquery.enums.StandardSqlTypeNames.JSON.value,
                    mode="NULLABLE",
                ),
            ],
        )

        self.load_rows_into_table(
            address=PRE_AGGREGATED_VIEW_BUILDER.table_for_query,
            data=data,
        )

    def test_simple_string_attribute_aggregation(self) -> None:
        """
        Tests basic aggregation based on a string attribute to see that both attribute change and date gap between
        sessions triggers a new session. Also checks that the date_gap_id and session_id are incremented
        as we would expect.
        """
        self._load_data_to_pre_aggregated_view(
            data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 2, 1),
                    "string_attribute": "A",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 1),
                    "end_date_exclusive": date(2024, 3, 1),
                    "string_attribute": "A",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date_exclusive": date(2024, 4, 1),
                    "string_attribute": "B",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 5, 1),
                    "end_date_exclusive": None,
                    "string_attribute": "B",
                },
            ],
        )

        self.run_query_test(
            query_str=aggregate_adjacent_spans(
                table_name=PRE_AGGREGATED_VIEW_BUILDER.address.to_str(),
                attribute="string_attribute",
                end_date_field_name="end_date_exclusive",
            ),
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 3, 1),
                    "session_id": 1,
                    "date_gap_id": 1,
                    "string_attribute": "A",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date_exclusive": date(2024, 4, 1),
                    "session_id": 2,
                    "date_gap_id": 1,
                    "string_attribute": "B",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 5, 1),
                    "end_date_exclusive": None,
                    "session_id": 3,
                    "date_gap_id": 2,
                    "string_attribute": "B",
                },
            ],
        )

    def test_aggregation_with_no_attribute(self) -> None:
        """
        Uses the same data as above but without a call to an "attribute". Tests that spans are aggregated based on
        temporal adjacency and that attribute is not included in the output.
        """
        self._load_data_to_pre_aggregated_view(
            data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 2, 1),
                    "string_attribute": "A",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 1),
                    "end_date_exclusive": date(2024, 3, 1),
                    "string_attribute": "A",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date_exclusive": date(2024, 4, 1),
                    "string_attribute": "B",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 5, 1),
                    "end_date_exclusive": None,
                    "string_attribute": "B",
                },
            ],
        )

        self.run_query_test(
            query_str=aggregate_adjacent_spans(
                table_name=PRE_AGGREGATED_VIEW_BUILDER.address.to_str(),
                end_date_field_name="end_date_exclusive",
            ),
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 4, 1),
                    "session_id": 1,
                    "date_gap_id": 1,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 5, 1),
                    "end_date_exclusive": None,
                    "session_id": 2,
                    "date_gap_id": 2,
                },
            ],
        )

    def test_aggregation_with_nulls(self) -> None:
        """
        Test that spans can be aggregated with multiple attributes and that null values are aggregated as we would expect
        """
        self._load_data_to_pre_aggregated_view(
            data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 2, 1),
                    "string_attribute": None,
                    "int_attribute": 1,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 1),
                    "end_date_exclusive": date(2024, 3, 1),
                    "string_attribute": None,
                    "int_attribute": 1,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date_exclusive": date(2024, 4, 1),
                    "string_attribute": None,
                    "int_attribute": None,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 1),
                    "end_date_exclusive": date(2024, 5, 1),
                    "string_attribute": None,
                    "int_attribute": None,
                },
            ],
        )

        self.run_query_test(
            query_str=aggregate_adjacent_spans(
                table_name=PRE_AGGREGATED_VIEW_BUILDER.address.to_str(),
                end_date_field_name="end_date_exclusive",
                attribute=["string_attribute", "int_attribute"],
            ),
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 3, 1),
                    "string_attribute": None,
                    "int_attribute": 1,
                    "session_id": 1,
                    "date_gap_id": 1,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date_exclusive": date(2024, 5, 1),
                    "string_attribute": None,
                    "int_attribute": None,
                    "session_id": 2,
                    "date_gap_id": 1,
                },
            ],
        )

    def test_aggregation_with_non_default_index_and_non_default_id(self) -> None:
        """
        The index columns default to person_id and state_code. This checks that the function works as expected
        when we specify that the index is just state_code.
        """
        self._load_data_to_pre_aggregated_view(
            data=[
                {
                    "state_code": "US_XX",
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 2, 1),
                },
                {
                    "state_code": "US_XX",
                    "start_date": date(2024, 2, 1),
                    "end_date_exclusive": date(2024, 3, 1),
                },
            ],
        )

        self.run_query_test(
            query_str=aggregate_adjacent_spans(
                table_name=PRE_AGGREGATED_VIEW_BUILDER.address.to_str(),
                index_columns=["state_code"],
                session_id_output_name="state_session_id",
                end_date_field_name="end_date_exclusive",
            ),
            expected_result=[
                {
                    "state_code": "US_XX",
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 3, 1),
                    "state_session_id": 1,
                    "date_gap_id": 1,
                },
            ],
        )

    def test_aggregation_with_default_end_date_name(self) -> None:
        """
        The function assumes the end date field is "end_date". This test confirms that that works as expected.
        """
        self._load_data_to_pre_aggregated_view(
            data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 1),
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 1),
                    "end_date": date(2024, 3, 1),
                },
            ],
        )

        self.run_query_test(
            query_str=aggregate_adjacent_spans(
                table_name=PRE_AGGREGATED_VIEW_BUILDER.address.to_str(),
            ),
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 3, 1),
                    "session_id": 1,
                    "date_gap_id": 1,
                },
            ],
        )

    def test_aggregation_with_struct(self) -> None:
        """
        Tests aggregation based on a column that represents a struct
        """
        self._load_data_to_pre_aggregated_view(
            data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 2, 1),
                    "struct_attribute": {"status": "A"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 1),
                    "end_date_exclusive": date(2024, 3, 1),
                    "struct_attribute": {"status": "A"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date_exclusive": date(2024, 4, 1),
                    "struct_attribute": {"status": "B"},
                },
            ],
        )

        self.run_query_test(
            query_str=aggregate_adjacent_spans(
                table_name=PRE_AGGREGATED_VIEW_BUILDER.address.to_str(),
                attribute="struct_attribute",
                struct_attribute_subset="struct_attribute",
                end_date_field_name="end_date_exclusive",
            ),
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 3, 1),
                    "struct_attribute": {"status": "A"},
                    "session_id": 1,
                    "date_gap_id": 1,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date_exclusive": date(2024, 4, 1),
                    "struct_attribute": {"status": "B"},
                    "session_id": 2,
                    "date_gap_id": 1,
                },
            ],
        )

    def test_aggregation_with_struct_and_non_struct(self) -> None:
        """
        Tests aggregation based on multiple columns, one of which represents a struct
        """
        self._load_data_to_pre_aggregated_view(
            data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 2, 1),
                    "int_attribute": 1,
                    "struct_attribute": {"status": "A"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 1),
                    "end_date_exclusive": date(2024, 3, 1),
                    "int_attribute": 1,
                    "struct_attribute": {"status": "A"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date_exclusive": date(2024, 4, 1),
                    "int_attribute": 1,
                    "struct_attribute": {"status": "B"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 1),
                    "end_date_exclusive": date(2024, 5, 1),
                    "int_attribute": 2,
                    "struct_attribute": {"status": "B"},
                },
            ],
        )

        self.run_query_test(
            query_str=aggregate_adjacent_spans(
                table_name=PRE_AGGREGATED_VIEW_BUILDER.address.to_str(),
                attribute=["struct_attribute", "int_attribute"],
                struct_attribute_subset="struct_attribute",
                end_date_field_name="end_date_exclusive",
            ),
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 3, 1),
                    "int_attribute": 1,
                    "struct_attribute": {"status": "A"},
                    "session_id": 1,
                    "date_gap_id": 1,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date_exclusive": date(2024, 4, 1),
                    "int_attribute": 1,
                    "struct_attribute": {"status": "B"},
                    "session_id": 2,
                    "date_gap_id": 1,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 1),
                    "end_date_exclusive": date(2024, 5, 1),
                    "int_attribute": 2,
                    "struct_attribute": {"status": "B"},
                    "session_id": 3,
                    "date_gap_id": 1,
                },
            ],
        )
