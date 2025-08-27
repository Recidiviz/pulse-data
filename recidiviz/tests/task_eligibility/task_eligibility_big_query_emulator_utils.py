# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Util methods for Task Eligibility Big Query Emulator Tests."""
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateAgnosticTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

# Simple test objects used to initialize the emulator views
TEST_POPULATION_BUILDER = StateAgnosticTaskCandidatePopulationBigQueryViewBuilder(
    population_name="SIMPLE_POPULATION",
    population_spans_query_template="SELECT * FROM `{project_id}.test_dataset.foo`;",
    description="Simple population description",
)


TEST_COMPLETION_EVENT_BUILDER = StateAgnosticTaskCompletionEventBigQueryViewBuilder(
    completion_event_type=TaskCompletionEventType.EARLY_DISCHARGE,
    completion_event_query_template="SELECT * FROM `{project_id}.test_dataset.foo`;",
    description="Simple event description",
)


def load_data_for_task_criteria_view(
    emulator: BigQueryEmulatorTestCase,
    criteria_view_builder: TaskCriteriaBigQueryViewBuilder,
    criteria_data: List[Dict[str, Any]],
) -> None:
    emulator.create_mock_table(
        address=criteria_view_builder.table_for_query,
        schema=[
            schema_field_for_type("state_code", str),
            schema_field_for_type("person_id", int),
            schema_field_for_type("start_date", date),
            schema_field_for_type("end_date", date),
            schema_field_for_type("meets_criteria", bool),
            bigquery.SchemaField(
                "reason",
                field_type=bigquery.enums.StandardSqlTypeNames.JSON.value,
                mode="NULLABLE",
            ),
            bigquery.SchemaField(
                "reason_v2",
                field_type=bigquery.enums.StandardSqlTypeNames.JSON.value,
                mode="NULLABLE",
            ),
        ],
    )
    emulator.load_rows_into_table(
        address=criteria_view_builder.table_for_query,
        data=criteria_data,
    )


def load_data_for_candidate_population_view(
    emulator: BigQueryEmulatorTestCase,
    population_date_spans: List[Tuple[date, Optional[date]]],
    test_person_id: int = 12345,
    create_table: bool = True,
) -> None:
    """Helper method for loading the mock candidate population view for the test person from state US_XX"""
    if create_table:
        emulator.create_mock_table(
            address=TEST_POPULATION_BUILDER.table_for_query,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("start_date", date),
                schema_field_for_type("end_date", date),
            ],
        )
    emulator.load_rows_into_table(
        address=TEST_POPULATION_BUILDER.table_for_query,
        data=[
            {
                "state_code": "US_XX",
                "person_id": test_person_id,
                "start_date": date_span[0],
                "end_date": date_span[1],
            }
            for date_span in population_date_spans
        ],
    )


def load_data_for_task_completion_event_view(
    emulator: BigQueryEmulatorTestCase,
    completion_event_view_builder: StateAgnosticTaskCompletionEventBigQueryViewBuilder,
    task_completion_data: List[Dict[str, Any]],
) -> None:
    emulator.create_mock_table(
        address=completion_event_view_builder.table_for_query,
        schema=[
            schema_field_for_type("state_code", str),
            schema_field_for_type("person_id", int),
            schema_field_for_type("completion_event_date", date),
        ],
    )
    emulator.load_rows_into_table(
        address=completion_event_view_builder.table_for_query,
        data=task_completion_data,
    )


def load_empty_task_completion_event_view(emulator: BigQueryEmulatorTestCase) -> None:
    """Helper method for loading the mock task completion event view with no rows"""
    load_data_for_task_completion_event_view(
        emulator=emulator,
        completion_event_view_builder=TEST_COMPLETION_EVENT_BUILDER,
        task_completion_data=[],
    )


def load_data_for_basic_task_eligibility_span_view(
    emulator: BigQueryEmulatorTestCase,
    basic_eligibility_address: BigQueryAddress,
    basic_eligibility_data: List[Dict[str, Any]],
) -> None:
    emulator.create_mock_table(
        address=basic_eligibility_address,
        schema=[
            schema_field_for_type("state_code", str),
            schema_field_for_type("person_id", int),
            schema_field_for_type("start_date", date),
            schema_field_for_type("end_date", date),
            schema_field_for_type("is_eligible", bool),
            bigquery.SchemaField(
                "reasons",
                field_type=bigquery.enums.StandardSqlTypeNames.JSON.value,
                mode="NULLABLE",
            ),
            bigquery.SchemaField(
                "reasons_v2",
                field_type=bigquery.enums.StandardSqlTypeNames.JSON.value,
                mode="NULLABLE",
            ),
            schema_field_for_type("ineligible_criteria", list),
        ],
    )
    emulator.load_rows_into_table(
        address=basic_eligibility_address,
        data=basic_eligibility_data,
    )


def load_data_for_almost_eligible_span_view(
    emulator: BigQueryEmulatorTestCase,
    almost_eligible_address: BigQueryAddress,
    almost_eligible_data: List[Dict[str, Any]],
) -> None:
    emulator.create_mock_table(
        address=almost_eligible_address,
        schema=[
            schema_field_for_type("state_code", str),
            schema_field_for_type("person_id", int),
            schema_field_for_type("start_date", date),
            schema_field_for_type("end_date", date),
            schema_field_for_type("is_almost_eligible", bool),
        ],
    )
    emulator.load_rows_into_table(
        address=almost_eligible_address,
        data=almost_eligible_data,
    )
