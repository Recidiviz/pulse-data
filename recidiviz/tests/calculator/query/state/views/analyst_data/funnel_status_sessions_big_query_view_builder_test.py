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
"""Tests for funnel_status_sessions_big_query_view_builder.py"""

import datetime
import unittest
from typing import Any

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.state.views.analyst_data.funnel_status_sessions_big_query_view_builder import (
    FunnelStatusEventQueryBuilder,
    FunnelStatusSessionsViewBuilder,
    FunnelStatusSpanQueryBuilder,
)
from recidiviz.tests.big_query.big_query_client_test import BigQueryEmulatorTestCase

MY_DUMMY_SPAN_FUNNEL_QUERY_1 = FunnelStatusSpanQueryBuilder(
    sql_source="SELECT * FROM `my_sessions.my_span_view_materialized`",
    start_date_col="my_start_date",
    end_date_exclusive_col="my_end_date_exclusive",
    index_cols=["person_id", "id_type"],
    status_cols_by_type=[
        ("is_incarcerated", bigquery.StandardSqlTypeNames.BOOL),
        ("has_diploma", bigquery.StandardSqlTypeNames.BOOL),
        ("custody_level", bigquery.StandardSqlTypeNames.STRING),
    ],
)

MY_DUMMY_SPAN_FUNNEL_QUERY_2 = FunnelStatusSpanQueryBuilder(
    sql_source="SELECT * FROM `my_sessions.my_span_view_materialized`",
    start_date_col="my_start_date",
    end_date_exclusive_col="my_end_date_exclusive",
    index_cols=["person_id", "id_type"],
    status_cols_by_type=[
        ("is_incarcerated", bigquery.StandardSqlTypeNames.BOOL),
        ("has_diploma", bigquery.StandardSqlTypeNames.BOOL),
        ("custody_level", bigquery.StandardSqlTypeNames.STRING),
    ],
    truncate_spans_at_reset_dates=False,
)

MY_DUMMY_EVENT_FUNNEL_QUERY_1 = FunnelStatusEventQueryBuilder(
    sql_source="SELECT * FROM `my_sessions.my_event_view_materialized`",
    event_date_col="my_event_date",
    index_cols=["person_id", "id_type"],
    status_cols_by_type=[
        ("is_discretionary", bigquery.StandardSqlTypeNames.BOOL),
    ],
)

MY_DUMMY_FUNNEL_VIEW_BUILDER = FunnelStatusSessionsViewBuilder(
    view_id="my_funnel",
    description="My funnel view",
    index_cols=["person_id", "id_type"],
    funnel_reset_dates_sql_source="SELECT * FROM `{project_id}.sessions.my_reset_dates_materialized`",
    funnel_status_query_builders=[
        MY_DUMMY_SPAN_FUNNEL_QUERY_1,
        MY_DUMMY_EVENT_FUNNEL_QUERY_1,
    ],
)


class TestFunnelStatusSpanQuery(unittest.TestCase):
    """Tests functionality of FunnelStatusSpanQuery class"""

    def test_status_cols(self) -> None:
        my_funnel_span_query = FunnelStatusSpanQueryBuilder(
            sql_source="SELECT * FROM `my_sessions.my_span_view_materialized`",
            start_date_col="my_start_date",
            end_date_exclusive_col="my_end_date_exclusive",
            index_cols=["person_id", "id_type"],
            status_cols_by_type=[
                ("is_incarcerated", bigquery.StandardSqlTypeNames.BOOL),
                ("has_diploma", bigquery.StandardSqlTypeNames.BOOL),
                ("custody_level", bigquery.StandardSqlTypeNames.STRING),
            ],
        )
        expected_status_cols = ["is_incarcerated", "has_diploma", "custody_level"]
        self.assertEqual(expected_status_cols, my_funnel_span_query.status_cols)

    def test_build_query(self) -> None:
        my_funnel_span_query = FunnelStatusSpanQueryBuilder(
            sql_source="SELECT * FROM `my_sessions.my_span_view_materialized`",
            start_date_col="my_start_date",
            end_date_exclusive_col="my_end_date_exclusive",
            index_cols=["person_id", "id_type"],
            status_cols_by_type=[
                ("is_incarcerated", bigquery.StandardSqlTypeNames.BOOL),
                ("has_diploma", bigquery.StandardSqlTypeNames.BOOL),
                ("custody_level", bigquery.StandardSqlTypeNames.STRING),
            ],
        )
        my_truncate_dates_sql_query = "SELECT person_id, id_type, release_date AS funnel_reset_date FROM `my_sessions.my_release dates`"
        actual_query_template = my_funnel_span_query.build_query(
            index_cols=["person_id", "id_type"],
            funnel_reset_dates_sql_source=my_truncate_dates_sql_query,
        )
        expected_query_template = f"""
    SELECT
        -- Index columns
        spans.id_type, spans.person_id,
        spans.my_start_date AS start_date,
        {revert_nonnull_end_date_clause(
            f'LEAST({nonnull_end_date_clause("spans.my_end_date_exclusive")}, {nonnull_end_date_clause("reset_dates.funnel_reset_date")})'
        )} AS end_date_exclusive,
        -- Status columns
        spans.is_incarcerated, spans.has_diploma, spans.custody_level
    FROM
        (SELECT * FROM `my_sessions.my_span_view_materialized`) spans
    LEFT JOIN
        (SELECT person_id, id_type, release_date AS funnel_reset_date FROM `my_sessions.my_release dates`) reset_dates
    ON
        spans.id_type = reset_dates.id_type
        AND spans.person_id = reset_dates.person_id
        AND reset_dates.funnel_reset_date > spans.my_start_date
    -- Use the first funnel reset date following the span start as the end date of the session
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_type, person_id, is_incarcerated, has_diploma, custody_level, start_date 
        ORDER BY COALESCE(end_date_exclusive, "9999-01-01")
    ) = 1"""
        self.assertEqual(expected_query_template, actual_query_template)

    def test_build_query_no_allowed_truncation(self) -> None:
        my_truncate_dates_sql_query = "SELECT person_id, id_type, release_date AS funnel_reset_date FROM `my_sessions.my_release dates`"
        actual_query_template = MY_DUMMY_SPAN_FUNNEL_QUERY_2.build_query(
            index_cols=["person_id", "id_type"],
            funnel_reset_dates_sql_source=my_truncate_dates_sql_query,
        )
        expected_query_template = """
    SELECT
        -- Index columns
        spans.id_type, spans.person_id,
        spans.my_start_date AS start_date,
        spans.my_end_date_exclusive AS end_date_exclusive,
        -- Status columns
        spans.is_incarcerated, spans.has_diploma, spans.custody_level
    FROM
        (SELECT * FROM `my_sessions.my_span_view_materialized`) spans"""
        self.assertEqual(expected_query_template, actual_query_template)

    def test_build_query_empty_status_cols(self) -> None:
        my_funnel_span_query = FunnelStatusSpanQueryBuilder(
            sql_source="SELECT * FROM `my_sessions.my_span_view_materialized`",
            start_date_col="my_start_date",
            end_date_exclusive_col="my_end_date_exclusive",
            index_cols=["person_id", "id_type"],
            status_cols_by_type=[
                ("custody_level", bigquery.StandardSqlTypeNames.STRING),
            ],
        )
        my_truncate_dates_sql_query = "SELECT person_id, id_type, release_date AS funnel_reset_date FROM `my_sessions.my_release dates`"
        actual_query_template = my_funnel_span_query.build_query(
            index_cols=["person_id", "id_type"],
            funnel_reset_dates_sql_source=my_truncate_dates_sql_query,
        )
        expected_query_template = f"""
    SELECT
        -- Index columns
        spans.id_type, spans.person_id,
        spans.my_start_date AS start_date,
        {revert_nonnull_end_date_clause(
            f'LEAST({nonnull_end_date_clause("spans.my_end_date_exclusive")}, {nonnull_end_date_clause("reset_dates.funnel_reset_date")})'
        )} AS end_date_exclusive,
        -- Status columns
        spans.custody_level
    FROM
        (SELECT * FROM `my_sessions.my_span_view_materialized`) spans
    LEFT JOIN
        (SELECT person_id, id_type, release_date AS funnel_reset_date FROM `my_sessions.my_release dates`) reset_dates
    ON
        spans.id_type = reset_dates.id_type
        AND spans.person_id = reset_dates.person_id
        AND reset_dates.funnel_reset_date > spans.my_start_date
    -- Use the first funnel reset date following the span start as the end date of the session
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_type, person_id, custody_level, start_date 
        ORDER BY COALESCE(end_date_exclusive, "9999-01-01")
    ) = 1"""
        self.assertEqual(expected_query_template, actual_query_template)


class TestFunnelStatusEventQuery(unittest.TestCase):
    """Tests functionality of FunnelStatusEventQuery"""

    def test_build_query(self) -> None:
        my_funnel_event_query = FunnelStatusEventQueryBuilder(
            sql_source="SELECT * FROM `my_sessions.my_event_view_materialized`",
            event_date_col="my_start_date",
            index_cols=["person_id", "task_type"],
            status_cols_by_type=[
                ("is_eligible", bigquery.StandardSqlTypeNames.BOOL),
            ],
        )
        my_truncate_dates_sql_query = "SELECT person_id, task_type, release_date AS funnel_reset_date FROM `my_sessions.my_release dates`"
        actual_query_template = my_funnel_event_query.build_query(
            index_cols=["person_id", "task_type"],
            funnel_reset_dates_sql_source=my_truncate_dates_sql_query,
        )
        expected_query_template = """
    SELECT
        -- Index columns
        events.person_id, events.task_type,
        events.my_start_date AS start_date,
        reset_dates.funnel_reset_date AS end_date_exclusive,
        -- Status columns
        events.is_eligible
    FROM
        (SELECT * FROM `my_sessions.my_event_view_materialized`) events
    LEFT JOIN
        (SELECT person_id, task_type, release_date AS funnel_reset_date FROM `my_sessions.my_release dates`) reset_dates
    ON
        events.person_id = reset_dates.person_id
        AND events.task_type = reset_dates.task_type
        AND reset_dates.funnel_reset_date > events.my_start_date
    -- Use the first funnel reset date following the event as the end date of the session
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, task_type, is_eligible, start_date 
        ORDER BY COALESCE(end_date_exclusive, "9999-01-01")
    ) = 1"""
        self.assertEqual(expected_query_template, actual_query_template)


class TestFunnelStatusSessionsBigQueryViewBuilder(unittest.TestCase):
    """Tests functionality of FunnelStatusSessionsBigQueryViewBuilder class"""

    def test_get_funnel_status_query_fragment_with_common_schema(self) -> None:
        actual_query = MY_DUMMY_FUNNEL_VIEW_BUILDER.get_funnel_status_query_fragment_with_common_schema(
            MY_DUMMY_SPAN_FUNNEL_QUERY_1
        )
        expected_query = f"""
SELECT
	id_type, person_id,
    start_date,
    end_date_exclusive,
    is_incarcerated,
    has_diploma,
    custody_level,
    NULL AS is_discretionary
FROM (
{MY_DUMMY_SPAN_FUNNEL_QUERY_1.build_query(["person_id", "id_type"], funnel_reset_dates_sql_source="SELECT * FROM `{project_id}.sessions.my_reset_dates_materialized`")}
)"""
        self.assertEqual(expected_query, actual_query)

    def test_mismatched_index_columns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r'All funnel status queries must have the same index columns. Expected: \["person_id", "id_type"\], found: \["person_id", "task_type"\] for item \[1\] in `funnel_status_query_builders`.',
        ):
            FunnelStatusSessionsViewBuilder(
                view_id="my_funnel",
                description="My funnel view",
                index_cols=["person_id", "id_type"],
                funnel_reset_dates_sql_source="SELECT * FROM `{project_id}.sessions.my_reset_dates_materialized`",
                funnel_status_query_builders=[
                    FunnelStatusSpanQueryBuilder(
                        sql_source="SELECT * FROM `my_sessions.my_span_view_materialized`",
                        start_date_col="my_start_date",
                        end_date_exclusive_col="my_end_date_exclusive",
                        index_cols=["person_id", "id_type"],
                        status_cols_by_type=[
                            ("is_incarcerated", bigquery.StandardSqlTypeNames.BOOL),
                            ("has_diploma", bigquery.StandardSqlTypeNames.BOOL),
                            ("custody_level", bigquery.StandardSqlTypeNames.STRING),
                        ],
                    ),
                    FunnelStatusEventQueryBuilder(
                        sql_source="SELECT * FROM `my_sessions.my_event_view_materialized`",
                        event_date_col="my_event_date",
                        index_cols=["person_id", "task_type"],
                        status_cols_by_type=[
                            ("is_discretionary", bigquery.StandardSqlTypeNames.BOOL),
                        ],
                    ),
                ],
            )

    def test_duplicate_status_column_names(self) -> None:
        with self.assertRaises(ValueError):
            FunnelStatusSessionsViewBuilder(
                view_id="my_funnel",
                description="My funnel view",
                index_cols=["person_id", "id_type"],
                funnel_reset_dates_sql_source="SELECT * FROM `{project_id}.sessions.my_reset_dates_materialized`",
                funnel_status_query_builders=[
                    FunnelStatusSpanQueryBuilder(
                        sql_source="SELECT * FROM `my_sessions.my_span_view_materialized`",
                        start_date_col="my_start_date",
                        end_date_exclusive_col="my_end_date_exclusive",
                        index_cols=["person_id", "id_type"],
                        status_cols_by_type=[
                            ("is_incarcerated", bigquery.StandardSqlTypeNames.BOOL),
                            ("has_diploma", bigquery.StandardSqlTypeNames.BOOL),
                            ("custody_level", bigquery.StandardSqlTypeNames.STRING),
                        ],
                    ),
                    FunnelStatusEventQueryBuilder(
                        sql_source="SELECT * FROM `my_sessions.my_event_view_materialized`",
                        event_date_col="my_event_date",
                        index_cols=["person_id", "task_type"],
                        status_cols_by_type=[
                            ("is_incarcerated", bigquery.StandardSqlTypeNames.BOOL),
                        ],
                    ),
                ],
            )


class TestFunnelStatusSessionsBigQueryViewBuilderEmulator(BigQueryEmulatorTestCase):
    """Emulator tests for FunnelStatusSessionsBigQueryViewBuilder"""

    def _create_funnel_reset_dates_table(
        self,
        *,
        data: list[dict[str, Any]],
    ) -> None:
        address = BigQueryAddress(dataset_id="my_sessions", table_id="my_reset_dates")
        self.create_mock_table(
            address=address,
            schema=[
                schema_field_for_type("person_id", int),
                schema_field_for_type("task_type", str),
                schema_field_for_type("funnel_reset_date", datetime.date),
            ],
        )
        self.load_rows_into_table(address=address, data=data)

    def _create_contact_events_table(
        self,
        *,
        data: list[dict[str, Any]],
    ) -> None:
        address = BigQueryAddress(dataset_id="my_sessions", table_id="contact_events")
        self.create_mock_table(
            address=address,
            schema=[
                schema_field_for_type("person_id", int),
                schema_field_for_type("task_type", str),
                schema_field_for_type("contact_date", datetime.date),
            ],
        )
        self.load_rows_into_table(address=address, data=data)

    def _create_usage_events_table(
        self,
        *,
        data: list[dict[str, Any]],
    ) -> None:
        address = BigQueryAddress(
            dataset_id="my_usage_events", table_id="segment_events"
        )
        self.create_mock_table(
            address=address,
            schema=[
                schema_field_for_type("person_id", int),
                schema_field_for_type("task_type", str),
                schema_field_for_type("event_date", datetime.date),
                schema_field_for_type("event", str),
            ],
        )
        self.load_rows_into_table(address=address, data=data)

    def _create_supervision_level_sessions_table(
        self,
        *,
        data: list[dict[str, Any]],
    ) -> None:
        address = BigQueryAddress(
            dataset_id="my_sessions", table_id="supervision_level_sessions"
        )
        self.create_mock_table(
            address=address,
            schema=[
                schema_field_for_type("person_id", int),
                schema_field_for_type("task_type", str),
                schema_field_for_type("start_date", datetime.date),
                schema_field_for_type("end_date_exclusive", datetime.date),
                schema_field_for_type("supervision_level", str),
            ],
        )
        self.load_rows_into_table(address=address, data=data)

    def _create_system_sessions_table(
        self,
        *,
        data: list[dict[str, Any]],
    ) -> None:
        address = BigQueryAddress(dataset_id="my_sessions", table_id="system_sessions")
        self.create_mock_table(
            address=address,
            schema=[
                schema_field_for_type("person_id", int),
                schema_field_for_type("task_type", str),
                schema_field_for_type("start_date", datetime.date),
                schema_field_for_type("end_date_exclusive", datetime.date),
            ],
        )
        self.load_rows_into_table(address=address, data=data)

    def test_build_event_only_funnel(self) -> None:
        builder = FunnelStatusSessionsViewBuilder(
            view_id="my_simple_funnel",
            description="My simple funnel",
            index_cols=["person_id", "task_type"],
            funnel_reset_dates_sql_source="SELECT * FROM `my_sessions.my_reset_dates`",
            funnel_status_query_builders=[
                FunnelStatusEventQueryBuilder(
                    sql_source="SELECT person_id, task_type, contact_date, TRUE AS received_contact FROM `my_sessions.contact_events`",
                    event_date_col="contact_date",
                    index_cols=["person_id", "task_type"],
                    status_cols_by_type=[
                        ("received_contact", bigquery.StandardSqlTypeNames.BOOL),
                    ],
                )
            ],
        )

        reset_dates_sql_source: list[dict[str, Any]] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_reset_date": datetime.date(2020, 1, 1),
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_reset_date": datetime.date(2023, 5, 1),
            },
        ]

        funnel_events_sql_source: list[dict[str, Any]] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "contact_date": datetime.date(2019, 12, 11),
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "contact_date": datetime.date(2023, 6, 1),
            },
        ]

        self._create_funnel_reset_dates_table(
            data=reset_dates_sql_source,
        )

        self._create_contact_events_table(
            data=funnel_events_sql_source,
        )

        expected_results: list[dict] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 1,
                "date_gap_id": 1,
                "start_date": datetime.date(2019, 12, 11),
                "end_date_exclusive": datetime.date(2020, 1, 1),
                "received_contact": True,
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 2,
                "date_gap_id": 2,
                "start_date": datetime.date(2023, 6, 1),
                "end_date_exclusive": None,
                "received_contact": True,
            },
        ]

        self.run_query_test(
            builder.build(sandbox_context=None).view_query, expected_results
        )

    def test_build_span_only_funnel(self) -> None:
        builder = FunnelStatusSessionsViewBuilder(
            view_id="my_simple_funnel_2",
            description="My simple funnel 2",
            index_cols=["person_id", "task_type"],
            funnel_reset_dates_sql_source="SELECT * FROM `my_sessions.my_reset_dates`",
            funnel_status_query_builders=[
                FunnelStatusSpanQueryBuilder(
                    sql_source="SELECT person_id, task_type, start_date, end_date_exclusive, supervision_level, FROM `my_sessions.supervision_level_sessions`",
                    start_date_col="start_date",
                    end_date_exclusive_col="end_date_exclusive",
                    index_cols=["person_id", "task_type"],
                    status_cols_by_type=[
                        ("supervision_level", bigquery.StandardSqlTypeNames.STRING),
                    ],
                )
            ],
        )

        reset_dates_sql_source: list[dict[str, Any]] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_reset_date": datetime.date(2020, 1, 1),
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_reset_date": datetime.date(2023, 5, 1),
            },
        ]

        funnel_spans_sql_source: list[dict[str, Any]] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "start_date": datetime.date(2019, 12, 11),
                "end_date_exclusive": datetime.date(2020, 4, 1),
                "supervision_level": "MINIMUM",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "start_date": datetime.date(2020, 4, 1),
                "end_date_exclusive": datetime.date(2022, 11, 15),
                "supervision_level": "MEDIUM",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "start_date": datetime.date(2022, 11, 15),
                "end_date_exclusive": datetime.date(2024, 3, 3),
                "supervision_level": "LIMITED",
            },
        ]

        self._create_funnel_reset_dates_table(
            data=reset_dates_sql_source,
        )

        self._create_supervision_level_sessions_table(
            data=funnel_spans_sql_source,
        )

        expected_results: list[dict] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 1,
                "date_gap_id": 1,
                "start_date": datetime.date(2019, 12, 11),
                "end_date_exclusive": datetime.date(2020, 1, 1),
                "supervision_level": "MINIMUM",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 2,
                "date_gap_id": 2,
                "start_date": datetime.date(2020, 4, 1),
                "end_date_exclusive": datetime.date(2022, 11, 15),
                "supervision_level": "MEDIUM",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 3,
                "date_gap_id": 2,
                "start_date": datetime.date(2022, 11, 15),
                "end_date_exclusive": datetime.date(2023, 5, 1),
                "supervision_level": "LIMITED",
            },
        ]

        self.run_query_test(
            builder.build(sandbox_context=None).view_query, expected_results
        )

    def test_build_span_only_no_reset_funnel(self) -> None:
        builder = FunnelStatusSessionsViewBuilder(
            view_id="my_simple_funnel_3",
            description="My simple funnel 3",
            index_cols=["person_id", "task_type"],
            funnel_reset_dates_sql_source="SELECT * FROM `my_sessions.my_reset_dates`",
            funnel_status_query_builders=[
                FunnelStatusSpanQueryBuilder(
                    sql_source="SELECT person_id, task_type, start_date, end_date_exclusive, supervision_level, FROM `my_sessions.supervision_level_sessions`",
                    start_date_col="start_date",
                    end_date_exclusive_col="end_date_exclusive",
                    index_cols=["person_id", "task_type"],
                    status_cols_by_type=[
                        ("supervision_level", bigquery.StandardSqlTypeNames.STRING),
                    ],
                    truncate_spans_at_reset_dates=False,
                )
            ],
        )

        reset_dates_sql_source: list[dict[str, Any]] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_reset_date": datetime.date(2020, 1, 1),
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_reset_date": datetime.date(2023, 5, 1),
            },
        ]

        funnel_spans_sql_source: list[dict[str, Any]] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "start_date": datetime.date(2019, 12, 11),
                "end_date_exclusive": datetime.date(2020, 4, 1),
                "supervision_level": "MINIMUM",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "start_date": datetime.date(2020, 4, 1),
                "end_date_exclusive": datetime.date(2022, 11, 15),
                "supervision_level": "MEDIUM",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "start_date": datetime.date(2022, 11, 15),
                "end_date_exclusive": datetime.date(2024, 3, 3),
                "supervision_level": "LIMITED",
            },
        ]

        self._create_funnel_reset_dates_table(
            data=reset_dates_sql_source,
        )

        self._create_supervision_level_sessions_table(
            data=funnel_spans_sql_source,
        )

        expected_results: list[dict] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 1,
                "date_gap_id": 1,
                "start_date": datetime.date(2019, 12, 11),
                "end_date_exclusive": datetime.date(2020, 4, 1),
                "supervision_level": "MINIMUM",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 2,
                "date_gap_id": 1,
                "start_date": datetime.date(2020, 4, 1),
                "end_date_exclusive": datetime.date(2022, 11, 15),
                "supervision_level": "MEDIUM",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 3,
                "date_gap_id": 1,
                "start_date": datetime.date(2022, 11, 15),
                "end_date_exclusive": datetime.date(2024, 3, 3),
                "supervision_level": "LIMITED",
            },
        ]

        self.run_query_test(
            builder.build(sandbox_context=None).view_query, expected_results
        )

    def test_build_spans_and_events_funnel(self) -> None:
        builder = FunnelStatusSessionsViewBuilder(
            view_id="my_simple_funnel_4",
            description="My simple funnel 4",
            index_cols=["person_id", "task_type"],
            funnel_reset_dates_sql_source="SELECT * FROM `my_sessions.my_reset_dates`",
            funnel_status_query_builders=[
                FunnelStatusSpanQueryBuilder(
                    sql_source="SELECT person_id, task_type, start_date, end_date_exclusive, supervision_level, FROM `my_sessions.supervision_level_sessions`",
                    start_date_col="start_date",
                    end_date_exclusive_col="end_date_exclusive",
                    index_cols=["person_id", "task_type"],
                    status_cols_by_type=[
                        ("supervision_level", bigquery.StandardSqlTypeNames.STRING),
                    ],
                ),
                FunnelStatusEventQueryBuilder(
                    sql_source="SELECT person_id, task_type, event_date, TRUE AS viewed FROM `my_usage_events.segment_events` WHERE event = 'VIEWED'",
                    event_date_col="event_date",
                    index_cols=["person_id", "task_type"],
                    status_cols_by_type=[
                        ("viewed", bigquery.StandardSqlTypeNames.BOOL)
                    ],
                ),
                FunnelStatusEventQueryBuilder(
                    sql_source="SELECT person_id, task_type, event_date, TRUE AS downloaded FROM `my_usage_events.segment_events` WHERE event = 'DOWNLOADED'",
                    event_date_col="event_date",
                    index_cols=["person_id", "task_type"],
                    status_cols_by_type=[
                        ("downloaded", bigquery.StandardSqlTypeNames.BOOL)
                    ],
                ),
            ],
        )

        reset_dates_sql_source: list[dict[str, Any]] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_reset_date": datetime.date(2020, 1, 1),
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_reset_date": datetime.date(2023, 5, 1),
            },
        ]

        funnel_spans_sql_source: list[dict[str, Any]] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "start_date": datetime.date(2019, 12, 11),
                "end_date_exclusive": datetime.date(2020, 4, 1),
                "supervision_level": "MINIMUM",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "start_date": datetime.date(2020, 4, 1),
                "end_date_exclusive": datetime.date(2022, 11, 15),
                "supervision_level": "MEDIUM",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "start_date": datetime.date(2022, 11, 15),
                "end_date_exclusive": datetime.date(2024, 3, 3),
                "supervision_level": "LIMITED",
            },
        ]

        usage_events_sql_source: list[dict[str, Any]] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "event_date": datetime.date(2018, 1, 1),
                "event": "VIEWED",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "event_date": datetime.date(2022, 6, 2),
                "event": "VIEWED",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "event_date": datetime.date(2022, 8, 9),
                "event": "DOWNLOADED",
            },
        ]

        self._create_funnel_reset_dates_table(
            data=reset_dates_sql_source,
        )

        self._create_supervision_level_sessions_table(
            data=funnel_spans_sql_source,
        )

        self._create_usage_events_table(
            data=usage_events_sql_source,
        )

        expected_results: list[dict] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 1,
                "date_gap_id": 1,
                "start_date": datetime.date(2018, 1, 1),
                "end_date_exclusive": datetime.date(2019, 12, 11),
                "supervision_level": None,
                "viewed": True,
                "downloaded": False,
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 2,
                "date_gap_id": 1,
                "start_date": datetime.date(2019, 12, 11),
                "end_date_exclusive": datetime.date(2020, 1, 1),
                "supervision_level": "MINIMUM",
                "viewed": True,
                "downloaded": False,
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 3,
                "date_gap_id": 2,
                "start_date": datetime.date(2020, 4, 1),
                "end_date_exclusive": datetime.date(2022, 6, 2),
                "supervision_level": "MEDIUM",
                "viewed": False,
                "downloaded": False,
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 4,
                "date_gap_id": 2,
                "start_date": datetime.date(2022, 6, 2),
                "end_date_exclusive": datetime.date(2022, 8, 9),
                "supervision_level": "MEDIUM",
                "viewed": True,
                "downloaded": False,
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 5,
                "date_gap_id": 2,
                "start_date": datetime.date(2022, 8, 9),
                "end_date_exclusive": datetime.date(2022, 11, 15),
                "supervision_level": "MEDIUM",
                "viewed": True,
                "downloaded": True,
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 6,
                "date_gap_id": 2,
                "start_date": datetime.date(2022, 11, 15),
                "end_date_exclusive": datetime.date(2023, 5, 1),
                "supervision_level": "LIMITED",
                "viewed": True,
                "downloaded": True,
            },
        ]

        self.run_query_test(
            builder.build(sandbox_context=None).view_query, expected_results
        )

    def test_build_spans_and_events_funnel_with_base_spans(self) -> None:
        builder = FunnelStatusSessionsViewBuilder(
            view_id="my_simple_funnel_4",
            description="My simple funnel 4",
            index_cols=["person_id", "task_type"],
            funnel_reset_dates_sql_source="SELECT * FROM `my_sessions.my_reset_dates`",
            funnel_status_query_builders=[
                FunnelStatusSpanQueryBuilder(
                    sql_source="SELECT person_id, task_type, start_date, end_date_exclusive, TRUE AS justice_involved, FROM `my_sessions.system_sessions`",
                    start_date_col="start_date",
                    end_date_exclusive_col="end_date_exclusive",
                    index_cols=["person_id", "task_type"],
                    status_cols_by_type=[
                        ("justice_involved", bigquery.StandardSqlTypeNames.BOOL),
                    ],
                    truncate_spans_at_reset_dates=False,
                ),
                FunnelStatusSpanQueryBuilder(
                    sql_source="SELECT person_id, task_type, start_date, end_date_exclusive, supervision_level, FROM `my_sessions.supervision_level_sessions`",
                    start_date_col="start_date",
                    end_date_exclusive_col="end_date_exclusive",
                    index_cols=["person_id", "task_type"],
                    status_cols_by_type=[
                        ("supervision_level", bigquery.StandardSqlTypeNames.STRING),
                    ],
                ),
                FunnelStatusEventQueryBuilder(
                    sql_source="SELECT person_id, task_type, event_date, TRUE AS viewed FROM `my_usage_events.segment_events` WHERE event = 'VIEWED'",
                    event_date_col="event_date",
                    index_cols=["person_id", "task_type"],
                    status_cols_by_type=[
                        ("viewed", bigquery.StandardSqlTypeNames.BOOL)
                    ],
                ),
                FunnelStatusEventQueryBuilder(
                    sql_source="SELECT person_id, task_type, event_date, TRUE AS downloaded FROM `my_usage_events.segment_events` WHERE event = 'DOWNLOADED'",
                    event_date_col="event_date",
                    index_cols=["person_id", "task_type"],
                    status_cols_by_type=[
                        ("downloaded", bigquery.StandardSqlTypeNames.BOOL)
                    ],
                ),
            ],
        )

        reset_dates_sql_source: list[dict[str, Any]] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_reset_date": datetime.date(2020, 1, 1),
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_reset_date": datetime.date(2023, 5, 1),
            },
        ]

        funnel_spans_sql_source: list[dict[str, Any]] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "start_date": datetime.date(2019, 12, 11),
                "end_date_exclusive": datetime.date(2020, 4, 1),
                "supervision_level": "MINIMUM",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "start_date": datetime.date(2020, 4, 1),
                "end_date_exclusive": datetime.date(2022, 11, 15),
                "supervision_level": "MEDIUM",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "start_date": datetime.date(2022, 11, 15),
                "end_date_exclusive": datetime.date(2024, 3, 3),
                "supervision_level": "LIMITED",
            },
        ]

        system_spans_sql_source: list[dict[str, Any]] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "start_date": datetime.date(2017, 4, 4),
                "end_date_exclusive": None,
            },
        ]

        usage_events_sql_source: list[dict[str, Any]] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "event_date": datetime.date(2018, 1, 1),
                "event": "VIEWED",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "event_date": datetime.date(2022, 6, 2),
                "event": "VIEWED",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "event_date": datetime.date(2022, 8, 9),
                "event": "DOWNLOADED",
            },
        ]

        self._create_funnel_reset_dates_table(
            data=reset_dates_sql_source,
        )

        self._create_supervision_level_sessions_table(
            data=funnel_spans_sql_source,
        )

        self._create_usage_events_table(
            data=usage_events_sql_source,
        )

        self._create_system_sessions_table(data=system_spans_sql_source)

        expected_results: list[dict] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 1,
                "date_gap_id": 1,
                "start_date": datetime.date(2017, 4, 4),
                "end_date_exclusive": datetime.date(2018, 1, 1),
                "justice_involved": True,
                "supervision_level": None,
                "viewed": False,
                "downloaded": False,
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 2,
                "date_gap_id": 1,
                "start_date": datetime.date(2018, 1, 1),
                "end_date_exclusive": datetime.date(2019, 12, 11),
                "justice_involved": True,
                "supervision_level": None,
                "viewed": True,
                "downloaded": False,
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 3,
                "date_gap_id": 1,
                "start_date": datetime.date(2019, 12, 11),
                "end_date_exclusive": datetime.date(2020, 1, 1),
                "justice_involved": True,
                "supervision_level": "MINIMUM",
                "viewed": True,
                "downloaded": False,
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 4,
                "date_gap_id": 1,
                "start_date": datetime.date(2020, 1, 1),
                "end_date_exclusive": datetime.date(2020, 4, 1),
                "justice_involved": True,
                "supervision_level": None,
                "viewed": False,
                "downloaded": False,
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 5,
                "date_gap_id": 1,
                "start_date": datetime.date(2020, 4, 1),
                "end_date_exclusive": datetime.date(2022, 6, 2),
                "justice_involved": True,
                "supervision_level": "MEDIUM",
                "viewed": False,
                "downloaded": False,
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 6,
                "date_gap_id": 1,
                "start_date": datetime.date(2022, 6, 2),
                "end_date_exclusive": datetime.date(2022, 8, 9),
                "justice_involved": True,
                "supervision_level": "MEDIUM",
                "viewed": True,
                "downloaded": False,
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 7,
                "date_gap_id": 1,
                "start_date": datetime.date(2022, 8, 9),
                "end_date_exclusive": datetime.date(2022, 11, 15),
                "justice_involved": True,
                "supervision_level": "MEDIUM",
                "viewed": True,
                "downloaded": True,
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 8,
                "date_gap_id": 1,
                "start_date": datetime.date(2022, 11, 15),
                "end_date_exclusive": datetime.date(2023, 5, 1),
                "justice_involved": True,
                "supervision_level": "LIMITED",
                "viewed": True,
                "downloaded": True,
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_status_session_id": 9,
                "date_gap_id": 1,
                "start_date": datetime.date(2023, 5, 1),
                "end_date_exclusive": None,
                "justice_involved": True,
                "supervision_level": None,
                "viewed": False,
                "downloaded": False,
            },
        ]

        self.run_query_test(
            builder.build(sandbox_context=None).view_query, expected_results
        )

    def test_output_columns_match(self) -> None:
        builder = FunnelStatusSessionsViewBuilder(
            view_id="my_simple_funnel_5",
            description="My simple funnel 5",
            index_cols=["person_id", "task_type"],
            funnel_reset_dates_sql_source="SELECT * FROM `my_sessions.my_reset_dates`",
            funnel_status_query_builders=[
                FunnelStatusSpanQueryBuilder(
                    sql_source="SELECT person_id, task_type, start_date, end_date_exclusive, supervision_level, FROM `my_sessions.supervision_level_sessions`",
                    start_date_col="start_date",
                    end_date_exclusive_col="end_date_exclusive",
                    index_cols=["person_id", "task_type"],
                    status_cols_by_type=[
                        ("supervision_level", bigquery.StandardSqlTypeNames.STRING),
                    ],
                ),
                FunnelStatusEventQueryBuilder(
                    sql_source="SELECT person_id, task_type, event_date, TRUE AS viewed FROM `my_usage_events.segment_events` WHERE event = 'VIEWED'",
                    event_date_col="event_date",
                    index_cols=["person_id", "task_type"],
                    status_cols_by_type=[
                        ("viewed", bigquery.StandardSqlTypeNames.BOOL)
                    ],
                ),
                FunnelStatusEventQueryBuilder(
                    sql_source="SELECT person_id, task_type, event_date, TRUE AS downloaded FROM `my_usage_events.segment_events` WHERE event = 'DOWNLOADED'",
                    event_date_col="event_date",
                    index_cols=["person_id", "task_type"],
                    status_cols_by_type=[
                        ("downloaded", bigquery.StandardSqlTypeNames.BOOL)
                    ],
                ),
            ],
        )

        reset_dates_sql_source: list[dict[str, Any]] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_reset_date": datetime.date(2020, 1, 1),
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "funnel_reset_date": datetime.date(2023, 5, 1),
            },
        ]

        funnel_spans_sql_source: list[dict[str, Any]] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "start_date": datetime.date(2019, 12, 11),
                "end_date_exclusive": datetime.date(2020, 4, 1),
                "supervision_level": "MINIMUM",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "start_date": datetime.date(2020, 4, 1),
                "end_date_exclusive": datetime.date(2022, 11, 15),
                "supervision_level": "MEDIUM",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "start_date": datetime.date(2022, 11, 15),
                "end_date_exclusive": datetime.date(2024, 3, 3),
                "supervision_level": "LIMITED",
            },
        ]

        usage_events_sql_source: list[dict[str, Any]] = [
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "event_date": datetime.date(2018, 1, 1),
                "event": "VIEWED",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "event_date": datetime.date(2022, 6, 2),
                "event": "VIEWED",
            },
            {
                "person_id": 1234,
                "task_type": "OPP_1",
                "event_date": datetime.date(2022, 8, 9),
                "event": "DOWNLOADED",
            },
        ]
        self._create_funnel_reset_dates_table(
            data=reset_dates_sql_source,
        )

        self._create_supervision_level_sessions_table(
            data=funnel_spans_sql_source,
        )

        self._create_usage_events_table(
            data=usage_events_sql_source,
        )

        expected_columns = [
            "person_id",
            "task_type",
            "funnel_status_session_id",
            "date_gap_id",
            "start_date",
            "end_date_exclusive",
            "supervision_level",
            "viewed",
            "downloaded",
        ]
        # Check if the output columns match the expected columns on builder object
        self.assertEqual(expected_columns, builder.output_columns)

        # Check if the output columns match the actual columns in the emulator result
        actual_columns = list(
            self.query(builder.build(sandbox_context=None).view_query).columns
        )
        self.assertEqual(expected_columns, actual_columns)
