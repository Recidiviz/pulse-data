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
"""Tests the US_MI_AT_LEAST_12_MONTHS_SINCE_LATEST_ASSESSMENT criterion."""
from datetime import date
from typing import Any

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.classification.classification_assessment_event_big_query_view_builder import (
    classification_assessment_event_schema,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mi.at_least_12_months_since_latest_assessment import (
    VIEW_BUILDER,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

_INITIAL_ASSESSMENTS_ADDRESS = BigQueryAddress(
    dataset_id="classification_assessment_events_us_mi",
    table_id="initial_materialized",
)
_REVIEW_ASSESSMENTS_ADDRESS = BigQueryAddress(
    dataset_id="classification_assessment_events_us_mi",
    table_id="review_materialized",
)
_ASSESSMENT_EVENTS_SCHEMA = [
    column.as_schema_field() for column in classification_assessment_event_schema()
]

_CUSTODIAL_AUTHORITY_SESSIONS_ADDRESS = BigQueryAddress(
    dataset_id="sessions", table_id="custodial_authority_sessions_materialized"
)
_CUSTODIAL_AUTHORITY_SESSIONS_SCHEMA = [
    bigquery.SchemaField("state_code", "STRING"),
    bigquery.SchemaField("person_id", "INTEGER"),
    bigquery.SchemaField("custodial_authority", "STRING"),
    bigquery.SchemaField("start_date", "DATE"),
    bigquery.SchemaField("end_date_exclusive", "DATE"),
]


class TestAtLeast12MonthsSinceLatestAssessment(BigQueryEmulatorTestCase):
    """Tests the US_MI_AT_LEAST_12_MONTHS_SINCE_LATEST_ASSESSMENT criterion."""

    def setUp(self) -> None:
        super().setUp()
        self.assertSetEqual(
            VIEW_BUILDER.build().parent_tables,
            {
                _INITIAL_ASSESSMENTS_ADDRESS,
                _REVIEW_ASSESSMENTS_ADDRESS,
                _CUSTODIAL_AUTHORITY_SESSIONS_ADDRESS,
            },
        )
        self.create_mock_table(_INITIAL_ASSESSMENTS_ADDRESS, _ASSESSMENT_EVENTS_SCHEMA)
        self.create_mock_table(_REVIEW_ASSESSMENTS_ADDRESS, _ASSESSMENT_EVENTS_SCHEMA)
        self.create_mock_table(
            _CUSTODIAL_AUTHORITY_SESSIONS_ADDRESS, _CUSTODIAL_AUTHORITY_SESSIONS_SCHEMA
        )

    def _run_criterion_query_test(self, expected_result: list[dict[str, Any]]) -> None:
        self.run_query_test(
            f"""
            SELECT * EXCEPT (reason), TO_JSON_STRING(reason) AS reason
            FROM ({VIEW_BUILDER.build().view_query})
            ORDER BY person_id, start_date
            """,
            expected_result=expected_result,
        )

    def test_last_assessment_spans_end_when_its_stay_ends(self) -> None:
        self.load_rows_into_table(
            _REVIEW_ASSESSMENTS_ADDRESS,
            [
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "assessment_id": 1,
                    "assessment_class": "RISK",
                    "assessment_type": "INTERNAL_UNKNOWN",
                    "assessment_date": date(2022, 7, 6),
                    "classification_date": date(2022, 7, 6),
                    "classification_decision_date": date(2022, 7, 6),
                    "assessment_due_date": date(2023, 7, 6),
                },
            ],
        )
        self.load_rows_into_table(
            _CUSTODIAL_AUTHORITY_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "custodial_authority": "STATE_PRISON",
                    "start_date": date(2022, 5, 27),
                    "end_date_exclusive": date(2023, 6, 27),
                },
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "custodial_authority": "SUPERVISION_AUTHORITY",
                    "start_date": date(2023, 6, 27),
                    "end_date_exclusive": date(2026, 10, 15),
                },
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "custodial_authority": "STATE_PRISON",
                    "start_date": date(2026, 10, 15),
                    "end_date_exclusive": None,
                },
            ],
        )
        self._run_criterion_query_test(
            expected_result=[
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2022, 7, 6),
                    "end_date": date(2023, 6, 22),
                    "meets_criteria": False,
                    "reason": '{"most_recent_assessment_date":"2022-07-06",'
                    '"assessment_due_date":"2023-07-06",'
                    '"assessment_due_month":"2023-07-01"}',
                    "reason_v2": {
                        "most_recent_assessment_date": "2022-07-06",
                        "assessment_due_date": "2023-07-06",
                        "assessment_due_month": "2023-07-01",
                    },
                },
                # Release from state prison on 2023-06-27 ends this span, so the 2022
                # assessment carries no assessment_due_date into the 2026 stay.
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2023, 6, 22),
                    "end_date": date(2023, 6, 27),
                    "meets_criteria": True,
                    "reason": '{"most_recent_assessment_date":"2022-07-06",'
                    '"assessment_due_date":"2023-07-06",'
                    '"assessment_due_month":"2023-07-01"}',
                    "reason_v2": {
                        "most_recent_assessment_date": "2022-07-06",
                        "assessment_due_date": "2023-07-06",
                        "assessment_due_month": "2023-07-01",
                    },
                },
            ],
        )

    def test_last_assessment_eligible_span_stays_open_in_open_stay(self) -> None:
        self.load_rows_into_table(
            _REVIEW_ASSESSMENTS_ADDRESS,
            [
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "assessment_id": 1,
                    "assessment_class": "RISK",
                    "assessment_type": "INTERNAL_UNKNOWN",
                    "assessment_date": date(2025, 3, 10),
                    "classification_date": date(2025, 3, 10),
                    "classification_decision_date": date(2025, 3, 10),
                    "assessment_due_date": date(2026, 3, 10),
                },
            ],
        )
        self.load_rows_into_table(
            _CUSTODIAL_AUTHORITY_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "custodial_authority": "STATE_PRISON",
                    "start_date": date(2025, 1, 2),
                    "end_date_exclusive": None,
                },
            ],
        )
        self._run_criterion_query_test(
            expected_result=[
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2025, 3, 10),
                    "end_date": date(2026, 2, 24),
                    "meets_criteria": False,
                    "reason": '{"most_recent_assessment_date":"2025-03-10",'
                    '"assessment_due_date":"2026-03-10",'
                    '"assessment_due_month":"2026-03-01"}',
                    "reason_v2": {
                        "most_recent_assessment_date": "2025-03-10",
                        "assessment_due_date": "2026-03-10",
                        "assessment_due_month": "2026-03-01",
                    },
                },
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2026, 2, 24),
                    "end_date": None,
                    "meets_criteria": True,
                    "reason": '{"most_recent_assessment_date":"2025-03-10",'
                    '"assessment_due_date":"2026-03-10",'
                    '"assessment_due_month":"2026-03-01"}',
                    "reason_v2": {
                        "most_recent_assessment_date": "2025-03-10",
                        "assessment_due_date": "2026-03-10",
                        "assessment_due_month": "2026-03-01",
                    },
                },
            ],
        )

    def test_consecutive_assessments_in_open_stay_chain_spans(self) -> None:
        self.load_rows_into_table(
            _REVIEW_ASSESSMENTS_ADDRESS,
            [
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "assessment_id": 1,
                    "assessment_class": "RISK",
                    "assessment_type": "INTERNAL_UNKNOWN",
                    "assessment_date": date(2024, 4, 15),
                    "classification_date": date(2024, 4, 15),
                    "classification_decision_date": date(2024, 4, 15),
                    "assessment_due_date": date(2025, 4, 15),
                },
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "assessment_id": 2,
                    "assessment_class": "RISK",
                    "assessment_type": "INTERNAL_UNKNOWN",
                    "assessment_date": date(2025, 4, 8),
                    "classification_date": date(2025, 4, 8),
                    "classification_decision_date": date(2025, 4, 8),
                    "assessment_due_date": date(2026, 4, 8),
                },
            ],
        )
        self.load_rows_into_table(
            _CUSTODIAL_AUTHORITY_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "custodial_authority": "STATE_PRISON",
                    "start_date": date(2024, 1, 20),
                    "end_date_exclusive": None,
                },
            ],
        )
        self._run_criterion_query_test(
            expected_result=[
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2024, 4, 15),
                    "end_date": date(2025, 4, 1),
                    "meets_criteria": False,
                    "reason": '{"most_recent_assessment_date":"2024-04-15",'
                    '"assessment_due_date":"2025-04-15",'
                    '"assessment_due_month":"2025-04-01"}',
                    "reason_v2": {
                        "most_recent_assessment_date": "2024-04-15",
                        "assessment_due_date": "2025-04-15",
                        "assessment_due_month": "2025-04-01",
                    },
                },
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2025, 4, 1),
                    "end_date": date(2025, 4, 8),
                    "meets_criteria": True,
                    "reason": '{"most_recent_assessment_date":"2024-04-15",'
                    '"assessment_due_date":"2025-04-15",'
                    '"assessment_due_month":"2025-04-01"}',
                    "reason_v2": {
                        "most_recent_assessment_date": "2024-04-15",
                        "assessment_due_date": "2025-04-15",
                        "assessment_due_month": "2025-04-01",
                    },
                },
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2025, 4, 8),
                    "end_date": date(2026, 3, 25),
                    "meets_criteria": False,
                    "reason": '{"most_recent_assessment_date":"2025-04-08",'
                    '"assessment_due_date":"2026-04-08",'
                    '"assessment_due_month":"2026-04-01"}',
                    "reason_v2": {
                        "most_recent_assessment_date": "2025-04-08",
                        "assessment_due_date": "2026-04-08",
                        "assessment_due_month": "2026-04-01",
                    },
                },
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2026, 3, 25),
                    "end_date": None,
                    "meets_criteria": True,
                    "reason": '{"most_recent_assessment_date":"2025-04-08",'
                    '"assessment_due_date":"2026-04-08",'
                    '"assessment_due_month":"2026-04-01"}',
                    "reason_v2": {
                        "most_recent_assessment_date": "2025-04-08",
                        "assessment_due_date": "2026-04-08",
                        "assessment_due_month": "2026-04-01",
                    },
                },
            ],
        )
