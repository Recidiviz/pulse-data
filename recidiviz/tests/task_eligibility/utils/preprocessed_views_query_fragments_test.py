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
"""Tests the exact-calendar-month window boundary behavior in
classification_incident_score_query_template, added for MI's disciplinary score
components (use_exact_calendar_month_windows=True)."""
from datetime import date

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_column import Date as DateColumn
from recidiviz.big_query.big_query_view_column import Integer as IntegerColumn
from recidiviz.big_query.big_query_view_column import String as StringColumn
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.classification_score_component_big_query_view_builder import (
    COMPONENT_SCORE_COLUMN_NAME,
    INITIAL_COMPONENT_SCORE_COLUMN_NAME,
)
from recidiviz.task_eligibility.utils.preprocessed_views_query_fragments import (
    classification_incident_score_query_template,
)
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
)

_CUSTODIAL_AUTHORITY_SESSIONS_ADDRESS = BigQueryAddress.from_str(
    "sessions.custodial_authority_sessions_materialized"
)
_INCIDENTS_ADDRESS = BigQueryAddress.from_str(
    "classification_views.incarceration_incidents_classification_preprocessed_materialized"
)

_PERSON_ID = 1
_SESSION_ID = 1
_SESSION_START_DATE = date(2020, 1, 1)

_PARENT_SCHEMAS = {
    _CUSTODIAL_AUTHORITY_SESSIONS_ADDRESS: [
        schema_field_for_type("person_id", int),
        schema_field_for_type("state_code", str),
        schema_field_for_type("custodial_authority_session_id", int),
        schema_field_for_type("custodial_authority", str),
        schema_field_for_type("start_date", date),
        schema_field_for_type("end_date_exclusive", date),
    ],
    _INCIDENTS_ADDRESS: [
        schema_field_for_type("person_id", int),
        schema_field_for_type("state_code", str),
        schema_field_for_type("custodial_authority_session_id", int),
        schema_field_for_type("incarceration_incident_id", int),
        schema_field_for_type("incident_date", date),
        schema_field_for_type("infraction_type_raw_text", str),
        schema_field_for_type("incident_class", str),
        schema_field_for_type("incident_category", str),
    ],
}


def _custodial_authority_session_row() -> dict:
    return {
        "person_id": _PERSON_ID,
        "state_code": StateCode.US_MI.value,
        "custodial_authority_session_id": _SESSION_ID,
        "custodial_authority": "STATE_PRISON",
        "start_date": _SESSION_START_DATE,
        "end_date_exclusive": None,
    }


def _incident_row(*, incarceration_incident_id: int, incident_date: date) -> dict:
    return {
        "person_id": _PERSON_ID,
        "state_code": StateCode.US_MI.value,
        "custodial_authority_session_id": _SESSION_ID,
        "incarceration_incident_id": incarceration_incident_id,
        "incident_date": incident_date,
        "infraction_type_raw_text": "TEST",
        "incident_class": "I",
        "incident_category": "other_class_i_or_ii",
    }


class SingleWindowExactCalendarMonthWindowsTest(SimpleBigQueryViewBuilderTestCase):
    """Tests the (0, 6) month window boundary with use_exact_calendar_month_windows=True."""

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        raw_template = classification_incident_score_query_template(
            incident_filter_condition="TRUE",
            state_code=StateCode.US_MI,
            score_definitions={(0, 6): {i: i for i in range(21)}},
            use_exact_calendar_month_windows=True,
        )
        return SimpleBigQueryViewBuilder(
            dataset_id="test_dataset",
            view_id="single_window_exact_calendar_month_windows",
            description="Test view isolating the (0, 6) month window boundary.",
            view_query_template=f"""
            SELECT
                state_code,
                person_id,
                start_date,
                end_date,
                {COMPONENT_SCORE_COLUMN_NAME},
            FROM ({raw_template})
            ORDER BY start_date
            """,
            schema=[
                StringColumn(
                    name="state_code", description="State code.", mode="REQUIRED"
                ),
                IntegerColumn(
                    name="person_id", description="Person ID.", mode="REQUIRED"
                ),
                DateColumn(
                    name="start_date", description="Span start date.", mode="REQUIRED"
                ),
                DateColumn(
                    name="end_date", description="Span end date.", mode="NULLABLE"
                ),
                IntegerColumn(
                    name=COMPONENT_SCORE_COLUMN_NAME,
                    description="Component score.",
                    mode="NULLABLE",
                ),
            ],
        )

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        return _PARENT_SCHEMAS

    def test_incident_exactly_six_months_ago_counts_in_nearer_window(self) -> None:
        incident_date = date(2026, 2, 4)
        input_data = {
            _CUSTODIAL_AUTHORITY_SESSIONS_ADDRESS: [_custodial_authority_session_row()],
            _INCIDENTS_ADDRESS: [
                _incident_row(incarceration_incident_id=1, incident_date=incident_date)
            ],
        }
        expected_output = [
            {
                "state_code": StateCode.US_MI.value,
                "person_id": _PERSON_ID,
                "start_date": incident_date,
                "end_date": date(2026, 8, 5),
                COMPONENT_SCORE_COLUMN_NAME: 1,
            }
        ]
        self.run_simple_view_builder_query_test_from_data(input_data, expected_output)

    def test_incident_one_day_later_shifts_end_boundary_by_one_day(self) -> None:
        """Regression guard: confirms the boundary is event_date + 6 months + 1 day
        (exact arithmetic), not a hardcoded date."""
        incident_date = date(2026, 2, 5)
        input_data = {
            _CUSTODIAL_AUTHORITY_SESSIONS_ADDRESS: [_custodial_authority_session_row()],
            _INCIDENTS_ADDRESS: [
                _incident_row(incarceration_incident_id=1, incident_date=incident_date)
            ],
        }
        expected_output = [
            {
                "state_code": StateCode.US_MI.value,
                "person_id": _PERSON_ID,
                "start_date": incident_date,
                "end_date": date(2026, 8, 6),
                COMPONENT_SCORE_COLUMN_NAME: 1,
            }
        ]
        self.run_simple_view_builder_query_test_from_data(input_data, expected_output)


class MultiWindowExactCalendarMonthWindowsTest(SimpleBigQueryViewBuilderTestCase):
    """Tests the (0,6)/(6,12)/(12,24)/(24,36) month window handoffs and the
    initial-vs-reclass score divergence, mirroring US_MI_VIOLENT_INCIDENTS's window
    and score mapping, with use_exact_calendar_month_windows=True."""

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        raw_template = classification_incident_score_query_template(
            incident_filter_condition="TRUE",
            state_code=StateCode.US_MI,
            initial_score_definitions={(0, 6): {i: i * 25 for i in range(21)}},
            reclass_score_definitions={
                (0, 6): {i: i * 25 for i in range(21)},
                (6, 12): {i: i * 8 for i in range(21)},
                (12, 24): {i: i * 5 for i in range(21)},
                (24, 36): {i: i * 4 for i in range(21)},
            },
            use_exact_calendar_month_windows=True,
        )
        return SimpleBigQueryViewBuilder(
            dataset_id="test_dataset",
            view_id="multi_window_exact_calendar_month_windows",
            description="Test view isolating window handoffs across all four windows.",
            view_query_template=f"""
            SELECT
                state_code,
                person_id,
                start_date,
                end_date,
                {COMPONENT_SCORE_COLUMN_NAME},
                {INITIAL_COMPONENT_SCORE_COLUMN_NAME},
            FROM ({raw_template})
            ORDER BY start_date
            """,
            schema=[
                StringColumn(
                    name="state_code", description="State code.", mode="REQUIRED"
                ),
                IntegerColumn(
                    name="person_id", description="Person ID.", mode="REQUIRED"
                ),
                DateColumn(
                    name="start_date", description="Span start date.", mode="REQUIRED"
                ),
                DateColumn(
                    name="end_date", description="Span end date.", mode="NULLABLE"
                ),
                IntegerColumn(
                    name=COMPONENT_SCORE_COLUMN_NAME,
                    description="Reclassification component score.",
                    mode="NULLABLE",
                ),
                IntegerColumn(
                    name=INITIAL_COMPONENT_SCORE_COLUMN_NAME,
                    description="Initial component score.",
                    mode="NULLABLE",
                ),
            ],
        )

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        return _PARENT_SCHEMAS

    def test_incident_ages_through_all_windows_with_shifted_boundaries(self) -> None:
        incident_date = date(2025, 8, 4)
        input_data = {
            _CUSTODIAL_AUTHORITY_SESSIONS_ADDRESS: [_custodial_authority_session_row()],
            _INCIDENTS_ADDRESS: [
                _incident_row(incarceration_incident_id=1, incident_date=incident_date)
            ],
        }
        expected_output = [
            {
                "state_code": StateCode.US_MI.value,
                "person_id": _PERSON_ID,
                "start_date": date(2025, 8, 4),
                "end_date": date(2026, 2, 5),
                COMPONENT_SCORE_COLUMN_NAME: 25,
                INITIAL_COMPONENT_SCORE_COLUMN_NAME: 25,
            },
            {
                "state_code": StateCode.US_MI.value,
                "person_id": _PERSON_ID,
                "start_date": date(2026, 2, 5),
                "end_date": date(2026, 8, 5),
                COMPONENT_SCORE_COLUMN_NAME: 8,
                INITIAL_COMPONENT_SCORE_COLUMN_NAME: 0,
            },
            {
                "state_code": StateCode.US_MI.value,
                "person_id": _PERSON_ID,
                "start_date": date(2026, 8, 5),
                "end_date": date(2027, 8, 5),
                COMPONENT_SCORE_COLUMN_NAME: 5,
                INITIAL_COMPONENT_SCORE_COLUMN_NAME: 0,
            },
            {
                "state_code": StateCode.US_MI.value,
                "person_id": _PERSON_ID,
                "start_date": date(2027, 8, 5),
                "end_date": date(2028, 8, 5),
                COMPONENT_SCORE_COLUMN_NAME: 4,
                INITIAL_COMPONENT_SCORE_COLUMN_NAME: 0,
            },
        ]
        self.run_simple_view_builder_query_test_from_data(input_data, expected_output)
