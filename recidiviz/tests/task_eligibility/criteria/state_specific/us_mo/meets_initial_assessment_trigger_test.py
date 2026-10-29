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
"""Unit tests for meets_initial_assessment_trigger.py"""
from datetime import date

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.task_eligibility.criteria.state_specific.us_mo.meets_initial_assessment_trigger import (
    VIEW_BUILDER,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

_STATE_ASSESSMENT_ADDRESS = BigQueryAddress(
    dataset_id="us_mo_normalized_state", table_id="state_assessment"
)
_STATE_ASSESSMENT_SCHEMA = [
    bigquery.SchemaField("state_code", "STRING"),
    bigquery.SchemaField("person_id", "INTEGER"),
    bigquery.SchemaField("assessment_date", "DATE"),
    bigquery.SchemaField("assessment_type", "STRING"),
]

_SUPER_SESSIONS_ADDRESS = BigQueryAddress(
    dataset_id="sessions",
    table_id="prioritized_supervision_super_sessions_materialized",
)
_SUPER_SESSIONS_SCHEMA = [
    bigquery.SchemaField("state_code", "STRING"),
    bigquery.SchemaField("person_id", "INTEGER"),
    bigquery.SchemaField("start_date", "DATE"),
    bigquery.SchemaField("end_date_exclusive", "DATE"),
]

_COMPARTMENT_SESSIONS_ADDRESS = BigQueryAddress(
    dataset_id="sessions", table_id="compartment_sessions_materialized"
)
_COMPARTMENT_SESSIONS_SCHEMA = [
    bigquery.SchemaField("state_code", "STRING"),
    bigquery.SchemaField("person_id", "INTEGER"),
    bigquery.SchemaField("start_date", "DATE"),
    bigquery.SchemaField("compartment_level_1", "STRING"),
]


class TestMeetsInitialAssessmentTriggerViewBuilder(BigQueryEmulatorTestCase):
    """Tests the initial ORAS assessment trigger: meets_criteria is True from a
    supervision period's start until a qualifying assessment, with the due date
    60 days after the start. A qualifying assessment from up to 365 days before
    the start also satisfies it, unless the person entered a LIBERTY session
    after that assessment."""

    def setUp(self) -> None:
        super().setUp()
        self.create_mock_table(
            address=_STATE_ASSESSMENT_ADDRESS,
            schema=_STATE_ASSESSMENT_SCHEMA,
        )
        self.create_mock_table(
            address=_SUPER_SESSIONS_ADDRESS,
            schema=_SUPER_SESSIONS_SCHEMA,
        )
        self.create_mock_table(
            address=_COMPARTMENT_SESSIONS_ADDRESS,
            schema=_COMPARTMENT_SESSIONS_SCHEMA,
        )

    def test_assessment_during_prior_period_satisfies_new_period_without_liberty_gap(
        self,
    ) -> None:
        # With no LIBERTY session between the two periods (for example, a
        # suspension and reinstatement), an assessment completed during the
        # first period also satisfies the second period's trigger (OBT-53667):
        # person_id=1's assessment on 2025-02-01 falls inside the first period
        # (2025-01-01 to 2025-03-15) and is within 365 days of the second
        # period's 2025-03-15 start.
        self.load_rows_into_table(
            _SUPER_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_MO",
                    "person_id": 1,
                    "start_date": date(2025, 1, 1),
                    "end_date_exclusive": date(2025, 3, 15),
                },
                {
                    "state_code": "US_MO",
                    "person_id": 1,
                    "start_date": date(2025, 3, 15),
                    "end_date_exclusive": None,
                },
            ],
        )
        self.load_rows_into_table(
            _STATE_ASSESSMENT_ADDRESS,
            [
                {
                    "state_code": "US_MO",
                    "person_id": 1,
                    "assessment_date": date(2025, 2, 1),
                    "assessment_type": "ORAS_COMMUNITY_SUPERVISION",
                },
            ],
        )
        self.load_rows_into_table(_COMPARTMENT_SESSIONS_ADDRESS, [])
        self.run_query_test(
            f"""
            SELECT start_date, end_date, meets_criteria
            FROM ({VIEW_BUILDER.build().view_query})
            """,
            expected_result=[
                # The first period has no earlier assessment, so the initial
                # assessment is due until the 2025-02-01 assessment. The second
                # period gets no row because that assessment already satisfies it.
                {
                    "start_date": date(2025, 1, 1),
                    "end_date": date(2025, 2, 1),
                    "meets_criteria": True,
                },
            ],
        )

    def test_assessment_before_liberty_gap_does_not_satisfy_new_period(
        self,
    ) -> None:
        # The person was discharged to LIBERTY on 2025-03-01, which starts a
        # new supervision cycle, so the 2025-02-01 assessment from the first
        # period doesn't satisfy the second period, even though it's within
        # 365 days of the 2025-04-01 start.
        self.load_rows_into_table(
            _SUPER_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_MO",
                    "person_id": 3,
                    "start_date": date(2025, 1, 1),
                    "end_date_exclusive": date(2025, 3, 1),
                },
                {
                    "state_code": "US_MO",
                    "person_id": 3,
                    "start_date": date(2025, 4, 1),
                    "end_date_exclusive": None,
                },
            ],
        )
        self.load_rows_into_table(
            _STATE_ASSESSMENT_ADDRESS,
            [
                {
                    "state_code": "US_MO",
                    "person_id": 3,
                    "assessment_date": date(2025, 2, 1),
                    "assessment_type": "ORAS_COMMUNITY_SUPERVISION",
                },
            ],
        )
        self.load_rows_into_table(
            _COMPARTMENT_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_MO",
                    "person_id": 3,
                    "start_date": date(2025, 3, 1),
                    "compartment_level_1": "LIBERTY",
                },
            ],
        )
        self.run_query_test(
            f"""
            SELECT start_date, end_date, meets_criteria
            FROM ({VIEW_BUILDER.build().view_query})
            """,
            expected_result=[
                {
                    "start_date": date(2025, 1, 1),
                    "end_date": date(2025, 2, 1),
                    "meets_criteria": True,
                },
                {
                    "start_date": date(2025, 4, 1),
                    "end_date": None,
                    "meets_criteria": True,
                },
            ],
        )

    def test_sar_assessment_during_liberty_satisfies_new_period(self) -> None:
        # Like a CST scored for a pre-sentence SAR: the person has been at
        # liberty since 2024-12-01, the assessment comes after that, and
        # supervision starts on 2025-02-03, so no initial assessment is due.
        self.load_rows_into_table(
            _SUPER_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_MO",
                    "person_id": 4,
                    "start_date": date(2025, 2, 3),
                    "end_date_exclusive": None,
                },
            ],
        )
        self.load_rows_into_table(
            _STATE_ASSESSMENT_ADDRESS,
            [
                {
                    "state_code": "US_MO",
                    "person_id": 4,
                    "assessment_date": date(2025, 1, 20),
                    "assessment_type": "ORAS_COMMUNITY_SUPERVISION",
                },
            ],
        )
        self.load_rows_into_table(
            _COMPARTMENT_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_MO",
                    "person_id": 4,
                    "start_date": date(2024, 12, 1),
                    "compartment_level_1": "LIBERTY",
                },
            ],
        )
        self.run_query_test(
            f"""
            SELECT start_date, end_date, meets_criteria
            FROM ({VIEW_BUILDER.build().view_query})
            """,
            expected_result=[],
        )

    def test_no_qualifying_assessment_is_due_from_supervision_start_with_60_day_due_date(
        self,
    ) -> None:
        self.load_rows_into_table(
            _SUPER_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_MO",
                    "person_id": 2,
                    "start_date": date(2025, 6, 1),
                    "end_date_exclusive": None,
                },
            ],
        )
        self.load_rows_into_table(_STATE_ASSESSMENT_ADDRESS, [])
        self.load_rows_into_table(_COMPARTMENT_SESSIONS_ADDRESS, [])
        self.run_query_test(
            f"""
            SELECT
                start_date,
                end_date,
                meets_criteria,
                SAFE_CAST(JSON_VALUE(reason_v2, '$.assessment_due_date') AS DATE)
                    AS assessment_due_date
            FROM ({VIEW_BUILDER.build().view_query})
            """,
            expected_result=[
                {
                    "start_date": date(2025, 6, 1),
                    "end_date": None,
                    "meets_criteria": True,
                    "assessment_due_date": date(2025, 7, 31),
                },
            ],
        )
