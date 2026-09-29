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
"""Unit tests for scored_low_risk_on_last_cst_or_csst.py"""
from datetime import date

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.task_eligibility.criteria.state_specific.us_mo.scored_low_risk_on_last_cst_or_csst import (
    VIEW_BUILDER,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

_ASSESSMENT_SCORE_SESSIONS_ADDRESS = BigQueryAddress(
    dataset_id="sessions", table_id="assessment_score_sessions_materialized"
)
_ASSESSMENT_SCORE_SESSIONS_SCHEMA = [
    bigquery.SchemaField("state_code", "STRING"),
    bigquery.SchemaField("person_id", "INTEGER"),
    bigquery.SchemaField("assessment_date", "DATE"),
    bigquery.SchemaField("assessment_type", "STRING"),
    bigquery.SchemaField("assessment_level", "STRING"),
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


class TestScoredLowRiskOnLastCstOrCsstViewBuilder(BigQueryEmulatorTestCase):
    """Tests the LOW-risk exemption span: how far a LOW CST/CSST score carries
    forward, and where the liberty-gap bound cuts it off."""

    def setUp(self) -> None:
        super().setUp()
        self.create_mock_table(
            address=_ASSESSMENT_SCORE_SESSIONS_ADDRESS,
            schema=_ASSESSMENT_SCORE_SESSIONS_SCHEMA,
        )
        self.create_mock_table(
            address=_COMPARTMENT_SESSIONS_ADDRESS,
            schema=_COMPARTMENT_SESSIONS_SCHEMA,
        )

    def test_low_score_exemption_persists_without_a_liberty_gap(self) -> None:
        # No later assessment and no later LIBERTY session (e.g. a
        # suspension/reinstatement with no full discharge in between), so the
        # exemption stays open indefinitely.
        self.load_rows_into_table(
            _ASSESSMENT_SCORE_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_MO",
                    "person_id": 1,
                    "assessment_date": date(2024, 3, 1),
                    "assessment_type": "ORAS_COMMUNITY_SUPERVISION",
                    "assessment_level": "LOW",
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
                {
                    "start_date": date(2024, 3, 1),
                    "end_date": None,
                    "meets_criteria": True,
                },
            ],
        )

    def test_low_score_exemption_ends_at_next_liberty_session_start(self) -> None:
        # A LIBERTY session (full discharge) after the LOW score cuts the
        # exemption off at the liberty gap's start date, even though the
        # person hasn't had a new CST/CSST.
        self.load_rows_into_table(
            _ASSESSMENT_SCORE_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_MO",
                    "person_id": 2,
                    "assessment_date": date(2024, 3, 1),
                    "assessment_type": "ORAS_COMMUNITY_SUPERVISION_SCREENING",
                    "assessment_level": "LOW",
                },
            ],
        )
        self.load_rows_into_table(
            _COMPARTMENT_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_MO",
                    "person_id": 2,
                    "start_date": date(2025, 1, 1),
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
                    "start_date": date(2024, 3, 1),
                    "end_date": date(2025, 1, 1),
                    "meets_criteria": True,
                },
            ],
        )
