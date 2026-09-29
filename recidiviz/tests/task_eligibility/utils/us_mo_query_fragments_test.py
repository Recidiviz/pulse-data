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
"""Unit tests for query fragments for US_MO"""
from datetime import date

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.task_eligibility.utils.us_mo_query_fragments import (
    us_mo_recurring_oras_assessment_criteria_view_builder,
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
    bigquery.SchemaField("sequence_num", "INTEGER"),
]


class TestUsMoRecurringOrasAssessmentCriteriaViewBuilder(BigQueryEmulatorTestCase):
    """Tests the assessment_lookback_days date math in
    us_mo_recurring_oras_assessment_criteria_view_builder: the lookback window
    itself, its clamp to the prior supervision episode's end, and the dedup
    when two pre-episode assessments land on the same clamped span_start."""

    def setUp(self) -> None:
        super().setUp()
        self.create_mock_table(
            address=_STATE_ASSESSMENT_ADDRESS, schema=_STATE_ASSESSMENT_SCHEMA
        )

    @staticmethod
    def _view_query(supervision_spans_query: str) -> str:
        view_builder = us_mo_recurring_oras_assessment_criteria_view_builder(
            criteria_name="US_MO_TEST_ORAS_CRITERIA",
            description="Test criterion for lookback/clamp/dedup behavior.",
            assessment_type="ORAS_COMMUNITY_SUPERVISION",
            assessment_display_name="ORAS",
            reassessment_cadence_months=12,
            supervision_spans_query=supervision_spans_query,
            assessment_lookback_days=365,
        )
        return view_builder.build().view_query

    def test_pre_episode_assessment_within_lookback_satisfies_episode(self) -> None:
        self.load_rows_into_table(
            _STATE_ASSESSMENT_ADDRESS,
            [
                {
                    "state_code": "US_MO",
                    "person_id": 1,
                    "assessment_date": date(2024, 3, 1),
                    "assessment_type": "ORAS_COMMUNITY_SUPERVISION",
                    "sequence_num": 1,
                },
            ],
        )
        self.run_query_test(
            f"""
            SELECT
                start_date,
                SAFE_CAST(JSON_VALUE(reason_v2, '$.most_recent_assessment_date') AS DATE)
                    AS most_recent_assessment_date
            FROM ({self._view_query(
                "SELECT 'US_MO' AS state_code, 1 AS person_id, "
                "DATE('2024-06-01') AS start_date, "
                "CAST(NULL AS DATE) AS end_date"
            )})
            WHERE NOT meets_criteria
            ORDER BY start_date
            """,
            expected_result=[
                {
                    "start_date": date(2024, 6, 1),
                    "most_recent_assessment_date": date(2024, 3, 1),
                },
            ],
        )

    def test_assessment_outside_lookback_does_not_satisfy_episode(self) -> None:
        # The assessment is more than 365 days before supervision start, so
        # it falls outside the lookback window and the episode gets no row
        # at all from this criterion (it falls through to the default).
        self.load_rows_into_table(
            _STATE_ASSESSMENT_ADDRESS,
            [
                {
                    "state_code": "US_MO",
                    "person_id": 4,
                    "assessment_date": date(2023, 1, 1),
                    "assessment_type": "ORAS_COMMUNITY_SUPERVISION",
                    "sequence_num": 1,
                },
            ],
        )
        self.run_query_test(
            f"""
            SELECT start_date
            FROM ({self._view_query(
                "SELECT 'US_MO' AS state_code, 4 AS person_id, "
                "DATE('2024-06-01') AS start_date, "
                "CAST(NULL AS DATE) AS end_date"
            )})
            """,
            expected_result=[],
        )

    def test_lookback_is_clamped_to_prior_episode_end(self) -> None:
        # An assessment from before the prior episode ended shouldn't reach
        # forward into the next episode, even if it falls within the lookback
        # window; an assessment taken in the gap between episodes should.
        self.load_rows_into_table(
            _STATE_ASSESSMENT_ADDRESS,
            [
                {
                    "state_code": "US_MO",
                    "person_id": 2,
                    "assessment_date": date(2023, 11, 1),
                    "assessment_type": "ORAS_COMMUNITY_SUPERVISION",
                    "sequence_num": 1,
                },
                {
                    "state_code": "US_MO",
                    "person_id": 2,
                    "assessment_date": date(2024, 2, 1),
                    "assessment_type": "ORAS_COMMUNITY_SUPERVISION",
                    "sequence_num": 2,
                },
            ],
        )
        self.run_query_test(
            f"""
            SELECT
                start_date,
                SAFE_CAST(JSON_VALUE(reason_v2, '$.most_recent_assessment_date') AS DATE)
                    AS most_recent_assessment_date
            FROM ({self._view_query(
                "SELECT 'US_MO' AS state_code, 2 AS person_id, "
                "DATE('2023-06-01') AS start_date, DATE('2024-01-01') AS end_date "
                "UNION ALL "
                "SELECT 'US_MO' AS state_code, 2 AS person_id, "
                "DATE('2024-03-01') AS start_date, CAST(NULL AS DATE) AS end_date"
            )})
            WHERE NOT meets_criteria
            ORDER BY start_date
            """,
            expected_result=[
                {
                    "start_date": date(2023, 11, 1),
                    "most_recent_assessment_date": date(2023, 11, 1),
                },
                {
                    "start_date": date(2024, 3, 1),
                    "most_recent_assessment_date": date(2024, 2, 1),
                },
            ],
        )

    def test_two_pre_episode_assessments_dedup_to_the_later_one(self) -> None:
        # Both assessments predate the episode and fall within the lookback
        # window, so both get clamped to the same span_start; the dedup
        # should keep the more current one.
        self.load_rows_into_table(
            _STATE_ASSESSMENT_ADDRESS,
            [
                {
                    "state_code": "US_MO",
                    "person_id": 3,
                    "assessment_date": date(2024, 4, 1),
                    "assessment_type": "ORAS_COMMUNITY_SUPERVISION",
                    "sequence_num": 1,
                },
                {
                    "state_code": "US_MO",
                    "person_id": 3,
                    "assessment_date": date(2024, 5, 1),
                    "assessment_type": "ORAS_COMMUNITY_SUPERVISION",
                    "sequence_num": 2,
                },
            ],
        )
        self.run_query_test(
            f"""
            SELECT
                start_date,
                SAFE_CAST(JSON_VALUE(reason_v2, '$.most_recent_assessment_date') AS DATE)
                    AS most_recent_assessment_date
            FROM ({self._view_query(
                "SELECT 'US_MO' AS state_code, 3 AS person_id, "
                "DATE('2024-06-01') AS start_date, "
                "CAST(NULL AS DATE) AS end_date"
            )})
            WHERE NOT meets_criteria
            ORDER BY start_date
            """,
            expected_result=[
                {
                    "start_date": date(2024, 6, 1),
                    "most_recent_assessment_date": date(2024, 5, 1),
                },
            ],
        )
