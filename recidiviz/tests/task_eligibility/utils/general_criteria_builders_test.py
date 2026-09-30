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
# ============================================================================
"""Unit tests for general_criteria_builders."""
from datetime import date
from typing import Any

from google.cloud import bigquery

from recidiviz.calculator.query.state.views.classification.recommended_classification_spans.us_mi.reclass_2026_policy import (
    VIEW_BUILDER as US_MI_RECLASS_2026_POLICY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.classification.recommended_classification_spans_big_query_view_builder import (
    CustodyLevelVsRecommended,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    custody_level_vs_recommended_criteria_view_builder,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

# The criteria takes its state from this policy and filters to it, so the rows below
# use US_MI rather than a fake state code.
_POLICY_SPANS_ADDRESS = US_MI_RECLASS_2026_POLICY_VIEW_BUILDER.table_for_query
_POLICY_SPANS_SCHEMA = [
    bigquery.SchemaField("state_code", "STRING"),
    bigquery.SchemaField("person_id", "INTEGER"),
    bigquery.SchemaField("start_date", "DATE"),
    bigquery.SchemaField("end_date_exclusive", "DATE"),
    bigquery.SchemaField("custody_level", "STRING"),
    bigquery.SchemaField("recommended_custody_level", "STRING"),
    bigquery.SchemaField("custody_level_vs_recommended", "STRING"),
]


class TestCustodyLevelVsRecommendedCriteriaViewBuilder(BigQueryEmulatorTestCase):
    """Tests the upcoming_eligibility_date logic in
    custody_level_vs_recommended_criteria_view_builder."""

    def setUp(self) -> None:
        super().setUp()
        self.create_mock_table(
            address=_POLICY_SPANS_ADDRESS, schema=_POLICY_SPANS_SCHEMA
        )

    def _run_criteria_test(
        self,
        *,
        policy_spans: list[dict[str, Any]],
        expected_result: list[dict[str, Any]],
    ) -> None:
        self.load_rows_into_table(_POLICY_SPANS_ADDRESS, policy_spans)
        view_query = (
            custody_level_vs_recommended_criteria_view_builder(
                criteria_name="US_MI_TEST_CUSTODY_LEVEL_HIGHER_THAN_RECOMMENDED",
                description="Test criteria for upcoming_eligibility_date behavior.",
                recommended_classification_spans_view_builder=(
                    US_MI_RECLASS_2026_POLICY_VIEW_BUILDER
                ),
                comparison=CustodyLevelVsRecommended.HIGHER_THAN_RECOMMENDED,
            )
            .build()
            .view_query
        )
        self.run_query_test(
            f"""
            SELECT
                start_date,
                end_date,
                meets_criteria,
                CAST(JSON_VALUE(reason_v2, '$.upcoming_eligibility_date') AS DATE)
                    AS upcoming_eligibility_date
            FROM ({view_query})
            ORDER BY start_date
            """,
            expected_result=expected_result,
        )

    def test_met_spans_separated_by_gap_each_get_own_start(self) -> None:
        """Regression test: the first met span used to report the second met span's
        start date, because the lookahead to the next span ignored the gap."""
        self._run_criteria_test(
            policy_spans=[
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 2, 1),
                    "custody_level": "MAXIMUM",
                    "recommended_custody_level": "MINIMUM",
                    "custody_level_vs_recommended": "HIGHER_THAN_RECOMMENDED",
                },
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2024, 3, 1),
                    "end_date_exclusive": date(2024, 4, 1),
                    "custody_level": "MAXIMUM",
                    "recommended_custody_level": "MINIMUM",
                    "custody_level_vs_recommended": "HIGHER_THAN_RECOMMENDED",
                },
            ],
            expected_result=[
                {
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 1),
                    "meets_criteria": True,
                    "upcoming_eligibility_date": date(2024, 1, 1),
                },
                {
                    "start_date": date(2024, 3, 1),
                    "end_date": date(2024, 4, 1),
                    "meets_criteria": True,
                    "upcoming_eligibility_date": date(2024, 3, 1),
                },
            ],
        )

    def test_adjacent_met_spans_with_different_custody_levels_share_run_start(
        self,
    ) -> None:
        self._run_criteria_test(
            policy_spans=[
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 2, 1),
                    "custody_level": "MAXIMUM",
                    "recommended_custody_level": "MINIMUM",
                    "custody_level_vs_recommended": "HIGHER_THAN_RECOMMENDED",
                },
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2024, 2, 1),
                    "end_date_exclusive": date(2024, 3, 1),
                    "custody_level": "CLOSE",
                    "recommended_custody_level": "MINIMUM",
                    "custody_level_vs_recommended": "HIGHER_THAN_RECOMMENDED",
                },
            ],
            expected_result=[
                {
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 1),
                    "meets_criteria": True,
                    "upcoming_eligibility_date": date(2024, 1, 1),
                },
                {
                    "start_date": date(2024, 2, 1),
                    "end_date": date(2024, 3, 1),
                    "meets_criteria": True,
                    # Still eligible without a break since 2024-01-01.
                    "upcoming_eligibility_date": date(2024, 1, 1),
                },
            ],
        )

    def test_unmet_span_followed_by_adjacent_met_span_gets_met_start(self) -> None:
        self._run_criteria_test(
            policy_spans=[
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 2, 1),
                    "custody_level": "MINIMUM",
                    "recommended_custody_level": "MINIMUM",
                    "custody_level_vs_recommended": "AS_RECOMMENDED",
                },
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2024, 2, 1),
                    "end_date_exclusive": date(2024, 3, 1),
                    "custody_level": "MAXIMUM",
                    "recommended_custody_level": "MINIMUM",
                    "custody_level_vs_recommended": "HIGHER_THAN_RECOMMENDED",
                },
            ],
            expected_result=[
                {
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 1),
                    "meets_criteria": False,
                    "upcoming_eligibility_date": date(2024, 2, 1),
                },
                {
                    "start_date": date(2024, 2, 1),
                    "end_date": date(2024, 3, 1),
                    "meets_criteria": True,
                    "upcoming_eligibility_date": date(2024, 2, 1),
                },
            ],
        )

    def test_unmet_span_followed_by_met_span_after_gap_gets_met_start(self) -> None:
        self._run_criteria_test(
            policy_spans=[
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 2, 1),
                    "custody_level": "MINIMUM",
                    "recommended_custody_level": "MINIMUM",
                    "custody_level_vs_recommended": "AS_RECOMMENDED",
                },
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2024, 3, 1),
                    "end_date_exclusive": date(2024, 4, 1),
                    "custody_level": "MAXIMUM",
                    "recommended_custody_level": "MINIMUM",
                    "custody_level_vs_recommended": "HIGHER_THAN_RECOMMENDED",
                },
            ],
            expected_result=[
                {
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 1),
                    "meets_criteria": False,
                    "upcoming_eligibility_date": date(2024, 3, 1),
                },
                {
                    "start_date": date(2024, 3, 1),
                    "end_date": date(2024, 4, 1),
                    "meets_criteria": True,
                    "upcoming_eligibility_date": date(2024, 3, 1),
                },
            ],
        )

    def test_unmet_span_between_met_spans_starts_new_run(self) -> None:
        self._run_criteria_test(
            policy_spans=[
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2024, 1, 1),
                    "end_date_exclusive": date(2024, 2, 1),
                    "custody_level": "MAXIMUM",
                    "recommended_custody_level": "MINIMUM",
                    "custody_level_vs_recommended": "HIGHER_THAN_RECOMMENDED",
                },
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2024, 2, 1),
                    "end_date_exclusive": date(2024, 3, 1),
                    "custody_level": "MINIMUM",
                    "recommended_custody_level": "MINIMUM",
                    "custody_level_vs_recommended": "AS_RECOMMENDED",
                },
                {
                    "state_code": "US_MI",
                    "person_id": 1,
                    "start_date": date(2024, 3, 1),
                    "end_date_exclusive": date(2024, 4, 1),
                    "custody_level": "MAXIMUM",
                    "recommended_custody_level": "MINIMUM",
                    "custody_level_vs_recommended": "HIGHER_THAN_RECOMMENDED",
                },
            ],
            expected_result=[
                {
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 1),
                    "meets_criteria": True,
                    "upcoming_eligibility_date": date(2024, 1, 1),
                },
                {
                    "start_date": date(2024, 2, 1),
                    "end_date": date(2024, 3, 1),
                    "meets_criteria": False,
                    "upcoming_eligibility_date": date(2024, 3, 1),
                },
                {
                    "start_date": date(2024, 3, 1),
                    "end_date": date(2024, 4, 1),
                    "meets_criteria": True,
                    "upcoming_eligibility_date": date(2024, 3, 1),
                },
            ],
        )
