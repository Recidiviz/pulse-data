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
"""Tests the US_IX_NOT_WITHIN_6_MONTHS_OF_INITIAL_CLASSIFICATION criterion."""
from datetime import date

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.sessions.compartment_level_1_super_sessions import (
    COMPARTMENT_LEVEL_1_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix.not_within_6_months_of_initial_classification import (
    VIEW_BUILDER,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

_STATE_ASSESSMENT_ADDRESS = BigQueryAddress(
    dataset_id="normalized_state", table_id="state_assessment"
)
_STATE_ASSESSMENT_SCHEMA = [
    bigquery.SchemaField("state_code", "STRING"),
    bigquery.SchemaField("person_id", "INTEGER"),
    bigquery.SchemaField("assessment_type", "STRING"),
    bigquery.SchemaField("assessment_date", "DATE"),
    bigquery.SchemaField("assessment_metadata", "STRING"),
]

_SUPER_SESSIONS_ADDRESS = (
    COMPARTMENT_LEVEL_1_SUPER_SESSIONS_VIEW_BUILDER.table_for_query
)
_SUPER_SESSIONS_SCHEMA = [
    bigquery.SchemaField("state_code", "STRING"),
    bigquery.SchemaField("person_id", "INTEGER"),
    bigquery.SchemaField("compartment_level_1", "STRING"),
    bigquery.SchemaField("start_date", "DATE"),
    bigquery.SchemaField("end_date_exclusive", "DATE"),
]


class TestNotWithin6MonthsOfInitialClassification(BigQueryEmulatorTestCase):
    """Tests the US_IX_NOT_WITHIN_6_MONTHS_OF_INITIAL_CLASSIFICATION criterion."""

    def setUp(self) -> None:
        super().setUp()
        self.assertSetEqual(
            VIEW_BUILDER.build().parent_tables,
            {_STATE_ASSESSMENT_ADDRESS, _SUPER_SESSIONS_ADDRESS},
        )
        self.create_mock_table(_STATE_ASSESSMENT_ADDRESS, _STATE_ASSESSMENT_SCHEMA)
        self.create_mock_table(_SUPER_SESSIONS_ADDRESS, _SUPER_SESSIONS_SCHEMA)

    def _run_criterion_query_test(self, expected_result: list[dict]) -> None:
        self.run_query_test(
            f"""
            SELECT * EXCEPT (reason), TO_JSON_STRING(reason) AS reason
            FROM ({VIEW_BUILDER.build().view_query})
            ORDER BY person_id, start_date
            """,
            expected_result=expected_result,
        )

    def test_eligibility_blocked_for_six_months_within_open_stint(self) -> None:
        self.load_rows_into_table(
            _STATE_ASSESSMENT_ADDRESS,
            [
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "assessment_type": "RSLS",
                    "assessment_date": date(2026, 7, 21),
                    "assessment_metadata": '{"CLASSIFICATIONTYPE": "INITIAL"}',
                },
            ],
        )
        self.load_rows_into_table(
            _SUPER_SESSIONS_ADDRESS,
            [
                # One row covers both the parole board hold and the move to
                # general population that followed it.
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "compartment_level_1": "INCARCERATION",
                    "start_date": date(2026, 5, 15),
                    "end_date_exclusive": None,
                },
            ],
        )
        self._run_criterion_query_test(
            expected_result=[
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "start_date": date(2026, 7, 21),
                    "end_date": date(2027, 1, 21),
                    "meets_criteria": False,
                    "reason": '{"initial_classification_date":"2026-07-21",'
                    '"eligible_date":"2027-01-21"}',
                    "reason_v2": {
                        "initial_classification_date": "2026-07-21",
                        "eligible_date": "2027-01-21",
                    },
                },
            ],
        )

    def test_eligibility_block_ends_when_incarceration_stint_ends(self) -> None:
        self.load_rows_into_table(
            _STATE_ASSESSMENT_ADDRESS,
            [
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "assessment_type": "RSLS",
                    "assessment_date": date(2026, 1, 10),
                    "assessment_metadata": '{"CLASSIFICATIONTYPE": "INITIAL"}',
                },
            ],
        )
        self.load_rows_into_table(
            _SUPER_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "compartment_level_1": "INCARCERATION",
                    "start_date": date(2025, 12, 1),
                    "end_date_exclusive": date(2026, 3, 1),
                },
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "compartment_level_1": "SUPERVISION",
                    "start_date": date(2026, 3, 1),
                    "end_date_exclusive": date(2026, 5, 15),
                },
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "compartment_level_1": "INCARCERATION",
                    "start_date": date(2026, 5, 15),
                    "end_date_exclusive": None,
                },
            ],
        )
        self._run_criterion_query_test(
            expected_result=[
                # Release to supervision on 2026-03-01 ends the block early, so the
                # classification does not block eligibility in the later stint.
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "start_date": date(2026, 1, 10),
                    "end_date": date(2026, 3, 1),
                    "meets_criteria": False,
                    "reason": '{"initial_classification_date":"2026-01-10",'
                    '"eligible_date":"2026-07-10"}',
                    "reason_v2": {
                        "initial_classification_date": "2026-01-10",
                        "eligible_date": "2026-07-10",
                    },
                },
            ],
        )

    def test_classification_during_supervision_does_not_block_eligibility(self) -> None:
        self.load_rows_into_table(
            _STATE_ASSESSMENT_ADDRESS,
            [
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "assessment_type": "RSLS",
                    "assessment_date": date(2026, 7, 21),
                    "assessment_metadata": '{"CLASSIFICATIONTYPE": "INITIAL"}',
                },
            ],
        )
        self.load_rows_into_table(
            _SUPER_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "compartment_level_1": "SUPERVISION",
                    "start_date": date(2026, 5, 15),
                    "end_date_exclusive": None,
                },
            ],
        )
        self._run_criterion_query_test(expected_result=[])

    def test_out_of_state_classification_blocks_eligibility_for_six_months(
        self,
    ) -> None:
        self.load_rows_into_table(
            _STATE_ASSESSMENT_ADDRESS,
            [
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "assessment_type": "RSLS",
                    "assessment_date": date(2026, 7, 21),
                    "assessment_metadata": '{"CLASSIFICATIONTYPE": "INITIAL"}',
                },
            ],
        )
        self.load_rows_into_table(
            _SUPER_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "compartment_level_1": "INCARCERATION_OUT_OF_STATE",
                    "start_date": date(2026, 5, 15),
                    "end_date_exclusive": None,
                },
            ],
        )
        self._run_criterion_query_test(
            expected_result=[
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "start_date": date(2026, 7, 21),
                    "end_date": date(2027, 1, 21),
                    "meets_criteria": False,
                    "reason": '{"initial_classification_date":"2026-07-21",'
                    '"eligible_date":"2027-01-21"}',
                    "reason_v2": {
                        "initial_classification_date": "2026-07-21",
                        "eligible_date": "2027-01-21",
                    },
                },
            ],
        )

    def test_scheduled_classification_does_not_block_eligibility(self) -> None:
        self.load_rows_into_table(
            _STATE_ASSESSMENT_ADDRESS,
            [
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "assessment_type": "RSLS",
                    "assessment_date": date(2026, 7, 21),
                    "assessment_metadata": '{"CLASSIFICATIONTYPE": "SCHEDULED"}',
                },
            ],
        )
        self.load_rows_into_table(
            _SUPER_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "compartment_level_1": "INCARCERATION",
                    "start_date": date(2026, 5, 15),
                    "end_date_exclusive": None,
                },
            ],
        )
        self._run_criterion_query_test(expected_result=[])

    def test_other_assessment_type_does_not_block_eligibility(self) -> None:
        self.load_rows_into_table(
            _STATE_ASSESSMENT_ADDRESS,
            [
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "assessment_type": "LSIR",
                    "assessment_date": date(2026, 7, 21),
                    "assessment_metadata": '{"CLASSIFICATIONTYPE": "INITIAL"}',
                },
            ],
        )
        self.load_rows_into_table(
            _SUPER_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "compartment_level_1": "INCARCERATION",
                    "start_date": date(2026, 5, 15),
                    "end_date_exclusive": None,
                },
            ],
        )
        self._run_criterion_query_test(expected_result=[])

    def test_later_initial_classification_dates_apply_where_blocks_overlap(
        self,
    ) -> None:
        self.load_rows_into_table(
            _STATE_ASSESSMENT_ADDRESS,
            [
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "assessment_type": "RSLS",
                    "assessment_date": date(2026, 1, 10),
                    "assessment_metadata": '{"CLASSIFICATIONTYPE": "INITIAL"}',
                },
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "assessment_type": "RSLS",
                    "assessment_date": date(2026, 3, 1),
                    "assessment_metadata": '{"CLASSIFICATIONTYPE": "INITIAL"}',
                },
            ],
        )
        self.load_rows_into_table(
            _SUPER_SESSIONS_ADDRESS,
            [
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "compartment_level_1": "INCARCERATION",
                    "start_date": date(2025, 12, 1),
                    "end_date_exclusive": None,
                },
            ],
        )
        self._run_criterion_query_test(
            expected_result=[
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "start_date": date(2026, 1, 10),
                    "end_date": date(2026, 3, 1),
                    "meets_criteria": False,
                    "reason": '{"initial_classification_date":"2026-01-10",'
                    '"eligible_date":"2026-07-10"}',
                    "reason_v2": {
                        "initial_classification_date": "2026-01-10",
                        "eligible_date": "2026-07-10",
                    },
                },
                # The two blocks overlap from 2026-03-01 to 2026-07-10. The later
                # classification's dates apply there and until its own block ends.
                {
                    "state_code": "US_IX",
                    "person_id": 1,
                    "start_date": date(2026, 3, 1),
                    "end_date": date(2026, 9, 1),
                    "meets_criteria": False,
                    "reason": '{"initial_classification_date":"2026-03-01",'
                    '"eligible_date":"2026-09-01"}',
                    "reason_v2": {
                        "initial_classification_date": "2026-03-01",
                        "eligible_date": "2026-09-01",
                    },
                },
            ],
        )
