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
"""Tests the sentence_inferred_group_projected_dates view in sentence_sessions."""
import datetime

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_group_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_ID,
)
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_ID,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_inferred_group_projected_dates import (
    SENTENCE_INFERRED_GROUP_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
)

_SCHEMA = [
    bigquery.SchemaField("state_code", "STRING"),
    bigquery.SchemaField("person_id", "INTEGER"),
    bigquery.SchemaField("sentence_inferred_group_id", "INTEGER"),
    bigquery.SchemaField("inferred_group_update_datetime", "DATETIME"),
    bigquery.SchemaField("parole_eligibility_date", "DATE"),
    bigquery.SchemaField("projected_parole_release_date", "DATE"),
    bigquery.SchemaField("projected_full_term_release_date_min", "DATE"),
    bigquery.SchemaField("projected_full_term_release_date_max", "DATE"),
]


class InferredProjectedDatesTest(SimpleBigQueryViewBuilderTestCase):
    """Tests the SENTENCE_INFERRED_GROUP_PROJECTED_DATES_VIEW_BUILDER."""

    agg_sentence_address = BigQueryAddress.from_str(
        f"sentence_sessions.{INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_ID}_materialized"
    )
    agg_group_address = BigQueryAddress.from_str(
        f"sentence_sessions.{INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_ID}_materialized"
    )

    state_code = StateCode.US_XX
    person_id = hash("TEST-PERSON-1")
    inferred_group_id = 42

    critical_date_1 = datetime.datetime(2022, 1, 1, 6)
    critical_date_2 = datetime.datetime(2022, 2, 1, 12, 30)
    critical_date_3 = datetime.datetime(2022, 3, 4)

    projected_date_4 = datetime.date(2025, 12, 1)
    projected_date_3 = datetime.date(2025, 8, 14)
    projected_date_2 = datetime.date(2024, 9, 1)
    projected_date_1 = datetime.date(2024, 5, 1)

    # Show full diffs on test failure
    maxDiff = None

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return SENTENCE_INFERRED_GROUP_PROJECTED_DATES_VIEW_BUILDER

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        return {
            self.agg_sentence_address: _SCHEMA,
            self.agg_group_address: _SCHEMA,
        }

    def test_no_aggregated_group_data(self) -> None:
        """Tests the view works when there is only aggregated sentence level data."""
        agg_sentence_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_1,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_4,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_2,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_3,
            },
        ]
        self.run_simple_view_builder_query_test_from_data(
            {self.agg_sentence_address: agg_sentence_data, self.agg_group_address: []},
            agg_sentence_data,
        )

    def test_only_aggregated_group_data(self) -> None:
        """Tests the view works when there is only aggregated sentence group level data."""
        agg_group_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_1,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_4,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_2,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_3,
            },
        ]
        self.run_simple_view_builder_query_test_from_data(
            {self.agg_sentence_address: [], self.agg_group_address: agg_group_data},
            agg_group_data,
        )

    def test_non_overlapping_data(self) -> None:
        """Tests the view works when aggregated sentence and aggregated group level data don't share critical dates."""
        agg_group_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_1,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_4,
            },
        ]
        agg_sentence_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_2,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_3,
            },
        ]
        self.run_simple_view_builder_query_test_from_data(
            {
                self.agg_sentence_address: agg_sentence_data,
                self.agg_group_address: agg_group_data,
            },
            agg_group_data + agg_sentence_data,
        )

    def test_overlapping_data(self) -> None:
        """Tests the view works when aggregated sentence and aggregated group level data share critical dates."""
        agg_group_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_1,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_4,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_2,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_3,
            },
        ]
        agg_sentence_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_1,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_3,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_2,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_4,
            },
        ]
        expected_output = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_1,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_4,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_2,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_4,
            },
        ]
        self.run_simple_view_builder_query_test_from_data(
            {
                self.agg_sentence_address: agg_sentence_data,
                self.agg_group_address: agg_group_data,
            },
            expected_output,
        )

    def test_overlapping_data_some_aggregated_sentence_data_is_all_none(self) -> None:
        """
        Tests the view works when aggregated sentence and aggregated group level data share critical dates.

        It's possible for all sentences in a given group to be suspended, nulling out projected dates for
        that period of time. If the state has still provided group level dates for that time, we'll use
        the group level dates. This will only happen if the critical dates are the same.
        """
        agg_group_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_1,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_4,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_3,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_3,
            },
        ]
        agg_sentence_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_2,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_3,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
        ]
        expected_output = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_1,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_4,
            },
            # Sentence level is nulled out, but not an overlapping date
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_2,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
            # Sentence level is nulled out, with an overlapping date
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_3,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_3,
            },
        ]
        self.run_simple_view_builder_query_test_from_data(
            {
                self.agg_sentence_address: agg_sentence_data,
                self.agg_group_address: agg_group_data,
            },
            expected_output,
        )
