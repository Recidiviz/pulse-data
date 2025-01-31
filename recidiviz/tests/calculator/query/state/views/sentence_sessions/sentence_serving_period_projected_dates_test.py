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
"""Tests the sentence_serving_period_projected_date_sessions view in sentence_sessions."""
from datetime import date, datetime

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_serving_period import (
    SENTENCE_SERVING_PERIOD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_serving_period_projected_dates import (
    SENTENCE_SERVING_PERIOD_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentences_preprocessed import (
    SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entity_table,
)
from recidiviz.persistence.entity.normalized_entities_utils import (
    queryable_address_for_normalized_entity,
)
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
)


class SentenceServingPeriodProjectedDatesTest(SimpleBigQueryViewBuilderTestCase):
    """Tests the SENTENCE_SERVING_PERIOD_PROJECTED_DATES_VIEW_BUILDER."""

    serving_period_address = SENTENCE_SERVING_PERIOD_VIEW_BUILDER.table_for_query
    sentence_length_address = queryable_address_for_normalized_entity(
        normalized_entities.NormalizedStateSentenceLength
    )
    sentences_preprocessed_address = SENTENCES_PREPROCESSED_VIEW_BUILDER.table_for_query

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return SENTENCE_SERVING_PERIOD_PROJECTED_DATES_VIEW_BUILDER

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        return {
            self.serving_period_address: [
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("sentence_id", int),
                schema_field_for_type("start_date", date),
                schema_field_for_type("end_date_exclusive", date),
            ],
            self.sentence_length_address: get_bq_schema_for_entity_table(
                normalized_entities, "state_sentence_length"
            ),
            self.sentences_preprocessed_address: [
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("sentence_id", int),
                schema_field_for_type("min_sentence_length_days_calculated", int),
                schema_field_for_type("max_sentence_length_days_calculated", int),
                schema_field_for_type("parole_eligibility_date", date),
                schema_field_for_type("projected_completion_date_min", date),
                schema_field_for_type("projected_completion_date_max", date),
            ],
        }

    def test_single_period_single_sentence_nested_within_date_session(self) -> None:
        """
        Test that when the projected date sessions extend beyond the serving period in either direction that the output
        gets clipped to the serving period.
        """
        serving_periods_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
            },
        ]

        sentence_length_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "length_update_datetime": datetime(2000, 1, 1, 12, 30),
                "parole_eligibility_date_external": None,
                "projected_parole_release_date_external": None,
                "projected_completion_date_min_external": None,
                "projected_completion_date_max_external": date(2025, 1, 1),
            },
        ]

        sentences_preprocessed_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
            },
        ]

        expected_output = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": date(2025, 1, 1),
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentence_length_address: sentence_length_data,
                self.sentences_preprocessed_address: sentences_preprocessed_data,
            },
            expected_output,
        )

    def test_date_session_starts_after_serving_period_end(self) -> None:
        """
        Test that when a projected date session starts after the serving period ends that the serving period is not
        hydrated with the projected date session values.
        """
        serving_periods_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
            },
        ]

        sentence_length_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "length_update_datetime": datetime(2018, 1, 1, 12, 30),
                "parole_eligibility_date_external": None,
                "projected_parole_release_date_external": None,
                "projected_completion_date_min_external": None,
                "projected_completion_date_max_external": date(2025, 1, 1),
            },
        ]

        sentences_preprocessed_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
            },
        ]

        expected_output = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentence_length_address: sentence_length_data,
                self.sentences_preprocessed_address: sentences_preprocessed_data,
            },
            expected_output,
        )

    def test_single_period_single_group_encompass_date_session(self) -> None:
        """
        Test that when the projected date sessions are nested within a serving period that the output includes the
        entire serving period with null projected dates.
        """
        serving_periods_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2018, 1, 1),
            },
        ]

        sentences_preprocessed_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
            },
        ]

        sentence_length_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "length_update_datetime": datetime(2016, 1, 1, 12, 30),
                "parole_eligibility_date_external": None,
                "projected_parole_release_date_external": None,
                "projected_completion_date_min_external": None,
                "projected_completion_date_max_external": date(2025, 1, 1),
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "length_update_datetime": datetime(2017, 1, 1, 12, 30),
                "parole_eligibility_date_external": None,
                "projected_parole_release_date_external": None,
                "projected_completion_date_min_external": None,
                "projected_completion_date_max_external": None,
            },
        ]

        expected_output = [
            # first session within serving period has no projected dates
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2016, 1, 1),
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
            # second session within serving period does have projected date
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2016, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": date(2025, 1, 1),
            },
            # last session within serving period does not have projected date
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2017, 1, 1),
                "end_date_exclusive": date(2018, 1, 1),
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentence_length_address: sentence_length_data,
                self.sentences_preprocessed_address: sentences_preprocessed_data,
            },
            expected_output,
        )

    def test_single_period_single_sentence_projected_date_change(self) -> None:
        """
        Test that projected date changes update the output
        """
        serving_periods_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2018, 1, 1),
            },
        ]

        sentence_length_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "length_update_datetime": datetime(2015, 1, 1, 12, 30),
                "parole_eligibility_date_external": None,
                "projected_parole_release_date_external": None,
                "projected_completion_date_min_external": None,
                "projected_completion_date_max_external": date(2025, 1, 1),
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "length_update_datetime": datetime(2017, 1, 1, 12, 30),
                "parole_eligibility_date_external": None,
                "projected_parole_release_date_external": None,
                "projected_completion_date_min_external": None,
                "projected_completion_date_max_external": date(2024, 1, 1),
            },
        ]

        sentences_preprocessed_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
            },
        ]

        expected_output = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": date(2025, 1, 1),
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2017, 1, 1),
                "end_date_exclusive": date(2018, 1, 1),
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": date(2024, 1, 1),
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentence_length_address: sentence_length_data,
                self.sentences_preprocessed_address: sentences_preprocessed_data,
            },
            expected_output,
        )

    def test_overlapping_sentences(self) -> None:
        """
        Test that overlapping sentences within a group get aggregated in the sentence id array
        """
        serving_periods_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 456,
                "start_date": date(2016, 1, 1),
                "end_date_exclusive": date(2018, 1, 1),
            },
        ]

        sentence_length_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "length_update_datetime": datetime(2015, 1, 1),
                "parole_eligibility_date_external": None,
                "projected_parole_release_date_external": None,
                "projected_completion_date_min_external": None,
                "projected_completion_date_max_external": date(2025, 1, 1),
            },
        ]

        sentences_preprocessed_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
            },
        ]

        expected_output = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": date(2025, 1, 1),
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 456,
                "start_date": date(2016, 1, 1),
                "end_date_exclusive": date(2018, 1, 1),
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentence_length_address: sentence_length_data,
                self.sentences_preprocessed_address: sentences_preprocessed_data,
            },
            expected_output,
        )
