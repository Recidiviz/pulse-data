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
"""Tests the sentence_inferred_group_projected_date_sessions view in sentence_sessions."""
from datetime import date
from typing import Dict, List

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.sentence_sessions.person_projected_date_sessions import (
    PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_inferred_group_projected_date_sessions import (
    SENTENCE_INFERRED_GROUP_PROJECTED_DATE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_projected_date_sessions import (
    SENTENCE_PROJECTED_DATE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_serving_period import (
    SENTENCE_SERVING_PERIOD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentences_and_charges import (
    SENTENCES_AND_CHARGES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentences_preprocessed import (
    SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
)


class InferredGroupServingPeriodProjectedDatesTest(SimpleBigQueryViewBuilderTestCase):
    """Tests the SENTENCE_INFERRED_GROUP_SERVING_PERIOD_PROJECTED_DATES_VIEW_BUILDER."""

    serving_period_address = SENTENCE_SERVING_PERIOD_VIEW_BUILDER.table_for_query
    sentences_and_charges_address = SENTENCES_AND_CHARGES_VIEW_BUILDER.table_for_query
    sentence_group_projected_dates_address = (
        SENTENCE_INFERRED_GROUP_PROJECTED_DATE_SESSIONS_VIEW_BUILDER.table_for_query
    )
    sentence_projected_dates_address = (
        SENTENCE_PROJECTED_DATE_SESSIONS_VIEW_BUILDER.table_for_query
    )
    sentences_preprocessed_address = SENTENCES_PREPROCESSED_VIEW_BUILDER.table_for_query

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER

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
            self.sentences_and_charges_address: [
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("sentence_id", int),
                schema_field_for_type("sentence_inferred_group_id", int),
            ],
            self.sentence_group_projected_dates_address: [
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("sentence_inferred_group_id", int),
                schema_field_for_type("start_date", date),
                schema_field_for_type("end_date_exclusive", date),
                schema_field_for_type("parole_eligibility_date", date),
                schema_field_for_type("projected_parole_release_date", date),
                schema_field_for_type("projected_full_term_release_date_min", date),
                schema_field_for_type("projected_full_term_release_date_max", date),
            ],
            self.sentence_projected_dates_address: [
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("sentence_id", int),
                schema_field_for_type("start_date", date),
                schema_field_for_type("end_date_exclusive", date),
                schema_field_for_type("parole_eligibility_date", date),
                schema_field_for_type("projected_parole_release_date", date),
                schema_field_for_type("projected_full_term_release_date_min", date),
                schema_field_for_type("projected_full_term_release_date_max", date),
                schema_field_for_type("sentence_length_days_min", int),
                schema_field_for_type("sentence_length_days_max", int),
                schema_field_for_type("good_time_days", int),
                schema_field_for_type("earned_time_days", int),
            ],
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

    def test_single_period_single_group_nested_within_date_session(self) -> None:
        """
        Test that when the sentence group projected date sessions extend beyond the serving period in either direction
        that the output gets clipped to the serving period. Additionally check that without sentence level projected
        dates data that the struct captures the sentence being served but does not have sentence level dates hydrated.
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

        sentences_and_charges_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "sentence_inferred_group_id": 888,
            },
        ]

        sentence_group_projected_dates_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2000, 1, 1),
                "end_date_exclusive": None,
                "projected_full_term_release_date_max": date(2025, 1, 1),
            },
        ]

        sentence_projected_dates_data: List[Dict] = []

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
                "sentence_inferred_group_id": 888,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": date(2025, 1, 1),
                "sentence_array": [
                    {
                        "sentence_id": 123,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentences_and_charges_address: sentences_and_charges_data,
                self.sentence_group_projected_dates_address: sentence_group_projected_dates_data,
                self.sentence_projected_dates_address: sentence_projected_dates_data,
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

        sentences_and_charges_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "sentence_inferred_group_id": 888,
            },
        ]

        sentence_group_projected_dates_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2018, 1, 1),
                "end_date_exclusive": None,
                "projected_full_term_release_date_max": date(2025, 1, 1),
            },
        ]

        sentence_projected_dates_data: List[Dict] = []

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
                "sentence_inferred_group_id": 888,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": None,
                "sentence_array": [
                    {
                        "sentence_id": 123,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentences_and_charges_address: sentences_and_charges_data,
                self.sentence_group_projected_dates_address: sentence_group_projected_dates_data,
                self.sentence_projected_dates_address: sentence_projected_dates_data,
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

        sentences_and_charges_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "sentence_inferred_group_id": 888,
            },
        ]

        sentences_preprocessed_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
            },
        ]
        sentence_group_projected_dates_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2016, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": date(2025, 1, 1),
            },
        ]

        sentence_projected_dates_data: List[Dict] = []

        expected_output = [
            # first session within serving period has no projected dates
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2016, 1, 1),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": None,
                "sentence_array": [
                    {
                        "sentence_id": 123,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
            # second session within serving period does have projected date
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2016, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": date(2025, 1, 1),
                "sentence_array": [
                    {
                        "sentence_id": 123,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
            # last session within serving period does not have projected date
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2017, 1, 1),
                "end_date_exclusive": date(2018, 1, 1),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": None,
                "sentence_array": [
                    {
                        "sentence_id": 123,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentences_and_charges_address: sentences_and_charges_data,
                self.sentence_group_projected_dates_address: sentence_group_projected_dates_data,
                self.sentence_projected_dates_address: sentence_projected_dates_data,
                self.sentences_preprocessed_address: sentences_preprocessed_data,
            },
            expected_output,
        )

    def test_single_period_single_group_projected_date_change(self) -> None:
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

        sentences_and_charges_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "sentence_inferred_group_id": 888,
            },
        ]

        sentence_group_projected_dates_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": date(2025, 1, 1),
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2017, 1, 1),
                "end_date_exclusive": date(2018, 1, 1),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": date(2024, 1, 1),
            },
        ]

        sentence_projected_dates_data: List[Dict] = []

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
                "sentence_inferred_group_id": 888,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": date(2025, 1, 1),
                "sentence_array": [
                    {
                        "sentence_id": 123,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2017, 1, 1),
                "end_date_exclusive": date(2018, 1, 1),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": date(2024, 1, 1),
                "sentence_array": [
                    {
                        "sentence_id": 123,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentences_and_charges_address: sentences_and_charges_data,
                self.sentence_group_projected_dates_address: sentence_group_projected_dates_data,
                self.sentence_projected_dates_address: sentence_projected_dates_data,
                self.sentences_preprocessed_address: sentences_preprocessed_data,
            },
            expected_output,
        )

    def test_overlapping_sentences_within_group(self) -> None:
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

        sentences_and_charges_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "sentence_inferred_group_id": 888,
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 456,
                "sentence_inferred_group_id": 888,
            },
        ]

        sentence_group_projected_dates_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2018, 1, 1),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": date(2025, 1, 1),
            },
        ]

        sentence_projected_dates_data: List[Dict] = []

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
                "sentence_inferred_group_id": 888,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2016, 1, 1),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": date(2025, 1, 1),
                "sentence_array": [
                    {
                        "sentence_id": 123,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2016, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": date(2025, 1, 1),
                "sentence_array": [
                    {
                        "sentence_id": 123,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                    {
                        "sentence_id": 456,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2017, 1, 1),
                "end_date_exclusive": date(2018, 1, 1),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": date(2025, 1, 1),
                "sentence_array": [
                    {
                        "sentence_id": 456,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentences_and_charges_address: sentences_and_charges_data,
                self.sentence_group_projected_dates_address: sentence_group_projected_dates_data,
                self.sentence_projected_dates_address: sentence_projected_dates_data,
                self.sentences_preprocessed_address: sentences_preprocessed_data,
            },
            expected_output,
        )

    def test_hydrated_sentence_level_projected_dates(self) -> None:
        """
        Test that when a sentence has hydrated projected dates, that the sentence_array struct gets hydrated with that
        value.
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

        sentences_and_charges_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "sentence_inferred_group_id": 888,
            },
        ]

        sentence_group_projected_dates_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2000, 1, 1),
                "end_date_exclusive": None,
                "projected_full_term_release_date_max": date(2025, 1, 1),
            },
        ]

        sentence_projected_dates_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2000, 1, 1),
                "end_date_exclusive": None,
                "projected_full_term_release_date_min": date(2024, 1, 1),
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
                "sentence_inferred_group_id": 888,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": date(2025, 1, 1),
                "sentence_array": [
                    {
                        "sentence_id": 123,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": date(
                            2024, 1, 1
                        ),
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentences_and_charges_address: sentences_and_charges_data,
                self.sentence_group_projected_dates_address: sentence_group_projected_dates_data,
                self.sentence_projected_dates_address: sentence_projected_dates_data,
                self.sentences_preprocessed_address: sentences_preprocessed_data,
            },
            expected_output,
        )

    def test_change_in_sentence_level_projected_dates(self) -> None:
        """
        Test that when there is a sentence-level change in projected dates that a new record is created (even without
        any change in serving period or group level date).
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

        sentences_and_charges_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "sentence_inferred_group_id": 888,
            },
        ]

        sentence_group_projected_dates_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2000, 1, 1),
                "end_date_exclusive": None,
                "projected_full_term_release_date_max": date(2025, 1, 1),
            },
        ]

        sentence_projected_dates_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2016, 1, 1),
                "end_date_exclusive": date(2016, 7, 1),
                "projected_full_term_release_date_min": date(2024, 1, 1),
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2016, 7, 1),
                "end_date_exclusive": None,
                "projected_full_term_release_date_min": date(2024, 7, 1),
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
                "sentence_inferred_group_id": 888,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2016, 1, 1),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": date(2025, 1, 1),
                "sentence_array": [
                    {
                        "sentence_id": 123,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2016, 1, 1),
                "end_date_exclusive": date(2016, 7, 1),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": date(2025, 1, 1),
                "sentence_array": [
                    {
                        "sentence_id": 123,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": date(
                            2024, 1, 1
                        ),
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2016, 7, 1),
                "end_date_exclusive": date(2017, 1, 1),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": date(2025, 1, 1),
                "sentence_array": [
                    {
                        "sentence_id": 123,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": date(
                            2024, 7, 1
                        ),
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentences_and_charges_address: sentences_and_charges_data,
                self.sentence_group_projected_dates_address: sentence_group_projected_dates_data,
                self.sentence_projected_dates_address: sentence_projected_dates_data,
                self.sentences_preprocessed_address: sentences_preprocessed_data,
            },
            expected_output,
        )

    def test_sentences_not_being_served_or_not_overlapping_do_not_hydrate_dates(
        self,
    ) -> None:
        """
        Test that when there is sentence level projected dates data for a sentence that is not being served that the
        array is not hydrated. Additionally test that a sentence level projected date session that does not overlap
        the serving period does not hydrate the sentence array.
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

        sentences_and_charges_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "sentence_inferred_group_id": 888,
            },
        ]

        sentence_group_projected_dates_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_inferred_group_id": 888,
                "start_date": date(2000, 1, 1),
                "end_date_exclusive": None,
                "projected_full_term_release_date_max": date(2025, 1, 1),
            },
        ]

        sentence_projected_dates_data = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 123,
                "start_date": date(2000, 1, 1),
                "end_date_exclusive": date(2002, 1, 1),
                "projected_full_term_release_date_min": date(2024, 1, 1),
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "sentence_id": 456,
                "start_date": date(2000, 1, 1),
                "end_date_exclusive": None,
                "projected_full_term_release_date_min": date(2024, 1, 1),
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
                "sentence_inferred_group_id": 888,
                "start_date": date(2015, 1, 1),
                "end_date_exclusive": date(2017, 1, 1),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": date(2025, 1, 1),
                "sentence_array": [
                    {
                        "sentence_id": 123,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentences_and_charges_address: sentences_and_charges_data,
                self.sentence_group_projected_dates_address: sentence_group_projected_dates_data,
                self.sentence_projected_dates_address: sentence_projected_dates_data,
                self.sentences_preprocessed_address: sentences_preprocessed_data,
            },
            expected_output,
        )
