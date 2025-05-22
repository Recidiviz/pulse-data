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
"""Tests the PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER."""
from datetime import date, datetime, timedelta
from typing import Dict, List

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.analyst_data.us_ma.us_ma_person_projected_date_sessions_preprocessed import (
    US_MA_PERSON_PROJECTED_DATE_SESSIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_group_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.person_projected_date_sessions import (
    PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
)


class PersonProjectedDateSessionsTest(SimpleBigQueryViewBuilderTestCase):
    """Tests the PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER."""

    aggregated_sentence_group_address = (
        INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER.table_for_query
    )
    aggregated_sentence_address = (
        INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER.table_for_query
    )

    us_ma_preprocessed_address = (
        US_MA_PERSON_PROJECTED_DATE_SESSIONS_PREPROCESSED_VIEW_BUILDER.table_for_query
    )

    state_code = StateCode.US_XX
    person_id = hash("TEST-PERSON-1")
    sentence_id_1 = 123
    sentence_id_2 = 456
    sentence_group_id_1 = 777
    sentence_group_id_2 = 999
    inferred_group_id = 888

    critical_date_1 = datetime(2022, 1, 1, 6)
    critical_date_2 = datetime(2022, 2, 1, 12, 30)
    critical_date_3 = datetime(2022, 3, 4)

    # These are used on tests with interleaved length and status updates
    suspended_dt = critical_date_1 + timedelta(days=4)
    back_to_serving_dt = critical_date_2 + timedelta(days=4)
    # Sanity check our dates are in an order we want for this test.
    assert (
        critical_date_1
        < suspended_dt
        < critical_date_2
        < back_to_serving_dt
        < critical_date_3
    )

    projected_date_1_min = date(2025, 1, 1)
    projected_date_2_min = date(2024, 8, 14)
    projected_date_3_min = date(2024, 8, 1)

    projected_date_1_med = projected_date_1_min + timedelta(days=15)
    projected_date_2_med = projected_date_2_min + timedelta(days=15)
    projected_date_3_med = projected_date_3_min + timedelta(days=15)

    projected_date_1_max = projected_date_1_min + timedelta(days=30)
    projected_date_2_max = projected_date_2_min + timedelta(days=30)
    projected_date_3_max = projected_date_3_min + timedelta(days=30)

    earned_time_days_1 = 5
    earned_time_days_2 = 10

    in_state_sentencing_authority = "STATE"
    out_of_state_sentencing_authority = "OTHER_STATE"

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        return {
            self.aggregated_sentence_group_address: [
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("sentence_inferred_group_id", int),
                schema_field_for_type("start_date", date),
                schema_field_for_type("end_date_exclusive", date),
                schema_field_for_type("parole_eligibility_date", date),
                schema_field_for_type("projected_parole_release_date", date),
                schema_field_for_type("projected_full_term_release_date_min", date),
                schema_field_for_type("projected_full_term_release_date_max", date),
                bigquery.SchemaField(
                    "sentence_group_array",
                    "RECORD",
                    mode="REPEATED",
                    fields=(
                        schema_field_for_type("sentence_group_id", int),
                        schema_field_for_type(
                            "sentence_group_parole_eligibility_date", date
                        ),
                        schema_field_for_type(
                            "sentence_group_projected_parole_release_date", date
                        ),
                        schema_field_for_type(
                            "sentence_group_projected_full_term_release_date_min", date
                        ),
                        schema_field_for_type(
                            "sentence_group_projected_full_term_release_date_max", date
                        ),
                    ),
                ),
            ],
            self.aggregated_sentence_address: [
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("sentence_inferred_group_id", int),
                schema_field_for_type("start_date", date),
                schema_field_for_type("end_date_exclusive", date),
                schema_field_for_type("parole_eligibility_date", date),
                schema_field_for_type("projected_parole_release_date", date),
                schema_field_for_type("projected_full_term_release_date_min", date),
                schema_field_for_type("projected_full_term_release_date_max", date),
                schema_field_for_type("good_time_days", int),
                schema_field_for_type("earned_time_days", int),
                schema_field_for_type("has_any_out_of_state_sentences", bool),
                schema_field_for_type("has_any_in_state_sentences", bool),
                bigquery.SchemaField(
                    "sentence_array",
                    "RECORD",
                    mode="REPEATED",
                    fields=(
                        schema_field_for_type("sentence_id", int),
                        schema_field_for_type("sentence_parole_eligibility_date", date),
                        schema_field_for_type(
                            "sentence_projected_parole_release_date", date
                        ),
                        schema_field_for_type(
                            "sentence_projected_full_term_release_date_min", date
                        ),
                        schema_field_for_type(
                            "sentence_projected_full_term_release_date_max", date
                        ),
                        schema_field_for_type("sentence_length_days_min", int),
                        schema_field_for_type("sentence_length_days_max", int),
                        schema_field_for_type("sentence_good_time_days", int),
                        schema_field_for_type("sentence_earned_time_days", int),
                        schema_field_for_type("sentencing_authority", str),
                    ),
                ),
            ],
            # TODO(#42451): Deprecate this view if sentence-level data is ingested from US_MA
            self.us_ma_preprocessed_address: [
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("sentence_inferred_group_id", int),
                schema_field_for_type("start_date", date),
                schema_field_for_type("end_date_exclusive", date),
                schema_field_for_type("group_parole_eligibility_date", date),
                schema_field_for_type("group_projected_parole_release_date", date),
                schema_field_for_type(
                    "group_projected_full_term_release_date_min", date
                ),
                schema_field_for_type(
                    "group_projected_full_term_release_date_max", date
                ),
                schema_field_for_type("good_time_days", int),
                schema_field_for_type("earned_time_days", int),
                schema_field_for_type("has_any_out_of_state_sentences", bool),
                schema_field_for_type("has_any_in_state_sentences", bool),
                bigquery.SchemaField(
                    "sentence_array",
                    "RECORD",
                    mode="REPEATED",
                    fields=(
                        schema_field_for_type("sentence_id", int),
                        schema_field_for_type("sentence_parole_eligibility_date", date),
                        schema_field_for_type(
                            "sentence_projected_parole_release_date", date
                        ),
                        schema_field_for_type(
                            "sentence_projected_full_term_release_date_min", date
                        ),
                        schema_field_for_type(
                            "sentence_projected_full_term_release_date_max", date
                        ),
                        schema_field_for_type("sentence_length_days_min", int),
                        schema_field_for_type("sentence_length_days_max", int),
                        schema_field_for_type("sentence_good_time_days", int),
                        schema_field_for_type("sentence_earned_time_days", int),
                        schema_field_for_type("sentencing_authority", str),
                    ),
                ),
            ],
        }

    def test_single_inferred_group_input_from_both_sources(self) -> None:
        """
        Test the case where there is a single inferred group aggregation sourced from sentence groups and a single
        inferred group aggregation sourced from sentences. The two inferred group aggregations have differing non-null
        projected dates, and we check that the max value is taken.
        """
        aggregated_sentence_groups_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "projected_full_term_release_date_max": self.projected_date_1_max,
            },
        ]

        aggregated_sentence_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "projected_full_term_release_date_max": self.projected_date_1_min,
                "earned_time_days": self.earned_time_days_1,
                "has_any_out_of_state_sentences": False,
                "has_any_in_state_sentences": True,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": self.projected_date_1_min,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": self.earned_time_days_1,
                        "sentencing_authority": self.in_state_sentencing_authority,
                    },
                ],
            },
        ]

        us_ma_preprocessed_data: List[Dict] = []

        expected_output = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": self.projected_date_1_max,
                "group_earned_time_days": self.earned_time_days_1,
                "group_good_time_days": None,
                "has_any_out_of_state_sentences": False,
                "has_any_in_state_sentences": True,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": self.projected_date_1_min,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": self.earned_time_days_1,
                        "sentencing_authority": self.in_state_sentencing_authority,
                    },
                ],
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.aggregated_sentence_group_address: aggregated_sentence_groups_data,
                self.aggregated_sentence_address: aggregated_sentence_data,
                self.us_ma_preprocessed_address: us_ma_preprocessed_data,
            },
            expected_output,
        )

    def test_multiple_inferred_group_sessions_sourced_from_sentences(self) -> None:
        """
        Test the case where there are multiple inferred group sessions sourced from sentences because of a change in the
        underlying sentence array. The group projected date value does not change in the output, but we get a new row
        because of the change in the sentence struct.
        """
        aggregated_sentence_groups_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_3.date(),
                "projected_full_term_release_date_max": self.projected_date_1_max,
            },
        ]

        aggregated_sentence_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "projected_full_term_release_date_max": self.projected_date_1_min,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": self.projected_date_1_min,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_2.date(),
                "end_date_exclusive": self.critical_date_3.date(),
                "projected_full_term_release_date_max": self.projected_date_1_med,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": self.projected_date_1_med,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
        ]

        us_ma_preprocessed_data: List[Dict] = []

        expected_output = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": self.projected_date_1_max,
                "group_earned_time_days": None,
                "group_good_time_days": None,
                "has_any_out_of_state_sentences": None,
                "has_any_in_state_sentences": None,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": self.projected_date_1_min,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                        "sentencing_authority": None,
                    },
                ],
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_2.date(),
                "end_date_exclusive": self.critical_date_3.date(),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": self.projected_date_1_max,
                "group_earned_time_days": None,
                "group_good_time_days": None,
                "has_any_out_of_state_sentences": None,
                "has_any_in_state_sentences": None,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": self.projected_date_1_med,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                        "sentencing_authority": None,
                    },
                ],
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.aggregated_sentence_group_address: aggregated_sentence_groups_data,
                self.aggregated_sentence_address: aggregated_sentence_data,
                self.us_ma_preprocessed_address: us_ma_preprocessed_data,
            },
            expected_output,
        )

    def test_multiple_inferred_group_sessions_from_each_source(self) -> None:
        """
        Test the case where there the same inferred group has multiple inferred group sessions sourced from sentences
        and multiple inferred group sessions sourced from sentence groups because of a change in the projected dates.
        Check that we take the max across the two sources during the periods of overlap.
        """
        aggregated_sentence_groups_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_3.date(),
                "projected_full_term_release_date_max": self.projected_date_1_max,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_3.date(),
                "end_date_exclusive": None,
                "projected_full_term_release_date_max": self.projected_date_1_min,
            },
        ]

        aggregated_sentence_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "projected_full_term_release_date_max": self.projected_date_1_min,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": self.projected_date_1_min,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_2.date(),
                "end_date_exclusive": None,
                "projected_full_term_release_date_max": self.projected_date_1_med,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": self.projected_date_1_med,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
        ]

        us_ma_preprocessed_data: List[Dict] = []

        expected_output = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": self.projected_date_1_max,
                "group_earned_time_days": None,
                "group_good_time_days": None,
                "has_any_out_of_state_sentences": None,
                "has_any_in_state_sentences": None,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": self.projected_date_1_min,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                        "sentencing_authority": None,
                    },
                ],
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_2.date(),
                "end_date_exclusive": self.critical_date_3.date(),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": self.projected_date_1_max,
                "group_earned_time_days": None,
                "group_good_time_days": None,
                "has_any_out_of_state_sentences": None,
                "has_any_in_state_sentences": None,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": self.projected_date_1_med,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                        "sentencing_authority": None,
                    },
                ],
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_3.date(),
                "end_date_exclusive": None,
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": self.projected_date_1_med,
                "group_earned_time_days": None,
                "group_good_time_days": None,
                "has_any_out_of_state_sentences": None,
                "has_any_in_state_sentences": None,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": self.projected_date_1_med,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                        "sentencing_authority": None,
                    },
                ],
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.aggregated_sentence_group_address: aggregated_sentence_groups_data,
                self.aggregated_sentence_address: aggregated_sentence_data,
                self.us_ma_preprocessed_address: us_ma_preprocessed_data,
            },
            expected_output,
        )

    def test_non_null_date_chosen_over_null(self) -> None:
        """
        Test the case where one source has a null date and the other source has a non-null date (something that occurs
        when a state has only group projected dates and not sentence projected dates, or vice versa). When aggregating
        across sources, we want to choose the non-null date.
        """
        aggregated_sentence_groups_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "projected_full_term_release_date_max": None,
            },
        ]

        aggregated_sentence_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "projected_full_term_release_date_max": self.projected_date_1_min,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": self.projected_date_1_min,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                    },
                ],
            },
        ]

        us_ma_preprocessed_data: List[Dict] = []

        expected_output = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "group_parole_eligibility_date": None,
                "group_projected_parole_release_date": None,
                "group_projected_full_term_release_date_min": None,
                "group_projected_full_term_release_date_max": self.projected_date_1_min,
                "group_earned_time_days": None,
                "group_good_time_days": None,
                "has_any_out_of_state_sentences": None,
                "has_any_in_state_sentences": None,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": self.projected_date_1_min,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                        "sentencing_authority": None,
                    },
                ],
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.aggregated_sentence_group_address: aggregated_sentence_groups_data,
                self.aggregated_sentence_address: aggregated_sentence_data,
                self.us_ma_preprocessed_address: us_ma_preprocessed_data,
            },
            expected_output,
        )
