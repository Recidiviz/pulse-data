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
"""Tests the inferred_sentence_group_aggregated_sentence_projected_dates view in sentence_sessions."""
from datetime import date, datetime, timedelta

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_projected_date_sessions import (
    SENTENCE_PROJECTED_DATE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentences_and_charges import (
    SENTENCES_AND_CHARGES_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
)


class SentenceProjectedDateSessionsTest(SimpleBigQueryViewBuilderTestCase):
    """Tests the inferred_sentence_group_aggregated_sentence_projected_dates view in sentence_sessions."""

    sentence_projected_dates_address = (
        SENTENCE_PROJECTED_DATE_SESSIONS_VIEW_BUILDER.table_for_query
    )
    sentences_and_charges_address = SENTENCES_AND_CHARGES_VIEW_BUILDER.table_for_query

    state_code = StateCode.US_XX
    person_id = hash("TEST-PERSON-1")
    sentence_id_1 = 123
    sentence_id_2 = 456
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
        return INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        return {
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
            self.sentences_and_charges_address: [
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("sentence_id", int),
                schema_field_for_type("sentence_inferred_group_id", int),
                schema_field_for_type("is_life", bool),
                schema_field_for_type("sentencing_authority", str),
            ],
        }

    def test_offset_overlapping_sentences_agg(self) -> None:
        """Tests that overlapping sentences get sub-sessionized and that we take (1) the max projected date, (2) the sum
        of credits earned across sentences, and (3) a TRUE value for has_any_in_state_sentences and
        has_any_out_of_state_sentences if any sentence being served is in state or out of state, respectively"""

        projected_dates_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_1,
                "start_date": self.critical_date_1,
                "end_date_exclusive": self.critical_date_3,
                "projected_full_term_release_date_min": self.projected_date_1_min,
                "earned_time_days": self.earned_time_days_1,
            },
            # sentence 2 has a larger `projected_full_term_release_date_min` value
            # and therefore the inferred group should take this value during the overlap
            # period from critical date 2 to critical date 3 as well as from critical date 3
            # onward because only group 2 is being served from critical date 3 onward.
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_2,
                "start_date": self.critical_date_2,
                "end_date_exclusive": None,
                "projected_full_term_release_date_min": self.projected_date_1_max,
                "earned_time_days": self.earned_time_days_2,
            },
        ]

        sentences_and_charges_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_1,
                "sentence_inferred_group_id": self.inferred_group_id,
                "is_life": False,
                "sentencing_authority": self.in_state_sentencing_authority,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_2,
                "sentence_inferred_group_id": self.inferred_group_id,
                "is_life": False,
                "sentencing_authority": self.out_of_state_sentencing_authority,
            },
        ]

        expected_data = [
            # first session has only sentence 1 and the date associated with that sentence is the max of the group and
            # the earned_time_days value is taken from sentence 1
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1_min,
                "projected_full_term_release_date_max": None,
                "earned_time_days": self.earned_time_days_1,
                "good_time_days": None,
                "has_any_out_of_state_sentences": False,
                "has_any_in_state_sentences": True,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": self.projected_date_1_min,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": self.earned_time_days_1,
                        "sentencing_authority": self.in_state_sentencing_authority,
                    },
                ],
            },
            # second session has both sentences and the 2nd sentence's date is the max of the group and the earned time
            # days value is the sum of the two sentences
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_2.date(),
                "end_date_exclusive": self.critical_date_3.date(),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1_max,
                "projected_full_term_release_date_max": None,
                "earned_time_days": self.earned_time_days_1 + self.earned_time_days_2,
                "good_time_days": None,
                "has_any_out_of_state_sentences": True,
                "has_any_in_state_sentences": True,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": self.projected_date_1_min,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": self.earned_time_days_1,
                        "sentencing_authority": self.in_state_sentencing_authority,
                    },
                    {
                        "sentence_id": self.sentence_id_2,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": self.projected_date_1_max,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": self.earned_time_days_2,
                        "sentencing_authority": self.out_of_state_sentencing_authority,
                    },
                ],
            },
            # third session only has the second sentence and the projected date stays the same but the earned time days
            # field is now taken from the second sentence, and the sentence array changes
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_3.date(),
                "end_date_exclusive": None,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1_max,
                "projected_full_term_release_date_max": None,
                "earned_time_days": self.earned_time_days_2,
                "good_time_days": None,
                "has_any_out_of_state_sentences": True,
                "has_any_in_state_sentences": False,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_2,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": self.projected_date_1_max,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": self.earned_time_days_2,
                        "sentencing_authority": self.out_of_state_sentencing_authority,
                    },
                ],
            },
        ]
        self.run_simple_view_builder_query_test_from_data(
            {
                self.sentence_projected_dates_address: projected_dates_data,
                self.sentences_and_charges_address: sentences_and_charges_data,
            },
            expected_data,
        )

    def test_dates_preserved_when_no_life_sentence(self) -> None:
        """Tests than when overlapping sentences are aggregated to an inferred group, that if none of the sentences are
        life sentences, we take non-null projected dates over null projected dates.
        """
        # Sentence 1 has null min and max projected dates, and sentence 2 has non-null values. The two sentences
        # perfectly overlap each other
        projected_dates_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_1,
                "start_date": self.critical_date_1,
                "end_date_exclusive": self.critical_date_2,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_2,
                "start_date": self.critical_date_1,
                "end_date_exclusive": self.critical_date_2,
                "projected_full_term_release_date_min": self.projected_date_1_min,
                "projected_full_term_release_date_max": self.projected_date_1_max,
            },
        ]

        # Both sentences are in the same inferred group and neither is a life sentence
        sentences_and_charges_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_1,
                "sentence_inferred_group_id": self.inferred_group_id,
                "is_life": False,
                "sentencing_authority": None,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_2,
                "sentence_inferred_group_id": self.inferred_group_id,
                "is_life": False,
                "sentencing_authority": None,
            },
        ]

        # The inferred group level min and max dates are non-null (from sentence 2)
        expected_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1_min,
                "projected_full_term_release_date_max": self.projected_date_1_max,
                "earned_time_days": None,
                "good_time_days": None,
                "has_any_out_of_state_sentences": False,
                "has_any_in_state_sentences": False,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                        "sentencing_authority": None,
                    },
                    {
                        "sentence_id": self.sentence_id_2,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": self.projected_date_1_min,
                        "sentence_projected_full_term_release_date_max": self.projected_date_1_max,
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
                self.sentence_projected_dates_address: projected_dates_data,
                self.sentences_and_charges_address: sentences_and_charges_data,
            },
            expected_data,
        )

    def test_nulls_preserved_when_is_life_sentence(self) -> None:
        """Tests than when overlapping sentences are aggregated to an inferred group, that a life sentence with a null
        projected max completion date has that null date preserved. Null dates for other types of dates (min completion
        date, parole eligibility date, etc. should not be preserved over non-null values even when the sentence is a
        life sentence)
        """
        # Sentence 1 has null min and max projected dates, and sentence 2 has non-null values. The two sentences
        # perfectly overlap each other
        projected_dates_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_1,
                "start_date": self.critical_date_1,
                "end_date_exclusive": self.critical_date_2,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_2,
                "start_date": self.critical_date_1,
                "end_date_exclusive": self.critical_date_2,
                "projected_full_term_release_date_min": self.projected_date_1_min,
                "projected_full_term_release_date_max": self.projected_date_1_max,
            },
        ]

        # Both sentences are in the same inferred group and sentence 1 is a life sentence
        sentences_and_charges_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_1,
                "sentence_inferred_group_id": self.inferred_group_id,
                "is_life": True,
                "sentencing_authority": None,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_2,
                "sentence_inferred_group_id": self.inferred_group_id,
                "is_life": False,
                "sentencing_authority": None,
            },
        ]

        # The inferred group level min date is not null (gets pulled from sentence 2), but the max date is null because
        # it is null in sentence 1 and sentence 1 is a life sentence
        expected_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1_min,
                "projected_full_term_release_date_max": None,
                "earned_time_days": None,
                "good_time_days": None,
                "has_any_out_of_state_sentences": False,
                "has_any_in_state_sentences": False,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                        "sentencing_authority": None,
                    },
                    {
                        "sentence_id": self.sentence_id_2,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": self.projected_date_1_min,
                        "sentence_projected_full_term_release_date_max": self.projected_date_1_max,
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
                self.sentence_projected_dates_address: projected_dates_data,
                self.sentences_and_charges_address: sentences_and_charges_data,
            },
            expected_data,
        )

    def test_sentencing_authority_aggregation_in_state_and_out_of_state(self) -> None:
        """Tests than when overlapping sentences are aggregated to an inferred group, that the group level booleans are
        `has_any_out_of_state_sentences` and `has_any_in_state_state_sentences` are both TRUE when there is one of
        each sentencing authority value in the underlying sentences
        """
        # The two sentences perfectly overlap each other
        projected_dates_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_1,
                "start_date": self.critical_date_1,
                "end_date_exclusive": self.critical_date_2,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_2,
                "start_date": self.critical_date_1,
                "end_date_exclusive": self.critical_date_2,
            },
        ]

        # Both sentences are in the same inferred group, one has an in state sentence and the other has an out of state
        # sentence
        sentences_and_charges_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_1,
                "sentence_inferred_group_id": self.inferred_group_id,
                "sentencing_authority": self.in_state_sentencing_authority,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_2,
                "sentence_inferred_group_id": self.inferred_group_id,
                "sentencing_authority": self.out_of_state_sentencing_authority,
            },
        ]

        # both sentence authority booleans are true
        expected_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
                "earned_time_days": None,
                "good_time_days": None,
                "has_any_out_of_state_sentences": True,
                "has_any_in_state_sentences": True,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                        "sentencing_authority": self.in_state_sentencing_authority,
                    },
                    {
                        "sentence_id": self.sentence_id_2,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                        "sentencing_authority": self.out_of_state_sentencing_authority,
                    },
                ],
            },
        ]
        self.run_simple_view_builder_query_test_from_data(
            {
                self.sentence_projected_dates_address: projected_dates_data,
                self.sentences_and_charges_address: sentences_and_charges_data,
            },
            expected_data,
        )

    def test_sentencing_authority_aggregation_in_state_and_null(self) -> None:
        """Tests than when overlapping sentences are aggregated to an inferred group, that the group level booleans are
        `has_any_out_of_state_sentences` and `has_any_in_state_state_sentences` are hydrated correctly when there is one
         hydrated sentence authority value and one null sentence authority value.
        """
        # The two sentences perfectly overlap each other
        projected_dates_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_1,
                "start_date": self.critical_date_1,
                "end_date_exclusive": self.critical_date_2,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_2,
                "start_date": self.critical_date_1,
                "end_date_exclusive": self.critical_date_2,
            },
        ]

        # Both sentences are in the same inferred group, one has an in state sentence and the other has a NULL value for
        # sentencing authority
        sentences_and_charges_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_1,
                "sentence_inferred_group_id": self.inferred_group_id,
                "sentencing_authority": self.in_state_sentencing_authority,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_2,
                "sentence_inferred_group_id": self.inferred_group_id,
                "sentencing_authority": None,
            },
        ]

        # both sentence authority booleans are non-null. The in-state boolean is TRUE and the out-of-state boolean is
        # FALSE
        expected_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
                "earned_time_days": None,
                "good_time_days": None,
                "has_any_out_of_state_sentences": False,
                "has_any_in_state_sentences": True,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                        "sentencing_authority": self.in_state_sentencing_authority,
                    },
                    {
                        "sentence_id": self.sentence_id_2,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
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
                self.sentence_projected_dates_address: projected_dates_data,
                self.sentences_and_charges_address: sentences_and_charges_data,
            },
            expected_data,
        )

    def test_sentencing_authority_aggregation_both_null(self) -> None:
        """Tests than when overlapping sentences are aggregated to an inferred group, that the group level booleans are
        both NULL when none of the underlying sentences have sentence_authority hydration
        """
        # The two sentences perfectly overlap each other
        projected_dates_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_1,
                "start_date": self.critical_date_1,
                "end_date_exclusive": self.critical_date_2,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_2,
                "start_date": self.critical_date_1,
                "end_date_exclusive": self.critical_date_2,
            },
        ]

        # Both sentences are in the same inferred group, both have NULL sentencing_authority values
        sentences_and_charges_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_1,
                "sentence_inferred_group_id": self.inferred_group_id,
                "sentencing_authority": None,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id_2,
                "sentence_inferred_group_id": self.inferred_group_id,
                "sentencing_authority": None,
            },
        ]

        # both sentence authority booleans are NULL
        expected_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
                "earned_time_days": None,
                "good_time_days": None,
                # TODO(#42827): Look into unexpected NULL handling in emulator output. These two flags should be NULL
                "has_any_out_of_state_sentences": False,
                "has_any_in_state_sentences": False,
                "sentence_array": [
                    {
                        "sentence_id": self.sentence_id_1,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
                        "sentence_length_days_min": None,
                        "sentence_length_days_max": None,
                        "sentence_good_time_days": None,
                        "sentence_earned_time_days": None,
                        "sentencing_authority": None,
                    },
                    {
                        "sentence_id": self.sentence_id_2,
                        "sentence_parole_eligibility_date": None,
                        "sentence_projected_parole_release_date": None,
                        "sentence_projected_full_term_release_date_min": None,
                        "sentence_projected_full_term_release_date_max": None,
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
                self.sentence_projected_dates_address: projected_dates_data,
                self.sentences_and_charges_address: sentences_and_charges_data,
            },
            expected_data,
        )
