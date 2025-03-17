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
"""Tests the sentence_projected_date_sessions view in sentence_sessions."""
from datetime import date, datetime, timedelta

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_projected_date_sessions import (
    SENTENCE_PROJECTED_DATE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_serving_period import (
    SENTENCE_SERVING_PERIOD_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entity_table,
)
from recidiviz.persistence.entity.normalized_entities_utils import (
    queryable_address_for_normalized_entity,
)
from recidiviz.persistence.entity.state import (
    normalized_entities as normalized_state_module,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSentenceLength,
)
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
)
from recidiviz.utils.types import assert_subclass


class SentenceProjectedDateSessionsTest(SimpleBigQueryViewBuilderTestCase):
    """Tests the SENTENCE_PROJECTED_DATE_SESSIONS_VIEW_BUILDER"""

    serving_period_address = SENTENCE_SERVING_PERIOD_VIEW_BUILDER.table_for_query
    sentence_length_address = queryable_address_for_normalized_entity(
        NormalizedStateSentenceLength
    )

    state_code = StateCode.US_XX
    person_id = hash("TEST-PERSON-1")
    sentence_id = 42

    critical_date_1 = datetime(2022, 1, 1, 6)
    critical_date_2 = datetime(2022, 2, 1, 12, 30)
    critical_date_3 = datetime(2022, 3, 4)

    early_datetime = datetime(2000, 1, 1)

    # These are used on tests with interleaved length and status updates
    suspended_dt = critical_date_1 + timedelta(days=4)
    back_to_serving_dt = critical_date_2 + timedelta(days=4)
    # Sanity check our dates are in an order we want for this test.
    assert (
        early_datetime
        < critical_date_1
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

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return SENTENCE_PROJECTED_DATE_SESSIONS_VIEW_BUILDER

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
                normalized_state_module,
                assert_subclass(NormalizedStateSentenceLength, Entity).get_table_id(),
            ),
        }

    def test_one_sentence_one_length(self) -> None:
        """
        The most simple case, a sentence is actively serving and has a single set of
        projected dates from imposition.
        """

        sentence_length_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "length_update_datetime": self.critical_date_1,
                "projected_completion_date_min_external": self.projected_date_1_min,
                "projected_completion_date_max_external": None,
            },
        ]

        serving_periods_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_1,
                "end_date_exclusive": None,
            },
        ]

        expected_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": None,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1_min,
                "projected_full_term_release_date_max": None,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
            },
        ]
        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentence_length_address: sentence_length_data,
            },
            expected_data,
        )

    def test_one_sentence_intermittent_suspension_one_projected_date(self) -> None:
        """
        When an inferred group is composed of single sentence and
        that sentence becomes SUSPENDED, we want the output to exclude
        the period of suspension as the dates are not relevant when the
        sentence is not actively being served.
        """
        sentence_length_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "length_update_datetime": self.critical_date_1,
                "projected_completion_date_min_external": self.projected_date_1_min,
                "projected_completion_date_max_external": None,
            },
        ]
        serving_periods_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_1,
                "end_date_exclusive": self.critical_date_2,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_3,
                "end_date_exclusive": None,
            },
        ]

        expected_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1_min,
                "projected_full_term_release_date_max": None,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_3.date(),
                "end_date_exclusive": None,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1_min,
                "projected_full_term_release_date_max": None,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
            },
        ]
        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentence_length_address: sentence_length_data,
            },
            expected_data,
        )

    def test_one_sentence_intermittent_suspension_updated_projected_dates(self) -> None:
        """
        This test ensures that interleaved length and status updates result with
        the correct projected date. Any update_datetime where a sentence is SUSPENDED
        should not show up. When the sentence is back to SERVING, we should see the most
        recent projected dates!
        """
        sentence_length_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "length_update_datetime": self.critical_date_1,
                "projected_completion_date_min_external": self.projected_date_1_min,
                "projected_completion_date_max_external": None,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "length_update_datetime": self.critical_date_2,
                "projected_completion_date_min_external": self.projected_date_2_min,
                "projected_completion_date_max_external": None,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "length_update_datetime": self.critical_date_3,
                "projected_completion_date_min_external": self.projected_date_3_min,
                "projected_completion_date_max_external": None,
            },
        ]

        serving_periods_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_1,
                "end_date_exclusive": self.suspended_dt,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.back_to_serving_dt,
                "end_date_exclusive": None,
            },
        ]

        expected_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.suspended_dt.date(),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1_min,
                "projected_full_term_release_date_max": None,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
            },
            # BACK TO SERVING, THE MOST RECENT LENGTH UPDATE WAS FROM CRITICAL DATE 2
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.back_to_serving_dt.date(),
                "end_date_exclusive": self.critical_date_3.date(),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_2_min,
                "projected_full_term_release_date_max": None,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_3.date(),
                "end_date_exclusive": None,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_3_min,
                "projected_full_term_release_date_max": None,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentence_length_address: sentence_length_data,
            },
            expected_data,
        )

    def test_hydration_for_length_update_after_sentence_completed(self) -> None:
        """
        Some states mark a sentence's projected data after they
        have completed that sentence ¯\\_(ツ)_//¯

        This test ensures that a completed sentence gets hydrated with the earliest sentence length for the time
        that sentence is served.
        """

        sentence_length_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "length_update_datetime": self.critical_date_3 + timedelta(days=1),
                "projected_completion_date_min_external": self.critical_date_3,
                "projected_completion_date_max_external": None,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "length_update_datetime": self.critical_date_3 + timedelta(days=3),
                "projected_completion_date_min_external": self.critical_date_3
                + timedelta(days=1),
                "projected_completion_date_max_external": None,
            },
        ]

        serving_periods_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_1,
                "end_date_exclusive": None,
            },
        ]

        expected_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_3.date() + timedelta(days=3),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.critical_date_3.date(),
                "projected_full_term_release_date_max": None,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_3.date() + timedelta(days=3),
                "end_date_exclusive": None,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.critical_date_3.date()
                + timedelta(days=1),
                "projected_full_term_release_date_max": None,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
            },
        ]
        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentence_length_address: sentence_length_data,
            },
            expected_data,
        )

    def test_length_update_before_serving_start(self) -> None:
        """
        Test that when sentence projected date sessions are updated before the serving period starts, that the output
        gets clipped to the serving period start
        """

        sentence_length_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "length_update_datetime": self.early_datetime,
                "projected_completion_date_max_external": self.projected_date_3_max,
            },
        ]

        serving_periods_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_3.date(),
            },
        ]

        expected_output = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_3.date(),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_3_max,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentence_length_address: sentence_length_data,
            },
            expected_output,
        )

    def test_length_update_occurs_after_serving_period_end(self) -> None:
        """
        Test that when a projected date update comes after the serving period ends that the serving period is still
        hydrated with the projected date session values.
        """

        sentence_length_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "length_update_datetime": self.critical_date_3,
                "projected_completion_date_max_external": self.projected_date_3_max,
            },
        ]

        serving_periods_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
            },
        ]
        expected_output = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_3_max,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentence_length_address: sentence_length_data,
            },
            expected_output,
        )

    def test_null_projected_date_update(self) -> None:
        """
        Test that when a sentence gets an update that turns a non-null projected date to a null projected date,
        that this null is preserved in the output.
        """

        sentence_length_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "length_update_datetime": self.critical_date_1,
                "projected_completion_date_max_external": self.projected_date_3_max,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "length_update_datetime": self.critical_date_2,
                "projected_completion_date_max_external": None,
            },
        ]

        serving_periods_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": None,
            },
        ]

        expected_output = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_3_max,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
            },
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_id": self.sentence_id,
                "start_date": self.critical_date_2.date(),
                "end_date_exclusive": None,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
                "sentence_length_days_min": None,
                "sentence_length_days_max": None,
                "good_time_days": None,
                "earned_time_days": None,
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.serving_period_address: serving_periods_data,
                self.sentence_length_address: sentence_length_data,
            },
            expected_output,
        )
