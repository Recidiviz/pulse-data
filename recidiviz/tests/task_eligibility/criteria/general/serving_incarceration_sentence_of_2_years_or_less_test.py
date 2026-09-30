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
"""Tests the SERVING_INCARCERATION_SENTENCE_OF_2_YEARS_OR_LESS criterion."""
import json
from datetime import date

import pandas as pd
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.sentence_sessions.person_projected_date_sessions import (
    PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentences_and_charges import (
    SENTENCES_AND_CHARGES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sessions import (
    COMPARTMENT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.criteria.general.serving_incarceration_sentence_of_2_years_or_less import (
    VIEW_BUILDER,
)
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
)


class ServingIncarcerationSentenceOf2YearsOrLessTest(SimpleBigQueryViewBuilderTestCase):
    """Tests the SERVING_INCARCERATION_SENTENCE_OF_2_YEARS_OR_LESS criterion."""

    projected_dates_address = (
        PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER.table_for_query
    )
    sentences_and_charges_address = SENTENCES_AND_CHARGES_VIEW_BUILDER.table_for_query
    compartment_sessions_address = COMPARTMENT_SESSIONS_VIEW_BUILDER.table_for_query

    state_code = StateCode.US_XX
    person_id = hash("TEST-PERSON-1")
    prison_sentence_id = 1
    probation_sentence_id = 2
    span_start = date(2025, 1, 1)

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return VIEW_BUILDER

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        return {
            self.projected_dates_address: [
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("start_date", date),
                schema_field_for_type("end_date_exclusive", date),
                bigquery.SchemaField(
                    "sentence_array",
                    "RECORD",
                    mode="REPEATED",
                    fields=(
                        schema_field_for_type("sentence_id", int),
                        schema_field_for_type("sentence_length_days_max", int),
                    ),
                ),
            ],
            self.sentences_and_charges_address: [
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("sentence_id", int),
                schema_field_for_type("sentence_type", str),
            ],
            self.compartment_sessions_address: [
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("compartment_level_1", str),
                schema_field_for_type("start_date", date),
                schema_field_for_type("end_date_exclusive", date),
            ],
        }

    def _input_data(
        self, *, prison_length_days: int
    ) -> dict[BigQueryAddress, list[dict]]:
        return {
            self.projected_dates_address: [
                {
                    "state_code": self.state_code.value,
                    "person_id": self.person_id,
                    "start_date": self.span_start,
                    "end_date_exclusive": None,
                    "sentence_array": [
                        {
                            "sentence_id": self.prison_sentence_id,
                            "sentence_length_days_max": prison_length_days,
                        },
                        # A probation term imposed with the prison sentence and queued to
                        # start after release, so it sits in the same span.
                        {
                            "sentence_id": self.probation_sentence_id,
                            "sentence_length_days_max": 900,
                        },
                    ],
                },
            ],
            self.sentences_and_charges_address: [
                {
                    "state_code": self.state_code.value,
                    "person_id": self.person_id,
                    "sentence_id": self.prison_sentence_id,
                    "sentence_type": "STATE_PRISON",
                },
                {
                    "state_code": self.state_code.value,
                    "person_id": self.person_id,
                    "sentence_id": self.probation_sentence_id,
                    "sentence_type": "PROBATION",
                },
            ],
            self.compartment_sessions_address: [
                {
                    "state_code": self.state_code.value,
                    "person_id": self.person_id,
                    "compartment_level_1": "INCARCERATION",
                    "start_date": self.span_start,
                    "end_date_exclusive": None,
                },
            ],
        }

    def _longest_sentence_length(self, *, prison_length_days: int) -> int:
        """Runs the criterion over one resident and returns the sentence length it measured.

        The test checks the length rather than meets_criteria: the emulator loads integer
        fields of a repeated record as strings, so the criterion's comparison to 2 years is
        not reliable there and MAX compares the lengths as text. The lengths below are chosen
        so that text and numeric order agree. The length is what the prison-only filter
        decides, since meets_criteria is that length compared to 2 years.
        """
        view = self.view_builder.build()
        self.assertSetEqual(view.parent_tables, set(self.parent_schemas))
        input_data = self._input_data(prison_length_days=prison_length_days)
        for address, schema in self.parent_schemas.items():
            self.create_mock_table(address, schema)
            self.load_rows_into_table(address, input_data[address])
        rows = self.query(view.view_query).to_dict("records")
        self.assertEqual(1, len(rows))
        row = rows[0]
        self.assertEqual(self.span_start, row["start_date"])
        # The query results come back as a DataFrame, where a NULL date is NaT
        self.assertTrue(pd.isna(row["end_date"]))
        return int(json.loads(row["reason"])["length_of_longest_sentence_in_days"])

    def test_queued_probation_term_does_not_count(self) -> None:
        # Without the prison-only filter this would be the 900 day probation term
        self.assertEqual(400, self._longest_sentence_length(prison_length_days=400))

    def test_prison_sentence_length_is_measured(self) -> None:
        self.assertEqual(950, self._longest_sentence_length(prison_length_days=950))
