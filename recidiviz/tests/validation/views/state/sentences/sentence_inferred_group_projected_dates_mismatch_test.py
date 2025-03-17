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
"""Tests the validation query"""
import datetime

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_group_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
)
from recidiviz.validation.views.state.sentences.sentence_inferred_group_projected_dates_mismatch import (
    SENTENCE_INFERRED_GROUP_PROJECTED_DATES_VALIDATION_VIEW_BUILDER,
)

_SCHEMA = [
    bigquery.SchemaField("state_code", "STRING"),
    bigquery.SchemaField("person_id", "INT"),
    bigquery.SchemaField("sentence_inferred_group_id", "INTEGER"),
    bigquery.SchemaField("start_date", "DATE"),
    bigquery.SchemaField("end_date_exclusive", "DATE"),
    bigquery.SchemaField("parole_eligibility_date", "DATE"),
    bigquery.SchemaField("projected_parole_release_date", "DATE"),
    bigquery.SchemaField("projected_full_term_release_date_min", "DATE"),
    bigquery.SchemaField("projected_full_term_release_date_max", "DATE"),
]


class InferredProjectedDatesValidationTest(SimpleBigQueryViewBuilderTestCase):
    """Tests the SENTENCE_INFERRED_GROUP_PROJECTED_DATES_VALIDATION_VIEW_BUILDER."""

    agg_sentence_address = (
        INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER.table_for_query
    )
    agg_group_address = (
        INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER.table_for_query
    )

    state_code = StateCode.US_XX
    inferred_group_id = 42
    person_id = 123

    critical_date_1 = datetime.datetime(2022, 1, 1, 6)
    critical_date_2 = datetime.datetime(2022, 2, 1, 12, 30)
    critical_date_3 = datetime.datetime(2022, 3, 4)

    # Show full diffs on test failure
    maxDiff = None

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return SENTENCE_INFERRED_GROUP_PROJECTED_DATES_VALIDATION_VIEW_BUILDER

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        return {
            self.agg_sentence_address: _SCHEMA,
            self.agg_group_address: _SCHEMA,
        }

    def test_no_aggregated_group_data(self) -> None:
        """Tests we return zero rows when there is only aggregated sentence level data."""
        agg_sentence_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": None,
                "parole_eligibility_date": datetime.date(2022, 3, 1),
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
        ]
        self.run_simple_view_builder_query_test_from_data(
            {self.agg_sentence_address: agg_sentence_data, self.agg_group_address: []},
            [],
        )

    def test_no_sentence_level_data(self) -> None:
        """Tests we return zero rows when there is only group level data."""
        group_lvl_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": None,
                "parole_eligibility_date": datetime.date(2022, 3, 1),
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
        ]
        self.run_simple_view_builder_query_test_from_data(
            {self.agg_sentence_address: [], self.agg_group_address: group_lvl_data},
            [],
        )

    def test_no_rows_when_data_is_the_same(self) -> None:
        input_data = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": None,
                "parole_eligibility_date": datetime.date(2022, 3, 1),
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
        ]
        self.run_simple_view_builder_query_test_from_data(
            {self.agg_sentence_address: input_data, self.agg_group_address: input_data},
            [],
        )

    def test_row_when_data_is_different(self) -> None:
        agg_sentence = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": None,
                "parole_eligibility_date": datetime.date(2022, 3, 1),
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
        ]
        group_lvl = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": None,
                "parole_eligibility_date": datetime.date(2022, 2, 1),
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
        ]
        self.run_simple_view_builder_query_test_from_data(
            {
                self.agg_sentence_address: agg_sentence,
                self.agg_group_address: group_lvl,
            },
            [
                {
                    "state_code": self.state_code.value,
                    "region_code": self.state_code.value,
                    "sentence_inferred_group_id": self.inferred_group_id,
                    "person_id": self.person_id,
                    "start_date": self.critical_date_1.date(),
                    "end_date_exclusive": None,
                    "parole_eligibility_date__sentence": datetime.date(2022, 3, 1),
                    "parole_eligibility_date__sentence_group": datetime.date(
                        2022, 2, 1
                    ),
                    "projected_parole_release_date__sentence": None,
                    "projected_parole_release_date__sentence_group": None,
                    "projected_full_term_release_date_min__sentence": None,
                    "projected_full_term_release_date_min__sentence_group": None,
                    "projected_full_term_release_date_max__sentence": None,
                    "projected_full_term_release_date_max__sentence_group": None,
                },
            ],
        )

    def test_row_when_data_is_different_but_other_row_is_the_same(self) -> None:
        agg_sentence = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "parole_eligibility_date": datetime.date(2022, 3, 1),
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
            # This row won't appear because it isn't in the other table
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_2.date(),
                "end_date_exclusive": self.critical_date_3.date(),
                "parole_eligibility_date": datetime.date(2022, 3, 1),
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
            # We expect data from this row.
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_3.date(),
                "end_date_exclusive": None,
                "parole_eligibility_date": datetime.date(2022, 3, 1),
                "projected_parole_release_date": datetime.date(2023, 3, 1),
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
        ]
        group_lvl = [
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_1.date(),
                "end_date_exclusive": self.critical_date_2.date(),
                "parole_eligibility_date": datetime.date(2022, 3, 1),
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
            # We expect data from this row.
            {
                "state_code": self.state_code.value,
                "person_id": self.person_id,
                "sentence_inferred_group_id": self.inferred_group_id,
                "start_date": self.critical_date_3.date(),
                "end_date_exclusive": None,
                "parole_eligibility_date": datetime.date(2022, 3, 1),
                "projected_parole_release_date": datetime.date(2023, 4, 1),
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
        ]
        self.run_simple_view_builder_query_test_from_data(
            {
                self.agg_sentence_address: agg_sentence,
                self.agg_group_address: group_lvl,
            },
            [
                {
                    "state_code": self.state_code.value,
                    "region_code": self.state_code.value,
                    "sentence_inferred_group_id": self.inferred_group_id,
                    "person_id": self.person_id,
                    "start_date": self.critical_date_3.date(),
                    "end_date_exclusive": None,
                    "parole_eligibility_date__sentence": datetime.date(2022, 3, 1),
                    "parole_eligibility_date__sentence_group": datetime.date(
                        2022, 3, 1
                    ),
                    "projected_parole_release_date__sentence": datetime.date(
                        2023, 3, 1
                    ),
                    "projected_parole_release_date__sentence_group": datetime.date(
                        2023, 4, 1
                    ),
                    "projected_full_term_release_date_min__sentence": None,
                    "projected_full_term_release_date_min__sentence_group": None,
                    "projected_full_term_release_date_max__sentence": None,
                    "projected_full_term_release_date_max__sentence_group": None,
                },
            ],
        )
