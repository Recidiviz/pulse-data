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
"""Tests the sentence_id_to_consecutive_sentence_id view in sentence_sessions."""
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.sessions.sentence_to_consecutive_parent_sentence import (
    CONSECUTIVE_SENTENCES_VIEW_BUILDER,
)
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entity_table,
)
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
)


class SentenceToParentConsecutiveViewTest(SimpleBigQueryViewBuilderTestCase):
    """Tests the CONSECUTIVE_SENTENCES_VIEW_BUILDER."""

    sentence_address = BigQueryAddress.from_str("normalized_state.state_sentence")

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return CONSECUTIVE_SENTENCES_VIEW_BUILDER

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        return {
            self.sentence_address: get_bq_schema_for_entity_table(
                normalized_entities, "state_sentence"
            )
        }

    def test_no_parent_sentences(self) -> None:
        input_data = [
            {
                "state_code": "US_XX",
                "person_id": 1,
                "sentence_id": 1,
                "external_id": "external_1",
                "parent_sentence_external_id_array": None,
            },
            {
                "state_code": "US_XX",
                "person_id": 1,
                "sentence_id": 2,
                "external_id": "external_2",
                "parent_sentence_external_id_array": None,
            },
            {
                "state_code": "US_XX",
                "person_id": 2,
                "sentence_id": 3,
                "external_id": "external_3",
                "parent_sentence_external_id_array": None,
            },
        ]
        self.run_simple_view_builder_query_test_from_data(
            {self.sentence_address: input_data}, []
        )

    def test_only_single_parent_sentences(self) -> None:
        """Tests Sentence 1 <- Sentence 2"""
        input_data = [
            {
                "state_code": "US_XX",
                "person_id": 1,
                "sentence_id": 1,
                "external_id": "external_1",
                "parent_sentence_external_id_array": None,
            },
            {
                "state_code": "US_XX",
                "person_id": 1,
                "sentence_id": 2,
                "external_id": "external_2",
                "parent_sentence_external_id_array": "external_1",
            },
            {
                "state_code": "US_XX",
                "person_id": 2,
                "sentence_id": 3,
                "external_id": "external_3",
                "parent_sentence_external_id_array": None,
            },
        ]
        output_data = [
            {"sentence_id": 2, "parent_sentence_id": 1},
        ]
        self.run_simple_view_builder_query_test_from_data(
            {self.sentence_address: input_data}, output_data
        )

    def test_only_multiple_child_sentence(self) -> None:
        """Tests Sentence 1 <- [Sentence 2, Sentence 3]"""
        input_data = [
            {
                "state_code": "US_XX",
                "person_id": 1,
                "sentence_id": 1,
                "external_id": "external_1",
                "parent_sentence_external_id_array": None,
            },
            {
                "state_code": "US_XX",
                "person_id": 1,
                "sentence_id": 2,
                "external_id": "external_2",
                "parent_sentence_external_id_array": "external_1",
            },
            {
                "state_code": "US_XX",
                "person_id": 1,
                "sentence_id": 3,
                "external_id": "external_3",
                "parent_sentence_external_id_array": "external_1",
            },
        ]
        output_data = [
            {"sentence_id": 2, "parent_sentence_id": 1},
            {"sentence_id": 3, "parent_sentence_id": 1},
        ]
        self.run_simple_view_builder_query_test_from_data(
            {self.sentence_address: input_data}, output_data
        )

    def test_only_multiple_parent_sentence(self) -> None:
        """Tests [Sentence 1, Sentence 2] <- Sentence 3"""
        input_data = [
            {
                "state_code": "US_XX",
                "person_id": 1,
                "sentence_id": 1,
                "external_id": "external_1",
                "parent_sentence_external_id_array": None,
            },
            {
                "state_code": "US_XX",
                "person_id": 1,
                "sentence_id": 2,
                "external_id": "external_2",
                "parent_sentence_external_id_array": None,
            },
            {
                "state_code": "US_XX",
                "person_id": 1,
                "sentence_id": 3,
                "external_id": "external_3",
                "parent_sentence_external_id_array": "external_1,external_2",
            },
        ]
        output_data = [
            {"sentence_id": 3, "parent_sentence_id": 1},
            {"sentence_id": 3, "parent_sentence_id": 2},
        ]
        self.run_simple_view_builder_query_test_from_data(
            {self.sentence_address: input_data}, output_data
        )

    def test_all_child_parent_relationship_types(self) -> None:
        """
        Tests multiple child, multiple parent, single parent, and no parent situations.

        Person 1:
            Sentence 1 <- [Sentence 2, Sentence 3] <- Sentence 4
        Person 2:
            Sentence 5 <- Sentence 6
        Person 3:
            Sentence 7
        """
        person_1_input = [
            {
                "state_code": "US_XX",
                "person_id": 1,
                "sentence_id": 1,
                "external_id": "external_1",
                "parent_sentence_external_id_array": None,
            },
            {
                "state_code": "US_XX",
                "person_id": 1,
                "sentence_id": 2,
                "external_id": "external_2",
                "parent_sentence_external_id_array": "external_1",
            },
            {
                "state_code": "US_XX",
                "person_id": 1,
                "sentence_id": 3,
                "external_id": "external_3",
                "parent_sentence_external_id_array": "external_1",
            },
            {
                "state_code": "US_XX",
                "person_id": 1,
                "sentence_id": 4,
                "external_id": "external_4",
                "parent_sentence_external_id_array": "external_2,external_3",
            },
        ]
        # Sentence 1 <- [Sentence 2, Sentence 3] <- Sentence 4
        person_1_output = [
            {"sentence_id": 2, "parent_sentence_id": 1},
            {"sentence_id": 3, "parent_sentence_id": 1},
            {"sentence_id": 4, "parent_sentence_id": 2},
            {"sentence_id": 4, "parent_sentence_id": 3},
        ]

        person_2_input = [
            {
                "state_code": "US_XX",
                "person_id": 2,
                "sentence_id": 5,
                "external_id": "external_5",
                "parent_sentence_external_id_array": None,
            },
            {
                "state_code": "US_XX",
                "person_id": 2,
                "sentence_id": 6,
                "external_id": "external_6",
                "parent_sentence_external_id_array": "external_5",
            },
        ]
        # Sentence 5 <- Sentence 6
        person_2_output = [
            {"sentence_id": 6, "parent_sentence_id": 5},
        ]
        # Sentence 7 (no parent, no output)
        person_3_input = [
            {
                "state_code": "US_XX",
                "person_id": 3,
                "sentence_id": 7,
                "external_id": "external_7",
                "parent_sentence_external_id_array": None,
            },
        ]
        input_data = person_1_input + person_2_input + person_3_input
        output_data = person_1_output + person_2_output
        self.run_simple_view_builder_query_test_from_data(
            {self.sentence_address: input_data}, output_data
        )
