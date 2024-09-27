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
"""Tests the inferred_group_aggregated_sentence_group_projected_dates view in sentence_sessions."""
import datetime

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_group_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER,
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
    NormalizedStatePerson,
    NormalizedStateSentenceGroup,
    NormalizedStateSentenceGroupLength,
)
from recidiviz.tests.big_query.load_entities_to_emulator_via_pipeline import (
    write_root_entities_to_emulator,
)
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
    TableRow,
)
from recidiviz.utils.types import assert_subclass


class InferredProjectedDatesTest(SimpleBigQueryViewBuilderTestCase):
    """Tests the INFERRED_GORUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER."""

    state_code = StateCode.US_XX
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
        return INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        return {
            queryable_address_for_normalized_entity(
                entity
            ): get_bq_schema_for_entity_table(
                normalized_state_module, assert_subclass(entity, Entity).get_table_id()
            )
            for entity in [
                NormalizedStateSentenceGroup,
                NormalizedStateSentenceGroupLength,
            ]
        }

    def _make_sentence_group(self, id_: int) -> NormalizedStateSentenceGroup:
        return NormalizedStateSentenceGroup(
            state_code=self.state_code.value,
            external_id=f"SG-{id_}",
            sentence_group_id=id_,
            sentence_inferred_group_id=self.inferred_group_id,
        )

    def _make_person(self, id_: int = 1) -> NormalizedStatePerson:
        return NormalizedStatePerson(
            state_code=self.state_code.value,
            person_id=hash(f"TEST-PERSON-{id_}"),
        )

    def run_test(
        self, people: list[NormalizedStatePerson], expected_result: list[TableRow]
    ) -> None:
        write_root_entities_to_emulator(
            emulator_tc=self, schema_mapping=self.parent_schemas, people=people
        )
        view = self.view_builder.build()
        self.run_query_test(view.view_query, expected_result)

    def test_one_group_with_lengths(self) -> None:
        """The most simple case, a single sentence group with projected dates."""
        group = self._make_sentence_group(1)
        group.sentence_group_lengths = [
            NormalizedStateSentenceGroupLength(
                state_code=self.state_code.value,
                group_update_datetime=self.critical_date_1,
                sentence_group_length_id=111,
                projected_full_term_release_date_max_external=self.projected_date_4,
            ),
            NormalizedStateSentenceGroupLength(
                state_code=self.state_code.value,
                group_update_datetime=self.critical_date_2,
                sentence_group_length_id=112,
                projected_full_term_release_date_max_external=self.projected_date_3,
            ),
        ]
        expected_data = [
            {
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_1,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_4,
            },
            {
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_2,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_3,
            },
        ]
        person = self._make_person()
        person.sentence_groups = [group]
        self.run_test([person], expected_data)

    def test_two_groups_with_different_critical_dates(self) -> None:
        """Tests that two sentence groups with interleaved critical dates have expected projected dates."""
        group_1 = self._make_sentence_group(1)
        group_1.sentence_group_lengths = [
            NormalizedStateSentenceGroupLength(
                state_code=self.state_code.value,
                group_update_datetime=self.critical_date_1,
                sentence_group_length_id=111,
                projected_full_term_release_date_max_external=self.projected_date_2,
            ),
            NormalizedStateSentenceGroupLength(
                state_code=self.state_code.value,
                group_update_datetime=self.critical_date_3,
                sentence_group_length_id=112,
                projected_full_term_release_date_max_external=self.projected_date_3,
            ),
        ]
        group_2 = self._make_sentence_group(2)
        group_2.sentence_group_lengths = [
            NormalizedStateSentenceGroupLength(
                state_code=self.state_code.value,
                group_update_datetime=self.critical_date_1,
                sentence_group_length_id=211,
                projected_full_term_release_date_min_external=self.projected_date_1,
                projected_full_term_release_date_max_external=self.projected_date_4,
            ),
            NormalizedStateSentenceGroupLength(
                state_code=self.state_code.value,
                group_update_datetime=self.critical_date_2,
                sentence_group_length_id=212,
                projected_full_term_release_date_max_external=self.projected_date_2,
            ),
        ]
        expected_data = [
            {
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_1,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1,
                "projected_full_term_release_date_max": self.projected_date_4,
            },
            {
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_2,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1,
                "projected_full_term_release_date_max": self.projected_date_2,
            },
            {
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_3,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1,
                "projected_full_term_release_date_max": self.projected_date_3,
            },
        ]
        person = self._make_person()
        person.sentence_groups = [group_1, group_2]
        self.run_test([person], expected_data)
