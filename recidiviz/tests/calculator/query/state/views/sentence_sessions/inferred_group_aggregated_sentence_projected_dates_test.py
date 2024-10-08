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
"""Tests the inferred_group_aggregated_sentence_projected_dates view in sentence_sessions."""
import datetime

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
    StateSentencingAuthority,
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
    NormalizedStateSentence,
    NormalizedStateSentenceLength,
    NormalizedStateSentenceStatusSnapshot,
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
    """
    Tests the INFERRED_GROUP_PROJECTED_DATES_VIEW_BUILDER.

    For simplicity, all tests are in the same inferred group
    with three critical dates. The final test has extended critical dates
    to test outcomes where the sentence status critical dates do not line
    up exactly with sentence length or group length critical dates.
    """

    state_code = StateCode.US_XX
    inferred_group_id = 42

    critical_date_1 = datetime.datetime(2022, 1, 1, 6)
    critical_date_2 = datetime.datetime(2022, 2, 1, 12, 30)
    critical_date_3 = datetime.datetime(2022, 3, 4)

    # These are used on tests with interleaved length and status updates
    suspended_dt = critical_date_1 + datetime.timedelta(days=4)
    back_to_serving_dt = critical_date_2 + datetime.timedelta(days=4)
    # Sanity check our dates are in an order we want for this test.
    assert (
        critical_date_1
        < suspended_dt
        < critical_date_2
        < back_to_serving_dt
        < critical_date_3
    )

    projected_date_1_min = datetime.date(2025, 1, 1)
    projected_date_2_min = datetime.date(2024, 8, 14)
    projected_date_3_min = datetime.date(2024, 8, 1)

    projected_date_1_med = projected_date_1_min + datetime.timedelta(days=15)
    projected_date_2_med = projected_date_2_min + datetime.timedelta(days=15)
    projected_date_3_med = projected_date_3_min + datetime.timedelta(days=15)

    projected_date_1_max = projected_date_1_min + datetime.timedelta(days=30)
    projected_date_2_max = projected_date_2_min + datetime.timedelta(days=30)
    projected_date_3_max = projected_date_3_min + datetime.timedelta(days=30)

    # Show full diffs on test failure
    maxDiff = None

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        return {
            queryable_address_for_normalized_entity(
                entity
            ): get_bq_schema_for_entity_table(
                normalized_state_module, assert_subclass(entity, Entity).get_table_id()
            )
            for entity in [
                NormalizedStateSentence,
                NormalizedStateSentenceLength,
                NormalizedStateSentenceStatusSnapshot,
            ]
        }

    def _make_sentence(self, id_: int) -> NormalizedStateSentence:
        return NormalizedStateSentence(
            state_code=self.state_code.value,
            external_id="SENTENCE-1",
            sentence_id=id_,
            sentence_group_external_id=None,
            sentence_inferred_group_id=self.inferred_group_id,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.STATE,
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

    def test_one_sentence_one_length(self) -> None:
        """
        The most simple case, a sentence is actively serving and has a single set of
        projected dates from imposition.
        """
        sentence_1 = self._make_sentence(1)
        sentence_1.sentence_lengths = [
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=111,
                length_update_datetime=self.critical_date_1,
                projected_completion_date_min_external=self.projected_date_1_min,
                projected_completion_date_max_external=None,
            )
        ]
        sentence_1.sentence_status_snapshots = [
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=114,
                status_update_datetime=self.critical_date_1,
                status_end_datetime=None,
                status=StateSentenceStatus.SERVING,
            ),
        ]
        expected_data = [
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_1,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1_min,
                "projected_full_term_release_date_max": None,
            },
        ]
        person = self._make_person()
        person.sentences = [sentence_1]
        self.run_test([person], expected_data)

    def test_one_sentence_intermittent_suspension_one_projected_date(self) -> None:
        """
        When an inferred group is composed of single sentence and
        that sentence becomes SUSPENDED, we want to have a row
        with the inferred_group_update_datetime of the suspension
        and all null projected dates.

        This way we have the suspension start accounted for in
        views like projected_full_term_release_date_spans if
        there is no state provided group level projected date.
        """
        sentence_1 = self._make_sentence(1)
        sentence_1.sentence_lengths = [
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=111,
                length_update_datetime=self.critical_date_1,
                projected_completion_date_min_external=self.projected_date_1_min,
                projected_completion_date_max_external=None,
            )
        ]
        sentence_1.sentence_status_snapshots = [
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=114,
                status_update_datetime=self.critical_date_1,
                status_end_datetime=self.critical_date_2,
                status=StateSentenceStatus.SERVING,
            ),
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=115,
                status_update_datetime=self.critical_date_2,
                status_end_datetime=self.critical_date_3,
                status=StateSentenceStatus.SUSPENDED,
            ),
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=116,
                status_update_datetime=self.critical_date_3,
                status_end_datetime=None,
                status=StateSentenceStatus.SERVING,
            ),
        ]
        expected_data = [
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_1,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1_min,
                "projected_full_term_release_date_max": None,
            },
            # SUSPENDED, ALL NULL
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_2,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_3,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1_min,
                "projected_full_term_release_date_max": None,
            },
        ]
        person = self._make_person()
        person.sentences = [sentence_1]
        self.run_test([person], expected_data)

    def test_one_sentence_intermittent_suspension_updated_projected_dates(self) -> None:
        """
        When an inferred group is composed of single sentence and
        that sentence becomes SUSPENDED, we want to have a row
        with the inferred_group_update_datetime of the suspension
        and all null projected dates.

        This test ensures that interleaved length and status updates result with
        the correct projected date. Any update_datetime where a sentence is SUSPENDED
        should be NULL. When the sentence is back to SERVING, we should see the most
        recent projected dates!
        """
        sentence_1 = self._make_sentence(1)
        sentence_1.sentence_lengths = [
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=111,
                length_update_datetime=self.critical_date_1,
                projected_completion_date_min_external=self.projected_date_1_min,
                projected_completion_date_max_external=None,
            ),
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=112,
                length_update_datetime=self.critical_date_2,
                projected_completion_date_min_external=self.projected_date_2_min,
                projected_completion_date_max_external=None,
            ),
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=113,
                length_update_datetime=self.critical_date_3,
                projected_completion_date_min_external=self.projected_date_3_min,
                projected_completion_date_max_external=None,
            ),
        ]
        sentence_1.sentence_status_snapshots = [
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=114,
                status_update_datetime=self.critical_date_1,
                status_end_datetime=self.suspended_dt,
                status=StateSentenceStatus.SERVING,
            ),
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=115,
                status_update_datetime=self.suspended_dt,
                status_end_datetime=self.back_to_serving_dt,
                status=StateSentenceStatus.SUSPENDED,
            ),
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=116,
                status_update_datetime=self.back_to_serving_dt,
                status_end_datetime=None,
                status=StateSentenceStatus.SERVING,
            ),
        ]
        expected_data = [
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_1,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1_min,
                "projected_full_term_release_date_max": None,
            },
            # SUSPENDED, ALL NULL
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.suspended_dt,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_2,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": None,
            },
            # BACK TO SERVING, THE MOST RECENT LENGTH UPDATE WAS FROM CRITICAL DATE 2
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.back_to_serving_dt,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_2_min,
                "projected_full_term_release_date_max": None,
            },
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_3,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_3_min,
                "projected_full_term_release_date_max": None,
            },
        ]
        person = self._make_person()
        person.sentences = [sentence_1]
        self.run_test([person], expected_data)

    def test_one_sentence_marked_completion_later(self) -> None:
        """
        Some states mark a sentence's completion after they
        have completed that sentence ¯\\_(ツ)_//¯

        This test ensures we do not have empty projected date
        rows for the statuses before the first projected date.
        """
        sentence_1 = self._make_sentence(1)
        later = self.critical_date_3 + datetime.timedelta(days=1)
        sentence_1.sentence_lengths = [
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=111,
                length_update_datetime=later,
                projected_completion_date_min_external=self.critical_date_3,
                projected_completion_date_max_external=None,
            )
        ]
        sentence_1.sentence_status_snapshots = [
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=114,
                status_update_datetime=self.critical_date_1,
                status_end_datetime=None,
                status=StateSentenceStatus.SERVING,
            ),
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=114,
                status_update_datetime=self.critical_date_2,
                status_end_datetime=None,
                status=StateSentenceStatus.COMPLETED,
            ),
        ]
        expected_data = [
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": later,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.critical_date_3.date(),
                "projected_full_term_release_date_max": None,
            },
        ]
        person = self._make_person()
        person.sentences = [sentence_1]
        self.run_test([person], expected_data)

    def test_only_active_sentences(self) -> None:
        """Tests that we take the maximum value of active sentences at each critical date."""
        sentence_1 = self._make_sentence(1)
        sentence_1.sentence_lengths = [
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=111,
                length_update_datetime=self.critical_date_1,
                projected_completion_date_min_external=self.projected_date_1_min,
                projected_completion_date_max_external=None,
            ),
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=112,
                length_update_datetime=self.critical_date_2,
                projected_completion_date_min_external=self.projected_date_2_min,
                projected_completion_date_max_external=None,
            ),
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=113,
                length_update_datetime=self.critical_date_3,
                projected_completion_date_min_external=self.projected_date_3_min,
                projected_completion_date_max_external=self.projected_date_3_max,
            ),
        ]
        sentence_1.sentence_status_snapshots = [
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=114,
                status_update_datetime=self.critical_date_1,
                status_end_datetime=None,
                status=StateSentenceStatus.SERVING,
            ),
        ]

        sentence_2 = self._make_sentence(2)
        sentence_2.sentence_lengths = [
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=121,
                length_update_datetime=self.critical_date_1,
                projected_completion_date_min_external=None,
                projected_completion_date_max_external=self.projected_date_1_max,
            ),
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=122,
                length_update_datetime=self.critical_date_2,
                projected_completion_date_min_external=self.projected_date_2_med,
                projected_completion_date_max_external=self.projected_date_2_max,
            ),
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=123,
                length_update_datetime=self.critical_date_3,
                projected_completion_date_min_external=self.projected_date_3_med,
                projected_completion_date_max_external=self.projected_date_3_max,
            ),
        ]
        sentence_2.sentence_status_snapshots = [
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=114,
                status_update_datetime=self.critical_date_1,
                status_end_datetime=None,
                status=StateSentenceStatus.SERVING,
            ),
        ]
        person = self._make_person()
        person.sentences = [sentence_1, sentence_2]

        expected_result = [
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_1,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1_min,
                "projected_full_term_release_date_max": self.projected_date_1_max,
            },
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_2,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_2_med,
                "projected_full_term_release_date_max": self.projected_date_2_max,
            },
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_3,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_3_med,
                "projected_full_term_release_date_max": self.projected_date_3_max,
            },
        ]
        self.run_test([person], expected_result)

    def test_one_terminated_sentence(self) -> None:
        """
        This example is two consecutive sentences, where sentence_1
        must be completed before sentence_2.

        The inferred group dates are then the values from sentence_2,
        which must be completed before release.
        """
        sentence_1 = self._make_sentence(1)
        sentence_1.sentence_lengths = [
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=111,
                length_update_datetime=self.critical_date_1,
                projected_completion_date_min_external=self.critical_date_3,
                projected_completion_date_max_external=self.critical_date_3,
            ),
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=112,
                length_update_datetime=self.critical_date_2,
                projected_completion_date_min_external=self.critical_date_3,
                projected_completion_date_max_external=self.critical_date_3,
            ),
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=113,
                length_update_datetime=self.critical_date_3,
                projected_completion_date_min_external=self.critical_date_3,
                projected_completion_date_max_external=self.critical_date_3,
            ),
        ]
        sentence_1.sentence_status_snapshots = [
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=114,
                status_update_datetime=self.critical_date_1,
                status_end_datetime=self.critical_date_3,
                status=StateSentenceStatus.SERVING,
            ),
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=115,
                status_update_datetime=self.critical_date_3,
                status_end_datetime=None,
                status=StateSentenceStatus.COMPLETED,
            ),
        ]

        sentence_2 = self._make_sentence(2)
        sentence_2.sentence_lengths = [
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=121,
                length_update_datetime=self.critical_date_1,
                projected_completion_date_min_external=self.projected_date_1_min,
                projected_completion_date_max_external=self.projected_date_1_max,
            ),
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=122,
                length_update_datetime=self.critical_date_2,
                projected_completion_date_min_external=self.projected_date_2_min,
                projected_completion_date_max_external=self.projected_date_2_max,
            ),
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=123,
                length_update_datetime=self.critical_date_3,
                projected_completion_date_min_external=self.projected_date_3_min,
                projected_completion_date_max_external=self.projected_date_3_max,
            ),
        ]
        # With consecutive sentences we typically see a an initial status at imposition,
        # we mark it as serving.
        sentence_2.sentence_status_snapshots = [
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=114,
                status_update_datetime=self.critical_date_1,
                status_end_datetime=None,
                status=StateSentenceStatus.SERVING,
            ),
        ]

        expected_result = [
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_1,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_1_min,
                "projected_full_term_release_date_max": self.projected_date_1_max,
            },
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_2,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_2_min,
                "projected_full_term_release_date_max": self.projected_date_2_max,
            },
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_3,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": self.projected_date_3_min,
                "projected_full_term_release_date_max": self.projected_date_3_max,
            },
        ]
        person = self._make_person()
        person.sentences = [sentence_1, sentence_2]
        self.run_test([person], expected_result)

    def test_suspended_then_active_sentence(self) -> None:
        """
        Tests two concurrent sentences with the same length_update_datetimes,
        and sentence_1 has larger projected dates than sentence_2.

        sentence_1 gets SUSPENDED between self.suspended_dt and self.back_to_serving_dt,
        so we should see the smaller projected date values of sentence_2 for that
        span of time.
        """
        sentence_1 = self._make_sentence(1)
        sentence_1.sentence_lengths = [
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=111,
                length_update_datetime=self.critical_date_1,
                projected_completion_date_max_external=self.projected_date_1_max,
            ),
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=112,
                length_update_datetime=self.critical_date_2,
                projected_completion_date_max_external=self.projected_date_2_max,
            ),
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=113,
                length_update_datetime=self.critical_date_3,
                projected_completion_date_max_external=self.projected_date_3_max,
            ),
        ]
        sentence_1.sentence_status_snapshots = [
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=114,
                status_update_datetime=self.critical_date_1,
                status_end_datetime=self.suspended_dt,
                status=StateSentenceStatus.SERVING,
            ),
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=115,
                status_update_datetime=self.suspended_dt,
                status_end_datetime=self.back_to_serving_dt,
                status=StateSentenceStatus.SUSPENDED,
            ),
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=116,
                status_update_datetime=self.back_to_serving_dt,
                status_end_datetime=None,
                status=StateSentenceStatus.SERVING,
            ),
        ]

        sentence_2 = self._make_sentence(2)
        sentence_2.sentence_lengths = [
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=221,
                length_update_datetime=self.critical_date_1,
                projected_completion_date_max_external=self.projected_date_1_min,
            ),
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=222,
                length_update_datetime=self.critical_date_2,
                projected_completion_date_max_external=self.projected_date_2_min,
            ),
            NormalizedStateSentenceLength(
                state_code=self.state_code.value,
                sentence_length_id=223,
                length_update_datetime=self.critical_date_3,
                projected_completion_date_max_external=self.projected_date_3_min,
            ),
        ]
        sentence_2.sentence_status_snapshots = [
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code.value,
                sentence_status_snapshot_id=214,
                status_update_datetime=self.critical_date_1,
                status_end_datetime=None,
                status=StateSentenceStatus.SERVING,
            ),
        ]

        expected_result = [
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_1,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_1_max,
            },
            # sentence_1 is SUSPENDED, so now we see sentence_2 projected dates
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.suspended_dt,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_1_min,
            },
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_2,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_2_min,
            },
            # sentence_1 is SERVING, so now we see sentence_1 projected dates
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.back_to_serving_dt,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_2_max,
            },
            {
                "state_code": self.state_code.value,
                "sentence_inferred_group_id": self.inferred_group_id,
                "inferred_group_update_datetime": self.critical_date_3,
                "parole_eligibility_date": None,
                "projected_parole_release_date": None,
                "projected_full_term_release_date_min": None,
                "projected_full_term_release_date_max": self.projected_date_3_max,
            },
        ]
        person = self._make_person()
        person.sentences = [sentence_1, sentence_2]
        self.run_test([person], expected_result)
