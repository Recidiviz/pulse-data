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
"""State agnostic testing of normalization of sentencing v2 entities."""
import datetime
import unittest
from typing import List

import attrs

from recidiviz.common.constants.state.state_charge import StateChargeV2Status
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import as_datetime
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_utils import set_backedges
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state.entities import (
    StateChargeV2,
    StateSentence,
    StateSentenceGroup,
    StateSentenceGroupLength,
    StateSentenceLength,
    StateSentenceStatusSnapshot,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateChargeV2,
    NormalizedStateSentence,
    NormalizedStateSentenceStatusSnapshot,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)
from recidiviz.pipelines.ingest.state.normalization.sentencing.normalize_all_sentencing_entities import (
    get_normalized_sentence_groups,
    get_normalized_sentences_for_person,
    normalize_charge_v2,
)
from recidiviz.utils.types import assert_type


class FakeDelegateThatCorrectsCompleted(StateSpecificSentenceNormalizationDelegate):
    @property
    def correct_early_completed_statuses(self) -> bool:
        return True


class FakeDelegateThatInfersProjectedDatesFromLengthDays(
    StateSpecificSentenceNormalizationDelegate
):
    @property
    def override_projected_completion_dates_using_sentence_length_days(self) -> bool:
        return True


class TestSentenceV2Normalization(unittest.TestCase):
    """Tests the normalization functionality for V2 sentencing entities."""

    def setUp(self) -> None:
        self.state_code = StateCode.US_XX
        self.state_code_value = "US_XX"
        self.start_date = datetime.date(2022, 1, 1)
        self.start_dt = as_datetime(self.start_date)
        self.delegate = StateSpecificSentenceNormalizationDelegate()

    def _get_normalized_sentences_for_person(
        self,
        person: state_entities.StatePerson,
        delegate: StateSpecificSentenceNormalizationDelegate,
    ) -> list[NormalizedStateSentence]:
        entities_module_context = entities_module_context_for_module(state_entities)
        person = assert_type(
            set_backedges(person, entities_module_context), state_entities.StatePerson
        )
        return get_normalized_sentences_for_person(person, delegate)

    def _new_charge(self, id_num: int) -> StateChargeV2:
        return StateChargeV2(
            state_code=self.state_code_value,
            external_id=f"charge-{id_num}",
            charge_v2_id=id_num,
            status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
            description="Very Descriptive",
            is_violent=False,
        )

    def _new_sentence(self, id_num: int) -> StateSentence:
        return StateSentence(
            state_code=self.state_code_value,
            external_id=f"sentence-{id_num}",
            sentence_id=id_num,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.STATE,
            imposed_date=self.start_date,
        )

    def _new_sentence_group(self, id_num: int) -> StateSentenceGroup:
        return StateSentenceGroup(
            state_code=self.state_code_value,
            external_id=f"sentence-group-{id_num}",
            sentence_group_id=id_num,
        )

    def test_normalized_charge_v2_has_external_fields(self) -> None:
        """Ensures we add 'exteral' fields to charges."""
        charge = self._new_charge(1)
        normalized_charge = normalize_charge_v2(charge)
        for field in attrs.fields(StateChargeV2):
            self.assertEqual(
                getattr(charge, field.name), getattr(normalized_charge, field.name)
            )
        for field in attrs.fields(NormalizedStateChargeV2):
            if field.name.endswith("_external") and "category" not in field.name:
                og_name = field.name.removesuffix("_external")
                self.assertEqual(
                    getattr(normalized_charge, field.name),
                    getattr(normalized_charge, og_name),
                )

    def test_get_normalized_sentences_one_to_one(self) -> None:
        """Tests normalization when there is one sentence to one charge."""
        charge = self._new_charge(1)
        sentence = self._new_sentence(1)
        sentence.charges = [charge]
        charge.sentences = [sentence]

        person = state_entities.StatePerson(
            state_code=self.state_code_value,
            sentences=[sentence],
        )
        normalized_sentences = self._get_normalized_sentences_for_person(
            person, self.delegate
        )
        normalized_sentence = normalized_sentences[0]
        assert len(normalized_sentence.charges) == 1
        normalized_charge = normalized_sentence.charges[0]
        assert normalized_sentence == normalized_charge.sentences[0]

    def test_get_normalized_sentences_one_to_many(self) -> None:
        """Tests normalization when there are many sentences to one charge."""
        sentence = self._new_sentence(1)
        charges = [
            self._new_charge(1),
            self._new_charge(2),
        ]
        sentence.charges = charges
        for charge in charges:
            charge.sentences = [sentence]
        person = state_entities.StatePerson(
            state_code=self.state_code_value,
            sentences=[sentence],
        )
        normalized_sentences = self._get_normalized_sentences_for_person(
            person, self.delegate
        )
        assert len(normalized_sentences) == 1
        normalized_sentence = normalized_sentences[0]
        assert len(normalized_sentence.charges) == 2
        for normalized_charge in normalized_sentence.charges:
            assert normalized_sentence == normalized_charge.sentences[0]

    def test_get_normalized_sentences_many_to_one(self) -> None:
        """Tests normalization when there are many sentences to one charge."""
        charge = self._new_charge(1)
        sentences = [
            self._new_sentence(1),
            self._new_sentence(2),
        ]
        charge.sentences = sentences
        for sentence in sentences:
            sentence.charges = [charge]
        person = state_entities.StatePerson(
            state_code=self.state_code_value,
            sentences=sentences,
        )
        normalized_sentences = self._get_normalized_sentences_for_person(
            person, self.delegate
        )
        for normalized_sentence in normalized_sentences:
            assert len(normalized_sentence.charges) == 1
            normalized_charge = normalized_sentence.charges[0]
            assert normalized_sentence in normalized_charge.sentences

    def test_get_normalized_sentences_many_to_many(self) -> None:
        """Tests normalization when there are many sentences to many charges."""
        charges: List[StateChargeV2] = [
            self._new_charge(0),
            self._new_charge(1),
        ]
        sentences: List[StateSentence] = [
            self._new_sentence(0),
            self._new_sentence(1),
            self._new_sentence(2),
        ]
        # Setup all sentences to be from first charge,
        for sentence in sentences:
            sentence.charges.append(charges[0])
            charges[0].sentences.append(sentence)
        # and the last sentence to be from both charges.
        sentences[2].charges.append(charges[1])
        charges[1].sentences.append(sentences[2])

        person = state_entities.StatePerson(
            state_code=self.state_code_value,
            sentences=sentences,
        )
        normalized_sentences = self._get_normalized_sentences_for_person(
            person, self.delegate
        )
        assert len(normalized_sentences) == 3

        # Check the first charge is related to all sentences
        for normalized_sentence in normalized_sentences:
            n_charges: List[NormalizedStateChargeV2] = sorted(
                normalized_sentence.charges,
                key=lambda c: c.charge_v2_id,
            )
            assert n_charges[0].charge_v2_id == 0
            assert normalized_sentence in n_charges[0].sentences

        # Check the last sentence is related to both charges
        last_sentence = sorted(
            normalized_sentences, key=lambda s: assert_type(s.sentence_id, int)
        )[-1]
        assert len(last_sentence.charges) == 2
        for charge in last_sentence.charges:
            assert last_sentence in charge.sentences

    def test_get_normalized_sentences_length_empty_projected_fields(self) -> None:
        sentence = self._new_sentence(1)
        sentence.sentence_status_snapshots = [
            StateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                sentence_status_snapshot_id=1,
                status=StateSentenceStatus.SERVING,
                status_update_datetime=self.start_dt,
                sentence=sentence,
            )
        ]
        sentence.sentence_lengths = [
            StateSentenceLength(
                state_code=self.state_code_value,
                sentence_length_id=111,
                length_update_datetime=self.start_dt,
                sentence_length_days_min=42,
                sentence=sentence,
            ),
            StateSentenceLength(
                state_code=self.state_code_value,
                sentence_length_id=222,
                length_update_datetime=self.start_dt + datetime.timedelta(days=10),
                sentence_length_days_min=49,
                sentence=sentence,
            ),
        ]
        person = state_entities.StatePerson(
            state_code=self.state_code_value,
            sentences=[sentence],
        )
        normalized_sentence = self._get_normalized_sentences_for_person(
            person, self.delegate
        )[0]
        normalized_lengths = sorted(
            normalized_sentence.sentence_lengths, key=lambda s: s.partition_key
        )
        assert len(normalized_lengths) == 2
        first, second = normalized_lengths
        assert first.sequence_num == 1
        assert first.sentence_length_days_min == 42
        assert second.sequence_num == 2
        assert second.sentence_length_days_min == 49

        # Now test we can infer completion dates.
        normalized_sentence = self._get_normalized_sentences_for_person(
            person, FakeDelegateThatInfersProjectedDatesFromLengthDays()
        )[0]
        normalized_lengths = sorted(
            normalized_sentence.sentence_lengths, key=lambda s: s.partition_key
        )
        assert len(normalized_lengths) == 2
        first, second = normalized_lengths
        assert first.sequence_num == 1
        assert first.sentence_length_days_min == 42
        expected_min = self.start_date + datetime.timedelta(days=42)
        assert first.projected_completion_date_min_external == expected_min
        assert not first.projected_completion_date_max_external

        assert second.sequence_num == 2
        assert second.sentence_length_days_min == 49
        expected_max = self.start_date + datetime.timedelta(days=49)
        assert second.projected_completion_date_min_external == expected_max
        assert not second.projected_completion_date_max_external

    def test_get_normalized_sentences_length_inconsistent_projected_fields(
        self,
    ) -> None:
        sentence = self._new_sentence(1)
        sentence.sentence_lengths = [
            StateSentenceLength(
                state_code=self.state_code_value,
                sentence_length_id=111,
                length_update_datetime=self.start_dt,
                # These are intended to be backwards
                sentence_length_days_max=142,
                sentence_length_days_min=149,
                # These are intended to be backwards
                projected_completion_date_max_external=self.start_date
                + datetime.timedelta(days=142),
                projected_completion_date_min_external=self.start_date
                + datetime.timedelta(days=149),
            ),
            StateSentenceLength(
                state_code=self.state_code_value,
                sentence_length_id=222,
                length_update_datetime=self.start_dt + datetime.timedelta(days=10),
                # These are intended to not be backwards
                sentence_length_days_max=115,
                sentence_length_days_min=100,
                # These are intended to not be backwards
                projected_completion_date_max_external=self.start_date
                + datetime.timedelta(days=115),
                projected_completion_date_min_external=self.start_date
                + datetime.timedelta(days=100),
            ),
        ]
        person = state_entities.StatePerson(
            state_code=self.state_code_value,
            sentences=[sentence],
        )
        normalized_sentence = self._get_normalized_sentences_for_person(
            person, self.delegate
        )[0]
        normalized_lengths = sorted(
            normalized_sentence.sentence_lengths, key=lambda s: s.partition_key
        )
        assert len(normalized_lengths) == 2
        first, second = normalized_lengths
        assert first.sequence_num == 1
        assert first.sentence_length_days_min == 142
        assert (
            first.projected_completion_date_min_external
            == self.start_date + datetime.timedelta(days=142)
        )
        assert first.sentence_length_days_max == 149
        assert (
            first.projected_completion_date_max_external
            == self.start_date + datetime.timedelta(days=149)
        )
        assert second.sequence_num == 2
        assert second.sentence_length_days_min == 100
        assert (
            second.projected_completion_date_min_external
            == self.start_date + datetime.timedelta(days=100)
        )
        assert second.sentence_length_days_max == 115
        assert (
            second.projected_completion_date_max_external
            == self.start_date + datetime.timedelta(days=115)
        )

    def test_get_normalized_sentences_override_projected_dates_does_nothing_with_no_serving_status(
        self,
    ) -> None:
        """Calculating sentence length from initial serving means we must have a SERVING status."""
        sentence = self._new_sentence(1)
        sentence.sentence_lengths = [
            StateSentenceLength(
                state_code=self.state_code_value,
                sentence_length_id=111,
                length_update_datetime=self.start_dt,
                sentence_length_days_max=142,
                sentence_length_days_min=None,
                projected_completion_date_max_external=None,
                projected_completion_date_min_external=None,
            ),
        ]
        person = state_entities.StatePerson(
            state_code=self.state_code_value,
            sentences=[sentence],
        )
        normalized_sentence = self._get_normalized_sentences_for_person(
            person, self.delegate
        )[0]
        length = normalized_sentence.sentence_lengths[0]
        assert length.sentence_length_days_max == 142
        assert not length.projected_completion_date_min_external
        assert not length.projected_completion_date_max_external

    def test_get_normalized_sentences_length_inconsistent_projected_fields_w_override(
        self,
    ) -> None:
        sentence = self._new_sentence(1)
        sentence.sentence_status_snapshots = [
            StateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                sentence_status_snapshot_id=1,
                status=StateSentenceStatus.SERVING,
                status_update_datetime=self.start_dt,
                sentence=sentence,
            )
        ]
        sentence.sentence_lengths = [
            StateSentenceLength(
                state_code=self.state_code_value,
                sentence_length_id=111,
                length_update_datetime=self.start_dt,
                # These are intended to be backwards
                sentence_length_days_max=142,
                sentence_length_days_min=149,
                # One is missing, but these will be ignored
                projected_completion_date_max_external=None,
                projected_completion_date_min_external=self.start_date
                + datetime.timedelta(days=149),
            ),
            StateSentenceLength(
                state_code=self.state_code_value,
                sentence_length_id=222,
                length_update_datetime=self.start_dt + datetime.timedelta(days=10),
                # These are intended to not be backwards
                sentence_length_days_max=115,
                sentence_length_days_min=100,
                # These are intended to not be backwards
                projected_completion_date_max_external=self.start_date
                + datetime.timedelta(days=115),
                projected_completion_date_min_external=self.start_date
                + datetime.timedelta(days=100),
            ),
        ]
        person = state_entities.StatePerson(
            state_code=self.state_code_value,
            sentences=[sentence],
        )
        normalized_sentence = self._get_normalized_sentences_for_person(
            person, FakeDelegateThatInfersProjectedDatesFromLengthDays()
        )[0]
        normalized_lengths = sorted(
            normalized_sentence.sentence_lengths, key=lambda s: s.partition_key
        )
        assert len(normalized_lengths) == 2
        first, second = normalized_lengths
        assert first.sequence_num == 1
        assert first.sentence_length_days_min == 142
        assert first.sentence_length_days_max == 149
        expected_proj_min = self.start_date + datetime.timedelta(days=142)
        expected_proj_max = self.start_date + datetime.timedelta(days=149)
        assert first.projected_completion_date_min_external == expected_proj_min
        assert first.projected_completion_date_max_external == expected_proj_max

        assert second.sequence_num == 2
        assert second.sentence_length_days_min == 100
        assert second.sentence_length_days_max == 115
        expected_proj_min = self.start_date + datetime.timedelta(days=100)
        expected_proj_max = self.start_date + datetime.timedelta(days=115)
        assert second.projected_completion_date_min_external == expected_proj_min
        assert second.projected_completion_date_max_external == expected_proj_max

    def test_get_normalized_sentence_groups_no_lengths(self) -> None:
        """Tests that all sentence group properties are persisted in normalization."""
        sentence_groups = [self._new_sentence_group(i) for i in range(5)]
        normalized_groups = get_normalized_sentence_groups(sentence_groups)
        for i in range(5):
            group = sentence_groups[i]
            normalized_group = normalized_groups[i]
            assert group.external_id == normalized_group.external_id
            assert group.sentence_group_id == normalized_group.sentence_group_id
            assert group.state_code == normalized_group.state_code

    def test_get_normalized_sentence_groups_length_empty_projected_fields(self) -> None:
        group = self._new_sentence_group(1)
        group.sentence_group_lengths = [
            StateSentenceGroupLength(
                state_code=self.state_code_value,
                sentence_group_length_id=111,
                group_update_datetime=self.start_dt,
                parole_eligibility_date_external=self.start_date
                + datetime.timedelta(days=100),
            ),
            StateSentenceGroupLength(
                state_code=self.state_code_value,
                sentence_group_length_id=222,
                group_update_datetime=self.start_dt + datetime.timedelta(days=10),
                parole_eligibility_date_external=self.start_date
                + datetime.timedelta(days=90),
            ),
        ]
        normalized_group = get_normalized_sentence_groups([group])[0]
        normalized_lengths = sorted(
            normalized_group.sentence_group_lengths, key=lambda s: s.partition_key
        )
        assert len(normalized_lengths) == 2
        first, second = normalized_lengths
        assert first.sequence_num == 1
        assert first.sentence_group_length_id == 111
        assert (
            first.parole_eligibility_date_external
            == self.start_date + datetime.timedelta(100)
        )
        assert second.sequence_num == 2
        assert second.sentence_group_length_id == 222
        assert (
            second.parole_eligibility_date_external
            == self.start_date + datetime.timedelta(90)
        )

    def test_get_normalized_sentence_groups_length_inconsistent_projected_fields(
        self,
    ) -> None:
        group = self._new_sentence_group(1)
        group.sentence_group_lengths = [
            StateSentenceGroupLength(
                state_code=self.state_code_value,
                sentence_group_length_id=111,
                group_update_datetime=self.start_dt,
                parole_eligibility_date_external=self.start_date
                + datetime.timedelta(days=100),
                # Intended to be more than the max value
                projected_full_term_release_date_min_external=self.start_date
                + datetime.timedelta(days=200),
                projected_full_term_release_date_max_external=self.start_date
                + datetime.timedelta(days=190),
            ),
            StateSentenceGroupLength(
                state_code=self.state_code_value,
                sentence_group_length_id=222,
                group_update_datetime=self.start_dt + datetime.timedelta(days=10),
                parole_eligibility_date_external=self.start_date
                + datetime.timedelta(days=90),
                # Intended to be less than the max value
                projected_full_term_release_date_min_external=self.start_date
                + datetime.timedelta(days=180),
                projected_full_term_release_date_max_external=self.start_date
                + datetime.timedelta(days=190),
            ),
        ]

        normalized_group = get_normalized_sentence_groups([group])[0]
        normalized_lengths = sorted(
            normalized_group.sentence_group_lengths, key=lambda s: s.partition_key
        )
        assert len(normalized_lengths) == 2
        first, second = normalized_lengths
        assert first.sequence_num == 1
        assert first.sentence_group_length_id == 111
        assert (
            first.parole_eligibility_date_external
            == self.start_date + datetime.timedelta(100)
        )
        assert (
            first.projected_full_term_release_date_min_external
            == self.start_date + datetime.timedelta(190)
        )
        assert (
            first.projected_full_term_release_date_max_external
            == self.start_date + datetime.timedelta(200)
        )
        assert second.sequence_num == 2
        assert second.sentence_group_length_id == 222
        assert (
            second.parole_eligibility_date_external
            == self.start_date + datetime.timedelta(90)
        )
        assert (
            second.projected_full_term_release_date_min_external
            == self.start_date + datetime.timedelta(180)
        )
        assert (
            second.projected_full_term_release_date_max_external
            == self.start_date + datetime.timedelta(190)
        )

    def test_normalize_sentences_serving_and_completed_status(self) -> None:
        sentence = self._new_sentence(1)
        second_dt = self.start_dt + datetime.timedelta(days=90)
        sentence.sentence_status_snapshots = [
            StateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=self.start_dt,
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=1,
            ),
            StateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=second_dt,
                status=StateSentenceStatus.COMPLETED,
                sentence_status_snapshot_id=2,
            ),
        ]

        person = state_entities.StatePerson(
            state_code=self.state_code_value,
            sentences=[sentence],
        )
        normalized_sentence = self._get_normalized_sentences_for_person(
            person, self.delegate
        )[0]
        expected_normalized_status_snapshots = [
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=self.start_dt,
                status_end_datetime=second_dt,
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=1,
                sequence_num=1,
            ),
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=second_dt,
                status_end_datetime=None,
                status=StateSentenceStatus.COMPLETED,
                sentence_status_snapshot_id=2,
                sequence_num=2,
            ),
        ]
        for snapshot in normalized_sentence.sentence_status_snapshots:
            snapshot.sentence = None
        assert (
            normalized_sentence.sentence_status_snapshots
            == expected_normalized_status_snapshots
        )

    def test_normalize_sentences_only_serving_status(self) -> None:
        sentence = self._new_sentence(1)
        second_dt = self.start_dt + datetime.timedelta(days=90)
        sentence.sentence_status_snapshots = [
            StateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=self.start_dt,
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=1,
            ),
            StateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=second_dt,
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=2,
            ),
        ]
        person = state_entities.StatePerson(
            state_code=self.state_code_value,
            sentences=[sentence],
        )
        normalized_sentence = self._get_normalized_sentences_for_person(
            person, self.delegate
        )[0]
        expected_normalized_status_snapshots = [
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=self.start_dt,
                status_end_datetime=second_dt,
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=1,
                sequence_num=1,
            ),
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=second_dt,
                status_end_datetime=None,
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=2,
                sequence_num=2,
            ),
        ]
        for snapshot in normalized_sentence.sentence_status_snapshots:
            snapshot.sentence = None
        assert (
            normalized_sentence.sentence_status_snapshots
            == expected_normalized_status_snapshots
        )

    def test_normalize_sentences_early_terminating_statuses(self) -> None:
        sentence = self._new_sentence(1)
        second_dt = self.start_dt + datetime.timedelta(days=90)
        third_dt = second_dt + datetime.timedelta(days=90)
        sentence.sentence_status_snapshots = [
            StateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=self.start_dt,
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=1,
            ),
            StateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=second_dt,
                status=StateSentenceStatus.COMPLETED,
                sentence_status_snapshot_id=2,
            ),
            StateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=third_dt,
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=3,
            ),
        ]
        person = state_entities.StatePerson(
            state_code=self.state_code_value,
            sentences=[sentence],
        )
        normalized_sentence = self._get_normalized_sentences_for_person(
            person, FakeDelegateThatCorrectsCompleted()
        )[0]
        expected_normalized_status_snapshots = [
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=self.start_dt,
                status_end_datetime=second_dt,
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=1,
                sequence_num=1,
            ),
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=second_dt,
                status_end_datetime=third_dt,
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=2,
                sequence_num=2,
            ),
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=third_dt,
                status_end_datetime=None,
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=3,
                sequence_num=3,
            ),
        ]
        for snapshot in normalized_sentence.sentence_status_snapshots:
            snapshot.sentence = None
        assert (
            normalized_sentence.sentence_status_snapshots
            == expected_normalized_status_snapshots
        )

        # Early terminating statuses should raise an Error
        sentence = self._new_sentence(1)
        sentence.sentence_status_snapshots = [
            StateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=self.start_dt,
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=1,
            ),
            StateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=second_dt,
                status=StateSentenceStatus.VACATED,
                sentence_status_snapshot_id=2,
            ),
            StateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=third_dt,
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=3,
            ),
        ]
        person = state_entities.StatePerson(
            state_code=self.state_code_value,
            sentences=[sentence],
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Found \[VACATED\] status that is not the final status. StateSentenceStatusSnapshot\(sentence_status_snapshot_id=2\)",
        ):
            _ = self._get_normalized_sentences_for_person(person, self.delegate)

    def test_normalize_sentences_statuses_on_same_day(self) -> None:
        """
        Inspired by US_MO, tests having multiple statuses on the same day
        with increasing but not contiguous sequence_num
        """
        sentence = self._new_sentence(1)
        second_dt = self.start_dt + datetime.timedelta(days=90)
        sentence.sentence_status_snapshots = [
            StateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=self.start_dt,
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=1,
                sequence_num=4,
            ),
            StateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=datetime.datetime(2022, 1, 1, 14, 30),
                status=StateSentenceStatus.INTERNAL_UNKNOWN,
                sentence_status_snapshot_id=7,
                sequence_num=7,
            ),
            StateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=datetime.datetime(2022, 1, 1, 17, 30),
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=8,
                sequence_num=8,
            ),
            StateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=second_dt,
                status=StateSentenceStatus.COMPLETED,
                sentence_status_snapshot_id=11,
                sequence_num=11,
            ),
        ]
        person = state_entities.StatePerson(
            state_code=self.state_code_value,
            sentences=[sentence],
        )
        normalized_sentence = self._get_normalized_sentences_for_person(
            person, self.delegate
        )[0]
        expected_normalized_status_snapshots = [
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=self.start_dt,
                status_end_datetime=datetime.datetime(2022, 1, 1, 14, 30),
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=1,
                sequence_num=1,
            ),
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=datetime.datetime(2022, 1, 1, 14, 30),
                status_end_datetime=datetime.datetime(2022, 1, 1, 17, 30),
                status=StateSentenceStatus.INTERNAL_UNKNOWN,
                sentence_status_snapshot_id=7,
                sequence_num=2,
            ),
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=datetime.datetime(2022, 1, 1, 17, 30),
                status_end_datetime=second_dt,
                status=StateSentenceStatus.SERVING,
                sentence_status_snapshot_id=8,
                sequence_num=3,
            ),
            NormalizedStateSentenceStatusSnapshot(
                state_code=self.state_code_value,
                status_update_datetime=second_dt,
                status_end_datetime=None,
                status=StateSentenceStatus.COMPLETED,
                sentence_status_snapshot_id=11,
                sequence_num=4,
            ),
        ]
        for snapshot in normalized_sentence.sentence_status_snapshots:
            snapshot.sentence = None
        assert (
            normalized_sentence.sentence_status_snapshots
            == expected_normalized_status_snapshots
        )
