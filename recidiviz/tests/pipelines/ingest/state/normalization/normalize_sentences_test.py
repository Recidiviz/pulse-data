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
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.persistence.entity.state.entities import (
    StateChargeV2,
    StateSentence,
    StateSentenceLength,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateChargeV2,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_sentences import (
    get_normalized_sentences,
    normalize_charge_v2,
)
from recidiviz.utils.types import assert_type


class TestSentenceV2Normalization(unittest.TestCase):
    """Tests the normalization functionality for V2 sentencing entities."""

    def setUp(self) -> None:
        self.state_code_value = "US_XX"

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
            imposed_date=datetime.date(2022, 2, 2),
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
        normalized_sentences = get_normalized_sentences([sentence])
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
        normalized_sentences = get_normalized_sentences([sentence])
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
        normalized_sentences = get_normalized_sentences(sentences)
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

        normalized_sentences = get_normalized_sentences(sentences)
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
        start_date = datetime.datetime(2022, 1, 1)
        sentence.sentence_lengths = [
            StateSentenceLength(
                state_code=self.state_code_value,
                sentence_length_id=111,
                length_update_datetime=start_date,
                sentence_length_days_min=42,
            ),
            StateSentenceLength(
                state_code=self.state_code_value,
                sentence_length_id=222,
                length_update_datetime=start_date + datetime.timedelta(days=10),
                sentence_length_days_min=49,
            ),
        ]
        normalized_sentence = get_normalized_sentences([sentence])[0]
        normalized_lengths = sorted(
            normalized_sentence.sentence_lengths, key=lambda s: s.partition_key
        )
        assert len(normalized_lengths) == 2
        first, second = normalized_lengths
        assert first.sequence_num == 1
        assert first.sentence_length_days_min == 42
        assert second.sequence_num == 2
        assert second.sentence_length_days_min == 49

    def test_get_normalized_sentences_length_inconsistent_projected_fields(
        self,
    ) -> None:
        sentence = self._new_sentence(1)
        start_date = datetime.datetime(2022, 1, 1)
        sentence.sentence_lengths = [
            StateSentenceLength(
                state_code=self.state_code_value,
                sentence_length_id=111,
                length_update_datetime=start_date,
                # These are intended to be backwards
                sentence_length_days_max=142,
                sentence_length_days_min=149,
                # These are intended to be backwards
                projected_completion_date_max_external=start_date
                + datetime.timedelta(days=142),
                projected_completion_date_min_external=start_date
                + datetime.timedelta(days=149),
            ),
            StateSentenceLength(
                state_code=self.state_code_value,
                sentence_length_id=222,
                length_update_datetime=start_date + datetime.timedelta(days=10),
                # These are intended to not be backwards
                sentence_length_days_max=115,
                sentence_length_days_min=100,
                # These are intended to not be backwards
                projected_completion_date_max_external=start_date
                + datetime.timedelta(days=115),
                projected_completion_date_min_external=start_date
                + datetime.timedelta(days=100),
            ),
        ]
        normalized_sentence = get_normalized_sentences([sentence])[0]
        normalized_lengths = sorted(
            normalized_sentence.sentence_lengths, key=lambda s: s.partition_key
        )
        assert len(normalized_lengths) == 2
        first, second = normalized_lengths
        assert first.sequence_num == 1
        assert first.sentence_length_days_min == 142
        assert (
            first.projected_completion_date_min_external
            == start_date + datetime.timedelta(days=142)
        )
        assert first.sentence_length_days_max == 149
        assert (
            first.projected_completion_date_max_external
            == start_date + datetime.timedelta(days=149)
        )
        assert second.sequence_num == 2
        assert second.sentence_length_days_min == 100
        assert (
            second.projected_completion_date_min_external
            == start_date + datetime.timedelta(days=100)
        )
        assert second.sentence_length_days_max == 115
        assert (
            second.projected_completion_date_max_external
            == start_date + datetime.timedelta(days=115)
        )
