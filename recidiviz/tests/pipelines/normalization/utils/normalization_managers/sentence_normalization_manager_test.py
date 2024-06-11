# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for sentence_normalization_manager.py."""
import unittest

from recidiviz.common.constants.state.state_charge import StateChargeStatus
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.entity.state.entities import (
    StateCharge,
    StateIncarcerationSentence,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.sentence_normalization_manager import (
    SentenceNormalizationManager,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_sentence_normalization_delegate import (
    UsXxSentenceNormalizationDelegate,
)


class TestSentenceNormalizationManager(unittest.TestCase):
    """Tests the sentence_normalization_manager.py."""

    def test_charge_metadata_gets_added(self) -> None:
        charge = StateCharge.new_with_defaults(
            state_code="US_XX",
            external_id="c-1",
            charge_id=1,
            status=StateChargeStatus.PRESENT_WITHOUT_INFO,
            description="FRAUD",
        )
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            external_id="i-1",
            incarceration_sentence_id=1,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge],
        )
        charge.incarceration_sentences = [incarceration_sentence]
        sentence_normalization_manager = SentenceNormalizationManager(
            incarceration_sentences=[incarceration_sentence],
            supervision_sentences=[],
            delegate=UsXxSentenceNormalizationDelegate(),
        )

        (
            processed_inc_sentences,
            additional_attributes,
        ) = (
            sentence_normalization_manager.normalized_incarceration_sentences_and_additional_attributes()
        )

        self.assertEqual([incarceration_sentence], processed_inc_sentences)
        self.assertDictEqual(
            additional_attributes,
            {
                StateIncarcerationSentence.__name__: {},
                StateCharge.__name__: {
                    1: {
                        "ncic_code_external": charge.ncic_code,
                        "ncic_category_external": None,
                        "description_external": charge.description,
                        "is_violent_external": charge.is_violent,
                        "is_drug_external": charge.is_drug,
                        "is_sex_offense_external": charge.is_sex_offense,
                    }
                },
            },
        )
