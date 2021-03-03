# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""File containing fake / test implementations of US_MO-specific classes."""
from typing import List

import attr

from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import (
    UsMoSupervisionSentence,
    UsMoIncarcerationSentence,
    SupervisionTypeSpan,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionSentence,
    StateIncarcerationSentence,
)


@attr.s
class FakeUsMoSupervisionSentence(UsMoSupervisionSentence):
    """Fake UsMoSupervisionSentence that allows you to set the supervision type spans manually."""

    @classmethod
    def fake_sentence_from_sentence(
        cls,
        sentence: StateSupervisionSentence,
        supervision_type_spans: List[SupervisionTypeSpan],
    ) -> UsMoSupervisionSentence:
        sentence = FakeUsMoSupervisionSentence.from_supervision_sentence(
            sentence,
            sentence_statuses_raw=[],
            subclass_args={"supervision_type_spans": supervision_type_spans},
        )
        return sentence


@attr.s
class FakeUsMoIncarcerationSentence(UsMoIncarcerationSentence):
    """Fake UsMoIncarcerationSentence that allows you to set the supervision type spans manually."""

    @classmethod
    def fake_sentence_from_sentence(
        cls,
        sentence: StateIncarcerationSentence,
        supervision_type_spans: List[SupervisionTypeSpan],
    ) -> UsMoIncarcerationSentence:
        sentence = FakeUsMoIncarcerationSentence.from_incarceration_sentence(
            sentence,
            sentence_statuses_raw=[],
            subclass_args={"supervision_type_spans": supervision_type_spans},
        )
        return sentence
