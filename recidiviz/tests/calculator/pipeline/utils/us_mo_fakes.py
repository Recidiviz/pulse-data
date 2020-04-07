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
from datetime import date
from typing import Optional

import attr

from recidiviz.calculator.pipeline.utils.us_mo_sentence_classification import UsMoSupervisionSentence, \
    UsMoIncarcerationSentence
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.persistence.entity.state.entities import StateSupervisionSentence, StateIncarcerationSentence


@attr.s
class FakeUsMoSupervisionSentence(UsMoSupervisionSentence):
    """Fake UsMoSupervisionSentence that allows you to set the supervision type manually."""

    test_supervision_type: Optional[StateSupervisionType] = attr.ib()
    @test_supervision_type.default
    def default_test_supervision_type(self):
        raise ValueError('Must set test_supervision_type')

    def get_sentence_supervision_type_on_day(
            self,
            _supervision_type_day: date
    ) -> Optional[StateSupervisionType]:
        return self.test_supervision_type

    @classmethod
    def fake_sentence_from_sentence(
            cls,
            sentence: StateSupervisionSentence,
            supervision_type: Optional[StateSupervisionType]
    ) -> UsMoSupervisionSentence:
        sentence = FakeUsMoSupervisionSentence.from_supervision_sentence(sentence,
                                                                         sentence_statuses_raw=[],
                                                                         subclass_args={
                                                                             'test_supervision_type': supervision_type
                                                                         })
        return sentence


@attr.s
class FakeUsMoIncarcerationSentence(UsMoIncarcerationSentence):
    """Fake UsMoIncarcerationSentence that allows you to set the supervision type manually."""

    test_supervision_type: Optional[StateSupervisionType] = attr.ib()
    @test_supervision_type.default
    def default_test_supervision_type(self):
        raise ValueError('Must set test_supervision_type')

    def get_sentence_supervision_type_on_day(
            self,
            _supervision_type_day: date
    ) -> Optional[StateSupervisionType]:
        return self.test_supervision_type

    @classmethod
    def fake_sentence_from_sentence(
            cls,
            sentence: StateIncarcerationSentence,
            supervision_type: Optional[StateSupervisionType]
    ) -> UsMoIncarcerationSentence:
        sentence = FakeUsMoIncarcerationSentence.from_incarceration_sentence(
            sentence,
            sentence_statuses_raw=[],
            subclass_args={
                'test_supervision_type': supervision_type
            })
        return sentence
