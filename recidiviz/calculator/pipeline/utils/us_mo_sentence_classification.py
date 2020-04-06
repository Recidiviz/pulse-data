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
"""Missouri-specific code for modeling sentence based on sentence statuses from table TAK026."""

from datetime import date
from typing import Optional, Dict, List, Any, Generic

import attr

from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.persistence.entity.state.entities import StateIncarcerationSentence, StateSupervisionSentence, \
    SentenceType


@attr.s
class UsMoSentenceStatus(BuildableAttr):
    # Unique id for this sentence status object
    sentence_status_external_id: str = attr.ib()

    # External id for the sentence associated with this status
    sentence_external_id = attr.ib()

    # Status date
    status_date: Optional[date] = attr.ib()

    # Status code like 15O1000
    status_code: str = attr.ib()

    # Human-readable status code description
    status_description: str = attr.ib()

    # MO DOC id for person associated with this sentence
    person_external_id: str = attr.ib()

    @person_external_id.default
    def _get_person_external_id(self) -> str:
        return self.sentence_external_id.split('-')[0]


@attr.s
class UsMoSentenceMixin(Generic[SentenceType]):
    base_sentence: SentenceType = attr.ib()

    @base_sentence.default
    def _base_sentence(self):
        raise ValueError('Must set base_sentence')

    sentence_statuses: List[UsMoSentenceStatus] = attr.ib()

    @sentence_statuses.default
    def _sentence_statuses(self):
        raise ValueError('Must set sentence_statuses')


@attr.s
class UsMoIncarcerationSentence(StateIncarcerationSentence, UsMoSentenceMixin[StateIncarcerationSentence]):

    @classmethod
    def from_incarceration_sentence(cls,
                                    sentence: StateIncarcerationSentence,
                                    sentence_statuses_raw: List[Dict[str, Any]]) -> 'UsMoIncarcerationSentence':
        sentence_statuses_converted = [UsMoSentenceStatus.build_from_dictionary(status_dict_raw)
                                       for status_dict_raw in sentence_statuses_raw]

        sentence_dict = {
            **sentence.__dict__,
            'base_sentence': sentence,
            'sentence_statuses': sentence_statuses_converted
        }

        return cls(
            **sentence_dict  # type: ignore
        )


@attr.s
class UsMoSupervisionSentence(StateSupervisionSentence, UsMoSentenceMixin[StateSupervisionSentence]):

    @classmethod
    def from_supervision_sentence(cls,
                                  sentence: StateSupervisionSentence,
                                  sentence_statuses_raw: List[Dict[str, Any]]) -> 'UsMoSupervisionSentence':
        sentence_statuses_converted = [UsMoSentenceStatus.build_from_dictionary(status_dict_raw)
                                       for status_dict_raw in sentence_statuses_raw]
        sentence_dict = {
            **sentence.__dict__,
            'base_sentence': sentence,
            'sentence_statuses': sentence_statuses_converted
        }

        return cls(
            **sentence_dict  # type: ignore
        )
