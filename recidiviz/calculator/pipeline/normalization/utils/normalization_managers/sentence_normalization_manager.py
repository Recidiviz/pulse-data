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
"""Contains the logic for a SentenceNormalizationManager that manages the normalization
of StateCharge entities in the calculation pipelines."""
from typing import Any, Dict, List, Optional, Tuple, Type

from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    AdditionalAttributesMap,
    get_shared_additional_attributes_map_for_entities,
)
from recidiviz.calculator.pipeline.utils.execution_utils import (
    list_of_dicts_to_dict_with_keys,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import (
    StateCharge,
    StateEarlyDischarge,
    StateIncarcerationSentence,
    StateSupervisionSentence,
)


# pylint: disable=unused-argument
class StateSpecificSentenceNormalizationDelegate(StateSpecificDelegate):
    """Interface for state-specific decisions involved in normalizing sentences
    for calculations."""

    def update_incarceration_sentence(
        self, incarceration_sentence: StateIncarcerationSentence
    ) -> StateIncarcerationSentence:
        """Contains state-specific logic for updating certain fields on incarceration sentences.

        By default, returns the incarceration sentence itself."""
        return incarceration_sentence

    def update_supervision_sentence(
        self, supervision_sentence: StateSupervisionSentence
    ) -> StateSupervisionSentence:
        """Contains state-specific logic for updating certain fields on supervision sentences.

        By default, returns the supervision sentence itself."""
        return supervision_sentence


class SentenceNormalizationManager(EntityNormalizationManager):
    """Interface for generalized and state-specific normalization of StateCharges
    for use in calculations."""

    def __init__(
        self,
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        charge_offense_description_to_labels_list: List[Dict[str, Any]],
        delegate: StateSpecificSentenceNormalizationDelegate,
    ) -> None:
        self._incarceration_sentences = incarceration_sentences
        self._supervision_sentences = supervision_sentences
        self._charge_offense_description_to_labels: Dict[
            int, Dict[str, Any]
        ] = list_of_dicts_to_dict_with_keys(
            charge_offense_description_to_labels_list, key="charge_id"
        )
        self._normalized_incarceration_sentences_and_additional_attributes: Optional[
            Tuple[List[StateIncarcerationSentence], AdditionalAttributesMap]
        ] = None
        self._normalized_supervision_sentences_and_additional_attributes: Optional[
            Tuple[List[StateSupervisionSentence], AdditionalAttributesMap]
        ] = None

        self.delegate = delegate

    @staticmethod
    def normalized_entity_classes() -> List[Type[Entity]]:
        return [
            StateCharge,
            StateSupervisionSentence,
            StateIncarcerationSentence,
            StateEarlyDischarge,
        ]

    @staticmethod
    def normalized_entity_associations() -> List[Tuple[Type[Entity], Type[Entity]]]:
        return [
            (StateCharge, StateSupervisionSentence),
            (StateCharge, StateIncarcerationSentence),
        ]

    def normalized_incarceration_sentences_and_additional_attributes(
        self,
    ) -> Tuple[List[StateIncarcerationSentence], AdditionalAttributesMap]:
        """Performs normalization on incarceration sentences."""

        if not self._normalized_incarceration_sentences_and_additional_attributes:
            incarceration_sentences_for_normalization = [
                self.delegate.update_incarceration_sentence(incarceration_sentence)
                for incarceration_sentence in self._incarceration_sentences
            ]
            self._normalized_incarceration_sentences_and_additional_attributes = (
                incarceration_sentences_for_normalization,
                self.additional_attributes_map_for_normalized_incarceration_sentences(
                    incarceration_sentences_for_normalization
                ),
            )
        return self._normalized_incarceration_sentences_and_additional_attributes

    @classmethod
    def additional_attributes_map_for_normalized_incarceration_sentences(
        cls,
        incarceration_sentences: List[StateIncarcerationSentence],
    ) -> AdditionalAttributesMap:
        """Returns the attributes that should be set on the normalized version of each of
        the StateIncarcerationSentences for each of the attributes that are unique to the
        NormalizedStateIncarcerationSentence."""

        return get_shared_additional_attributes_map_for_entities(
            entities=incarceration_sentences
        )

    def normalized_supervision_sentences_and_additional_attributes(
        self,
    ) -> Tuple[List[StateSupervisionSentence], AdditionalAttributesMap]:
        """Performs normalization on supervision sentences."""

        if not self._normalized_supervision_sentences_and_additional_attributes:
            supervision_sentences_for_normalization = [
                self.delegate.update_supervision_sentence(supervision_sentence)
                for supervision_sentence in self._supervision_sentences
            ]
            self._normalized_supervision_sentences_and_additional_attributes = (
                supervision_sentences_for_normalization,
                self.additional_attributes_map_for_normalized_supervision_sentences(
                    supervision_sentences_for_normalization
                ),
            )
        return self._normalized_supervision_sentences_and_additional_attributes

    @classmethod
    def additional_attributes_map_for_normalized_supervision_sentences(
        cls,
        supervision_sentences: List[StateSupervisionSentence],
    ) -> AdditionalAttributesMap:
        """Returns the attributes that should be set on the normalized version of each of
        the StateSupervisionSentences for each of the attributes that are unique to the
        NormalizedStateSupervisionSentence."""

        return get_shared_additional_attributes_map_for_entities(
            entities=supervision_sentences
        )
