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
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateCharge,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    AdditionalAttributesMap,
    get_shared_additional_attributes_map_for_entities,
    merge_additional_attributes_maps,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entity_conversion_utils import (
    fields_unique_to_normalized_class,
)
from recidiviz.calculator.pipeline.utils.execution_utils import (
    list_of_dicts_to_dict_with_keys,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.common.attr_mixins import (
    BuildableAttrFieldType,
    attr_field_type_for_field_name,
)
from recidiviz.common.ncic import get_description
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

    def additional_attributes_map_for_normalized_incarceration_sentences(
        self,
        incarceration_sentences: List[StateIncarcerationSentence],
    ) -> AdditionalAttributesMap:
        """Returns the attributes that should be set on the normalized version of each of
        the StateIncarcerationSentences for each of the attributes that are unique to the
        NormalizedStateIncarcerationSentence."""

        shared_attributes = get_shared_additional_attributes_map_for_entities(
            entities=incarceration_sentences
        )

        charges = [
            charge
            for incarceration_sentence in incarceration_sentences
            for charge in incarceration_sentence.charges
        ]

        return merge_additional_attributes_maps(
            [
                shared_attributes,
                self.additional_attributes_map_for_normalized_charges(charges),
            ]
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

    def additional_attributes_map_for_normalized_supervision_sentences(
        self,
        supervision_sentences: List[StateSupervisionSentence],
    ) -> AdditionalAttributesMap:
        """Returns the attributes that should be set on the normalized version of each of
        the StateSupervisionSentences for each of the attributes that are unique to the
        NormalizedStateSupervisionSentence."""

        shared_attributes = get_shared_additional_attributes_map_for_entities(
            entities=supervision_sentences
        )

        charges = [
            charge
            for supervision_sentence in supervision_sentences
            for charge in supervision_sentence.charges
        ]

        return merge_additional_attributes_maps(
            [
                shared_attributes,
                self.additional_attributes_map_for_normalized_charges(charges),
            ]
        )

    def additional_attributes_map_for_normalized_charges(
        self, charges: List[StateCharge]
    ) -> AdditionalAttributesMap:
        shared_additional_attributes = (
            get_shared_additional_attributes_map_for_entities(entities=charges)
        )

        charges_additional_attributes_map: Dict[int, Dict[str, Any]] = {}

        for charge in charges:
            if not charge.charge_id:
                raise ValueError(f"Unexpected charge with no charge_id {charge}")
            charges_additional_attributes_map[
                charge.charge_id
            ] = self._get_extra_metadata_for_charge(charge)

        return merge_additional_attributes_maps(
            [
                shared_additional_attributes,
                {StateCharge.__name__: charges_additional_attributes_map},
            ]
        )

    def _get_extra_metadata_for_charge(self, charge: StateCharge) -> Dict[str, Any]:
        """Adds extra metadata for the StateCharge entity."""
        if not charge.charge_id:
            raise ValueError(f"Unexpected charge with no charge_id {charge}")

        offense_labels_for_charge = (
            self._charge_offense_description_to_labels.get(charge.charge_id) or {}
        )

        # Fill the external values
        external_values = {
            "ncic_code_external": charge.ncic_code,
            "ncic_category_external": get_description(charge.ncic_code)
            if charge.ncic_code
            else None,
            "description_external": charge.description,
            "is_violent_external": charge.is_violent,
            "is_drug_external": charge.is_drug,
            "is_sex_offense_external": charge.is_sex_offense,
        }

        uniform_fields = [
            field
            for field in fields_unique_to_normalized_class(NormalizedStateCharge)
            if field not in external_values
        ]

        # Fields for the charge
        uniform_values = {
            field: offense_labels_for_charge.get(
                field.replace("_uniform", ""), "EXTERNAL_UNKNOWN"
            )
            if attr_field_type_for_field_name(NormalizedStateCharge, field)
            == BuildableAttrFieldType.STRING
            else offense_labels_for_charge.get(field.replace("_uniform", ""), None)
            for field in uniform_fields
        }

        return {**external_values, **uniform_values}
