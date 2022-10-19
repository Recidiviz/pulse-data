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
from typing import List, Tuple, Type

from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import (
    StateCharge,
    StateEarlyDischarge,
    StateIncarcerationSentence,
    StateSupervisionSentence,
)


class SentenceNormalizationManager(EntityNormalizationManager):
    """Interface for generalized and state-specific normalization of StateCharges
    for use in calculations."""

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
