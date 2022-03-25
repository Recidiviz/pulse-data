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
"""Base class for normalizing entities in a normalization calculation pipeline."""
import abc
from typing import Any, Dict, Sequence, Tuple

import attr

from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    AdditionalAttributesMap,
)
from recidiviz.persistence.entity.base_entity import Entity

EntityNormalizerContext = Dict[str, Any]

EntityNormalizerResult = Tuple[Dict[str, Sequence[Entity]], AdditionalAttributesMap]


@attr.s
class BaseEntityNormalizer(abc.ABC):
    """A base class for normalizing entities in a normalization pipeline."""

    @abc.abstractmethod
    def normalize_entities(
        self,
        person_id: int,
        normalizer_args: EntityNormalizerContext,
    ) -> EntityNormalizerResult:
        """Normalizes all entities with corresponding normalization managers.

        Returns a dictionary mapping the entity class name to the list of normalized
        entities, as well as a map of additional attributes that should be persisted
        to the normalized entity tables.
        """
