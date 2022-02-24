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
from typing import Any, Dict, Sequence

import attr

from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities import (
    NormalizedStateEntity,
)

EntityNormalizerContext = Dict[str, Any]


@attr.s
class BaseEntityNormalizer(abc.ABC):
    """A base class for normalizing entities in a normalization pipeline."""

    @abc.abstractmethod
    def normalize_entities(
        self,
        normalizer_args: EntityNormalizerContext,
    ) -> Dict[str, Sequence[NormalizedStateEntity]]:
        """Define the function to normalize entities."""
