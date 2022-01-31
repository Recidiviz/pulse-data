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
"""Stores the base EntityNormalizationManager class for the entity normalization
managers."""
import abc
from typing import List, Type

from recidiviz.persistence.entity.base_entity import Entity


class EntityNormalizationManager(metaclass=abc.ABCMeta):
    """Base class for a class that manages the normalization of state entities."""

    @staticmethod
    @abc.abstractmethod
    def normalized_entity_classes() -> List[Type[Entity]]:
        """Must be implemented by subclasses. Defines all entities that are
        normalized by this normalization manager."""
