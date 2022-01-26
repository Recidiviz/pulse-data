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
"""Utils for working with NormalizedStateEntity objects."""
import abc
from functools import lru_cache
from typing import List, Type

from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities as state_entities


class EntityNormalizationManager(metaclass=abc.ABCMeta):
    """Base class for a class that manages the normalization of state entities."""

    @staticmethod
    @abc.abstractmethod
    def normalized_entity_classes() -> List[Type[Entity]]:
        """Must be implemented by subclasses. Defines all entities that are
        normalized by this normalization manager."""


def entity_class_can_be_hydrated_in_pipelines(entity_class: Type[Entity]) -> bool:
    """Returns whether the given |entity_class| can be hydrated in a Dataflow
    pipeline. An entity class must have a column with the class id of the
    unifying class (which is currently StatePerson for all pipelines) in order to be
    hydrated in a Dataflow pipeline."""
    schema_class: Type[StateBase] = schema_utils.get_state_database_entity_with_name(
        entity_class.__name__
    )

    # If the class's corresponding table does not have the person_id
    # field then we will never bring an entity of this type into
    # pipelines
    return hasattr(schema_class, state_entities.StatePerson.get_class_id_name())


@lru_cache(maxsize=None)
def _get_entity_class_names_excluded_from_pipelines() -> List[str]:
    """Returns the names of all entity classes that cannot be hydrated in pipelines."""
    return [
        entity_cls.__name__
        for entity_cls in get_all_entity_classes_in_module(state_entities)
        if not entity_class_can_be_hydrated_in_pipelines(entity_cls)
    ]


def get_entity_class_names_excluded_from_normalization() -> List[str]:
    """Returns the names of all entity classes that are never modified by
    normalization.

    We never normalize the StatePerson entity, and classes that are excluded from
    pipelines cannot be normalized.
    """
    return [
        state_entities.StatePerson.__name__
    ] + _get_entity_class_names_excluded_from_pipelines()
