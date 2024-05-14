# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""
Contains a set of convenience helpers for converting Entity objects to their
corresponding schema Base objects and vice versa.
"""
from types import ModuleType
from typing import Any, List, Sequence, Type, TypeVar

from more_itertools import one

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.operations import schema as operations_schema
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_entity_converter.operations.schema_entity_converter import (
    OperationsEntityToSchemaConverter,
    OperationsSchemaToEntityConverter,
)
from recidiviz.persistence.database.schema_entity_converter.state.schema_entity_converter import (
    StateEntityToSchemaConverter,
    StateSchemaToEntityConverter,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    EntityFieldType,
    SchemaEdgeDirectionChecker,
)
from recidiviz.persistence.entity.operations import entities as operations_entities
from recidiviz.persistence.entity.state import entities as state_entities


def _is_obj_in_module(obj: Any, module: ModuleType) -> bool:
    return obj.__module__ == module.__name__


def convert_entities_to_schema(
    entities: Sequence[Entity], populate_back_edges: bool = True
) -> List[DatabaseEntity]:
    def _is_state_entity(obj: Any) -> bool:
        return _is_obj_in_module(obj, state_entities) and issubclass(
            obj.__class__, Entity
        )

    def _is_operations_entity(obj: Any) -> bool:
        return _is_obj_in_module(obj, operations_entities) and issubclass(
            obj.__class__, Entity
        )

    if all(_is_state_entity(obj) for obj in entities):
        return StateEntityToSchemaConverter().convert_all(entities, populate_back_edges)
    if all(_is_operations_entity(obj) for obj in entities):
        return OperationsEntityToSchemaConverter().convert_all(
            entities, populate_back_edges
        )

    raise ValueError(
        f"Expected all types to belong to the same schema, one of "
        f"[{operations_schema.__name__}] or [{state_schema.__name__}]"
    )


def convert_schema_objects_to_entity(
    schema_objects: Sequence[DatabaseEntity], populate_back_edges: bool = True
) -> List[Entity]:
    def _is_state_schema_object(obj: Any) -> bool:
        return _is_obj_in_module(obj, state_schema) and issubclass(
            obj.__class__, DatabaseEntity
        )

    def _is_operations_schema_object(obj: Any) -> bool:
        return _is_obj_in_module(obj, operations_schema) and issubclass(
            obj.__class__, DatabaseEntity
        )

    if all(_is_state_schema_object(obj) for obj in schema_objects):
        return StateSchemaToEntityConverter().convert_all(
            schema_objects, populate_back_edges
        )
    if all(_is_operations_schema_object(obj) for obj in schema_objects):
        return OperationsSchemaToEntityConverter().convert_all(
            schema_objects, populate_back_edges
        )
    raise ValueError(
        f"Expected all types to belong to the same schema, one of "
        f"[{state_schema.__name__}] or [{operations_schema.__name__}]"
    )


def convert_entity_to_schema_object(entity: Entity) -> DatabaseEntity:
    result_list = convert_entities_to_schema([entity])
    if len(result_list) != 1:
        raise AssertionError(
            "Call to convert object should have only returned one result."
        )
    return result_list[0]


EntityT = TypeVar("EntityT", bound=Entity)


_core_entity_field_index_by_entity_type = {}


def _get_field_index_for_db_entity(db_entity: DatabaseEntity) -> CoreEntityFieldIndex:
    if _is_obj_in_module(db_entity, state_schema):
        if state_entities.__name__ not in _core_entity_field_index_by_entity_type:
            _core_entity_field_index_by_entity_type[
                state_entities.__name__
            ] = CoreEntityFieldIndex(
                direction_checker=SchemaEdgeDirectionChecker.state_direction_checker()
            )
        return _core_entity_field_index_by_entity_type[state_entities.__name__]
    if _is_obj_in_module(db_entity, operations_schema):
        if operations_entities.__name__ not in _core_entity_field_index_by_entity_type:
            _core_entity_field_index_by_entity_type[
                operations_entities.__name__
            ] = CoreEntityFieldIndex(
                direction_checker=SchemaEdgeDirectionChecker.operations_direction_checker()
            )
        return _core_entity_field_index_by_entity_type[operations_entities.__name__]

    raise ValueError(
        f"Expected {type(db_entity)} to belong to either [{state_schema.__name__}] or "
        f"[{operations_schema.__name__}]"
    )


def convert_schema_object_to_entity(
    schema_object: DatabaseEntity,
    entity_type: Type[EntityT],
    populate_back_edges: bool = True,
) -> EntityT:
    conncted_schema_objects = [schema_object]

    if populate_back_edges:
        field_index = _get_field_index_for_db_entity(schema_object)
        for backedge_key in field_index.get_all_core_entity_fields(
            entity_type, EntityFieldType.BACK_EDGE
        ):
            if attribute := getattr(schema_object, backedge_key):
                conncted_schema_objects.append(attribute)

    result_list = convert_schema_objects_to_entity(
        conncted_schema_objects, populate_back_edges
    )

    if len(result_list) != len(conncted_schema_objects):
        raise AssertionError(
            f"Call to convert {len(conncted_schema_objects)} objects returned "
            f"{len(result_list)} objects"
        )

    return one(entity for entity in result_list if isinstance(entity, entity_type))
