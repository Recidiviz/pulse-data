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
from typing import List, Any, Sequence

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.schema_person_type import \
    SchemaPersonType
from recidiviz.persistence.database.schema_entity_converter.county.\
    schema_entity_converter import (
        CountyEntityToSchemaConverter,
        CountySchemaToEntityConverter,
    )
from recidiviz.persistence.database.schema_entity_converter.operations.schema_entity_converter import \
    OperationsSchemaToEntityConverter, OperationsEntityToSchemaConverter
from recidiviz.persistence.database.schema_entity_converter.state.\
    schema_entity_converter import (
        StateEntityToSchemaConverter,
        StateSchemaToEntityConverter,
    )
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.persistence.entity.operations import entities as operations_entities
from recidiviz.persistence.database.schema.operations import schema as operations_schema
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.entity.entities import EntityPersonType
from recidiviz.persistence.entity.state import entities as state_entities


def _is_obj_in_module(obj: Any, module: ModuleType) -> bool:
    return obj.__module__ == module.__name__


def convert_entity_people_to_schema_people(
        people: List[EntityPersonType],
        populate_back_edges: bool = True) -> List[SchemaPersonType]:

    def _as_schema_person_type(e: DatabaseEntity) -> SchemaPersonType:
        if not isinstance(e, county_schema.Person) and \
                not isinstance(e, state_schema.StatePerson):
            raise ValueError(f"Unexpected database entity type: [{type(e)}]")
        return e

    return [_as_schema_person_type(p)
            for p in convert_entities_to_schema(people, populate_back_edges)]


def convert_entities_to_schema(
        entities: Sequence[Entity],
        populate_back_edges: bool = True) -> List[DatabaseEntity]:
    def _is_county_entity(obj: Any) -> bool:
        return _is_obj_in_module(obj, county_entities) and \
            issubclass(obj.__class__, Entity)

    def _is_state_entity(obj: Any) -> bool:
        return _is_obj_in_module(obj, state_entities) and \
            issubclass(obj.__class__, Entity)

    def _is_operations_entity(obj: Any) -> bool:
        return _is_obj_in_module(obj, operations_entities) and \
            issubclass(obj.__class__, Entity)

    if all(_is_county_entity(obj) for obj in entities):
        return list(CountyEntityToSchemaConverter().convert_all(
            entities, populate_back_edges))
    if all(_is_state_entity(obj) for obj in entities):
        return StateEntityToSchemaConverter().convert_all(
            entities, populate_back_edges)
    if all(_is_operations_entity(obj) for obj in entities):
        return OperationsEntityToSchemaConverter().convert_all(
            entities, populate_back_edges)

    raise ValueError(f"Expected all types to belong to the same schema, one of "
                     f"[{county_schema.__name__}] or [{state_schema.__name__}]")


def convert_schema_objects_to_entity(
        schema_objects: Sequence[DatabaseEntity],
        populate_back_edges: bool = True) -> List[Entity]:
    def _is_county_schema_object(obj: Any) -> bool:
        return _is_obj_in_module(obj, county_schema) and \
            issubclass(obj.__class__, DatabaseEntity)

    def _is_state_schema_object(obj: Any) -> bool:
        return _is_obj_in_module(obj, state_schema) and \
            issubclass(obj.__class__, DatabaseEntity)

    def _is_operations_schema_object(obj: Any) -> bool:
        return _is_obj_in_module(obj, operations_schema) and \
            issubclass(obj.__class__, DatabaseEntity)

    if all(_is_county_schema_object(obj) for obj in schema_objects):
        return CountySchemaToEntityConverter().convert_all(schema_objects)
    if all(_is_state_schema_object(obj) for obj in schema_objects):
        return StateSchemaToEntityConverter().convert_all(
            schema_objects, populate_back_edges)
    if all(_is_operations_schema_object(obj) for obj in schema_objects):
        return OperationsSchemaToEntityConverter().convert_all(
            schema_objects, populate_back_edges)
    raise ValueError(f"Expected all types to belong to the same schema, one of "
                     f"[{county_schema.__name__}], [{state_schema.__name__}], or [{operations_schema.__name__}]")


def convert_entity_to_schema_object(entity: Entity) -> DatabaseEntity:
    result_list = convert_entities_to_schema([entity])
    if len(result_list) != 1:
        raise AssertionError("Call to convert object should have only returned one result.")
    return result_list[0]


def convert_schema_object_to_entity(schema_object: DatabaseEntity,
                                    populate_back_edges: bool = True) -> Entity:
    result_list = convert_schema_objects_to_entity(
        [schema_object], populate_back_edges)
    if len(result_list) != 1:
        raise AssertionError("Call to convert object should have only returned one result.")
    return result_list[0]
