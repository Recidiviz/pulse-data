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
Defines a base abstract class for converting between Entity and schema Base
objects.
"""

import abc
from collections import defaultdict
from enum import Enum
from types import ModuleType
from typing import Generic, Dict, List, Type, Optional, Any, Tuple, Sequence, \
    TypeVar

import attr

from recidiviz.common.attr_utils import is_enum, get_enum_cls
from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.persistence.database.base_schema import Base
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import SchemaEdgeDirectionChecker

SrcIdType = int
FieldNameType = str

SrcBaseType = TypeVar('SrcBaseType', Base, Entity)
DstBaseType = TypeVar('DstBaseType', Base, Entity)


class DatabaseConversionError(Exception):
    """Raised if an error is encountered when converting between entity
    objects and schema objects (or vice versa).
    """


class _Direction(Enum):
    SCHEMA_TO_ENTITY = 1
    ENTITY_TO_SCHEMA = 2

    @staticmethod
    def for_cls(src_cls):
        if issubclass(src_cls, Entity):
            return _Direction.ENTITY_TO_SCHEMA

        if issubclass(src_cls, Base):
            return _Direction.SCHEMA_TO_ENTITY

        raise DatabaseConversionError(
            "Unable to convert class [{0}]".format(src_cls))


class BaseSchemaEntityConverter(Generic[SrcBaseType, DstBaseType]):
    """A base abstract class for converting between Entity and schema Base
    objects. For each individual schema, define a subclass which provides the
    entities and schema modules, as well as defines an explicit ordering of
    entity types.
    """

    def __init__(self, direction_checker: SchemaEdgeDirectionChecker):
        """
        Args:
            direction_checker: A SchemaEdgeDirectionChecker object that is
            specific to the schema required by the subclass. Will be used to
            determine which edges of the graph are back edges.
        """
        self._direction_checker = direction_checker

        # Cache of src object id to corresponding converted object
        self._converted_map: Dict[SrcIdType, DstBaseType] = {}

        # For each src object id in the src graph, holds a map of fields to
        # lists of src type object ids that correspond to edges that traverse
        # backwards along the object graph ordering, and need to be filled in
        # once all the forward edges have been traversed.
        self._back_edges: Dict[SrcIdType,
                               Dict[FieldNameType, List[SrcIdType]]] = \
            defaultdict(lambda: defaultdict(list))

    @abc.abstractmethod
    def _get_entities_module(self) -> ModuleType:
        pass

    @abc.abstractmethod
    def _get_schema_module(self) -> ModuleType:
        pass

    @abc.abstractmethod
    def _should_skip_field(self, field: FieldNameType) -> bool:
        pass

    @staticmethod
    def _id_from_src_object(src: SrcBaseType) -> SrcIdType:
        return id(src)

    def _register_back_edge(self,
                            from_src_obj: SrcBaseType,
                            to_src_obj: SrcBaseType,
                            field: FieldNameType) -> None:
        """
        Records an edge on the source object graph that is a back edge, i.e. it
        travels in a direction opposite to the class ranking. This edge will be
        filled out later once all the forward edges have been completed.

        Args:
            from_src_obj: An object that is the origin of this edge
            to_src_obj: An object that is the destination of this edge
            field: Field name on from_src_obj that from_src_obj belongs to.
        """
        from_id = self._id_from_src_object(from_src_obj)
        to_id = self._id_from_src_object(to_src_obj)
        self._back_edges[from_id][field].append(to_id)

    def _check_back_edges_empty(self):
        """
        Raises an assertion if there are any back edges that have yet to be
        filled in.
        """
        if self._back_edges.keys():
            key = next(iter(self._back_edges.keys()))
            raise DatabaseConversionError(
                f"Found back edges that have yet to be filled in for "
                f"[{len(self._back_edges.keys())}] items. Should have been 0."
                f"First unfilled edge: {key}: {self._back_edges[key]}")

    def convert_all(self, src: Sequence[SrcBaseType]) -> List[DstBaseType]:
        """Converts the given list of objects into their entity/schema
        counterparts.

        Args:
            src: list of schema objects or entity objects
        Returns:
            The converted list, a schema or entity list.
        """
        result = [self._convert(s) for s in src]
        self._check_back_edges_empty()
        return result

    def convert(self, src: SrcBaseType) -> DstBaseType:
        """Converts the given object into its entity/schema counterpart.

        Args:
            src: a schema object or entity object
        Returns:
            The converted object, a schema or entity object.
        """
        result = self._convert(src)
        self._check_back_edges_empty()
        return result

    def _convert(self, src: SrcBaseType) -> DstBaseType:
        dst = self._convert_forward(src)
        self._fill_back_edges()
        return dst

    def _convert_forward(self, src: SrcBaseType) -> DstBaseType:
        """Converts the given src object to its entity/schema counterpart."""

        src_id = self._id_from_src_object(src)
        if src_id in self._converted_map:
            return self._converted_map[src_id]

        direction = _Direction.for_cls(src.__class__)

        schema_cls = self._get_schema_class(src)
        entity_cls = self._get_entity_class(src)

        if entity_cls is None or schema_cls is None:
            raise DatabaseConversionError("Both |entity_cls| and |schema_cls| "
                                          "should be not None")

        if direction is _Direction.ENTITY_TO_SCHEMA:
            dst = schema_cls()
        else:
            if not issubclass(entity_cls, BuildableAttr):
                raise DatabaseConversionError(
                    f"Expected [{entity_cls}] to be a subclass of "
                    f"BuildableAttr, but it is not")

            dst = entity_cls.builder()

        for field, attribute in attr.fields_dict(entity_cls).items():
            if self._should_skip_field(field):
                continue

            v = getattr(src, field)

            if not isinstance(attribute, attr.Attribute):
                raise DatabaseConversionError(
                    f"Expected attribute with class [{attribute.__class__}] to "
                    f"be an instance of Attribute, but it is not")

            if isinstance(v, list):
                values = []
                for next_src in v:
                    if self._direction_checker.is_back_edge(src, next_src):
                        self._register_back_edge(src, next_src, field)
                        continue
                    values.append(self._convert_forward(next_src))

                if not values:
                    continue

                value: Optional[Any] = values
            elif issubclass(type(v), Entity) or issubclass(type(v), Base):
                next_src = v
                if self._direction_checker.is_back_edge(src, next_src):
                    self._register_back_edge(src, next_src, field)
                    continue
                value = self._convert_forward(v)
            elif v is None:
                value = None
            elif is_enum(attribute):
                value = self._convert_enum(src,
                                           field,
                                           attribute,
                                           direction)
            else:
                value = v

            setattr(dst, field, value)

        if direction is _Direction.SCHEMA_TO_ENTITY:
            dst = dst.build()

        self._converted_map[src_id] = dst

        return dst

    def _lookup_edges(
            self,
            next_src_ids: List[SrcIdType]
    ) -> Tuple[List[DstBaseType], List[SrcIdType]]:
        """Look up objects in the destination object graph corresponding to the
        provided list of object ids from the source object graph.

        Args:
            next_src_ids: A list of ids to search for in our map of converted
                objects

        Returns:
            A tuple where the first value is the list of converted objects we
                found corresponding to those objects and the second value is a
                list of ids that have not yet been converted.
        """

        next_dst_objects: List[DstBaseType] = []
        not_found_next_src_ids: List[SrcIdType] = []
        for next_src_id in next_src_ids:
            if next_src_id in self._converted_map:
                next_dst_objects.append(self._converted_map[next_src_id])
            else:
                not_found_next_src_ids.append(next_src_id)

        return next_dst_objects, not_found_next_src_ids

    def _fill_back_edges(self):
        """Fills back edges that have been identified during conversion for any
        objects that have been properly created.
        """

        not_found_back_edges: \
            Dict[SrcIdType, Dict[FieldNameType, List[SrcIdType]]] = \
            defaultdict(lambda: defaultdict(list))

        for src_id, back_edges_map in self._back_edges.items():
            dst_object = self._converted_map[src_id]
            for field, next_src_ids in back_edges_map.items():
                next_dst_objects, not_found_next_src_ids = \
                    self._lookup_edges(next_src_ids)

                if len(next_dst_objects) + len(not_found_next_src_ids) != \
                        len(next_src_ids):
                    raise DatabaseConversionError(
                        f'Expected to find {len(next_src_ids)} '
                        f'next_dst_objects or not_found_next_src_ids, instead '
                        f'found {len(next_dst_objects)} objects and '
                        f'{len(not_found_next_src_ids)} not found ids.')

                v = getattr(dst_object, field)
                if isinstance(v, list):
                    existing = {id(obj) for obj in v}

                    v.extend([obj for obj in next_dst_objects
                              if id(obj) not in existing])
                else:
                    if len(next_src_ids) > 1:
                        raise DatabaseConversionError(
                            f"Found [{len(next_src_ids)}] edges for non-list "
                            f"field [{field}] on object with class name "
                            f"{dst_object.__class__.__name__}")

                    if len(next_dst_objects) == 1:
                        next_dst_object = next_dst_objects[0]
                        setattr(dst_object, field, next_dst_object)

                for next_src_id in not_found_next_src_ids:
                    not_found_back_edges[src_id][field].append(next_src_id)

        self._back_edges = not_found_back_edges

    def _check_is_valid_src_module(self, src: SrcBaseType):
        if src.__module__ not in [self._get_schema_module().__name__,
                                  self._get_entities_module().__name__]:
            raise DatabaseConversionError(
                f"Attempting to convert class with unexpected"
                f" module: [{src.__module__}]")

    def _get_entity_class(self, src: SrcBaseType) -> Type[Entity]:
        self._check_is_valid_src_module(src)
        return getattr(self._get_entities_module(), src.__class__.__name__)

    def _get_schema_class(self, src: SrcBaseType) -> Type[Base]:
        self._check_is_valid_src_module(src)
        return getattr(self._get_schema_module(), src.__class__.__name__)

    @staticmethod
    def _convert_enum(src, field, attribute, direction):
        if direction is _Direction.SCHEMA_TO_ENTITY:
            enum_cls = get_enum_cls(attribute)
            return enum_cls(getattr(src, field))

        return getattr(src, field).value
