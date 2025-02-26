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

from collections import defaultdict
from enum import Enum
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import attr

from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.attr_utils import get_enum_cls, is_enum
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema_entity_converter.schema_to_entity_class_mapper import (
    SchemaToEntityClassMapper,
)
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.core_entity import CoreEntity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    SchemaEdgeDirectionChecker,
    get_all_db_objs_from_tree,
    get_all_entities_from_tree,
)

SrcIdType = int
FieldNameType = str

SrcBaseType = TypeVar("SrcBaseType", DatabaseEntity, Entity)
DstBaseType = TypeVar("DstBaseType", DatabaseEntity, Entity)


class DatabaseConversionError(Exception):
    """Raised if an error is encountered when converting between entity
    objects and schema objects (or vice versa).
    """


class _Direction(Enum):
    SCHEMA_TO_ENTITY = 1
    ENTITY_TO_SCHEMA = 2

    @staticmethod
    def for_cls(src_cls: Type) -> "_Direction":
        if issubclass(src_cls, Entity):
            return _Direction.ENTITY_TO_SCHEMA

        if issubclass(src_cls, DatabaseEntity):
            return _Direction.SCHEMA_TO_ENTITY

        raise DatabaseConversionError(f"Unable to convert class [{src_cls}]")


class BaseSchemaEntityConverter(Generic[SrcBaseType, DstBaseType]):
    """A base abstract class for converting between Entity and schema Base
    objects. For each individual schema, define a subclass which provides the
    entities and schema modules, as well as defines an explicit ordering of
    entity types.
    """

    def __init__(
        self,
        class_mapper: SchemaToEntityClassMapper,
        direction_checker: Optional[SchemaEdgeDirectionChecker],
    ):
        """
        Args:
            direction_checker: A SchemaEdgeDirectionChecker object that is
            specific to the schema required by the subclass. Will be used to
            determine which edges of the graph are back edges. May be null if there
            are no edges between nodes in the graph.
        """
        self._class_mapper = class_mapper
        self._direction_checker = direction_checker

        # Cache of src object id to corresponding converted object
        self._converted_map: Dict[SrcIdType, DstBaseType] = {}

        # For each src object id in the src graph, holds a map of fields to
        # lists of src type object ids that correspond to edges that traverse
        # backwards along the object graph ordering, and need to be filled in
        # once all the forward edges have been traversed.
        self._back_edges: Dict[
            SrcIdType, Dict[FieldNameType, List[SrcIdType]]
        ] = defaultdict(lambda: defaultdict(list))
        self.field_index = CoreEntityFieldIndex()

    def _populate_root_entity_back_edges(self, dst: DstBaseType) -> None:
        """If |dst| is a root entity, populates all root entity back edges on entities
        connected to |dst|. Back edges to non-root-entity parents are populated by
        _pouplate_direct_back_edge
        """
        root_entity_cls = self._get_entity_class(dst)
        if not issubclass(root_entity_cls, RootEntity):
            return

        back_edge_field_name = root_entity_cls.back_edge_field_name()
        if isinstance(dst, DatabaseEntity):
            all_entities: Sequence[CoreEntity] = list(
                get_all_db_objs_from_tree(dst, self.field_index)
            )
        else:
            all_entities = get_all_entities_from_tree(dst, self.field_index)

        for child_dst in all_entities:
            if hasattr(child_dst, back_edge_field_name):
                setattr(child_dst, back_edge_field_name, dst)

    @staticmethod
    def _id_from_src_object(src: SrcBaseType) -> SrcIdType:
        return id(src)

    def _register_back_edge(
        self, from_src_obj: SrcBaseType, to_src_obj: SrcBaseType, field: FieldNameType
    ) -> None:
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

    def _check_back_edges_empty(self) -> None:
        """
        Raises an assertion if there are any back edges that have yet to be
        filled in.
        """
        if self._back_edges.keys():
            key = next(iter(self._back_edges.keys()))
            raise DatabaseConversionError(
                f"Found back edges that have yet to be filled in for "
                f"[{len(self._back_edges.keys())}] items. Should have been 0."
                f"First unfilled edge: {key}: {self._back_edges[key]}"
            )

    def convert_all(
        self, src: Sequence[SrcBaseType], populate_back_edges: bool = True
    ) -> List[DstBaseType]:
        """Converts the given list of objects into their entity/schema
        counterparts.

        Args:
            src: list of schema objects or entity objects
            populate_back_edges: Whether or not back edges should be
                populated during conversion
        Returns:
            The converted list, a schema or entity list.
        """
        result = [self._convert(s, populate_back_edges) for s in src]
        if populate_back_edges:
            self._check_back_edges_empty()
        return result

    def convert(
        self, src: SrcBaseType, populate_back_edges: bool = True
    ) -> DstBaseType:
        """Converts the given object into its entity/schema counterpart.

        Args:
            src: a schema object or entity object
            populate_back_edges: Whether or not back edges should be
                populated during conversion
        Returns:
            The converted object, a schema or entity object.
        """
        result = self._convert(src, populate_back_edges)
        if populate_back_edges:
            self._check_back_edges_empty()
        return result

    def _convert(self, src: SrcBaseType, populate_back_edges: bool) -> DstBaseType:
        dst = self._convert_forward(src, populate_back_edges)
        if populate_back_edges:
            self._populate_direct_back_edges()
            self._populate_root_entity_back_edges(dst)
        return dst

    def _convert_forward(
        self, src: SrcBaseType, populate_back_edges: bool
    ) -> DstBaseType:
        """Converts the given src object to its entity/schema counterpart."""

        src_id = self._id_from_src_object(src)
        if src_id in self._converted_map:
            return self._converted_map[src_id]

        schema_cls: Type[DatabaseEntity] = self._get_schema_class(src)
        entity_cls: Type[Entity] = self._get_entity_class(src)

        if entity_cls is None or schema_cls is None:
            raise DatabaseConversionError(
                "Both |entity_cls| and |schema_cls| should be not None"
            )

        if isinstance(src, Entity):
            dst_builder: Union[BuildableAttr.Builder, DatabaseEntity] = schema_cls()
        elif isinstance(src, DatabaseEntity):
            if not issubclass(entity_cls, BuildableAttr):
                raise DatabaseConversionError(
                    f"Expected [{entity_cls}] to be a subclass of "
                    f"BuildableAttr, but it is not"
                )

            dst_builder = entity_cls.builder()
        else:
            raise DatabaseConversionError(f"Unable to convert class [{type(src)}]")

        for field, attribute in attr.fields_dict(entity_cls).items():  # type: ignore[arg-type]
            v = getattr(src, field)

            if not isinstance(attribute, attr.Attribute):
                raise DatabaseConversionError(
                    f"Expected attribute with class [{attribute.__class__}] to "
                    f"be an instance of Attribute, but it is not"
                )

            if isinstance(v, list):
                if not self._direction_checker:
                    raise ValueError(
                        f"Found null direction checker for entity [{entity_cls}] with "
                        f"relationship field [{field}]"
                    )
                is_backedge = self._direction_checker.is_back_edge(type(src), field)
                if is_backedge and not populate_back_edges:
                    continue
                values = []
                for next_src in v:
                    if is_backedge:
                        self._register_back_edge(src, next_src, field)
                        continue
                    values.append(self._convert_forward(next_src, populate_back_edges))

                if not values:
                    continue

                value: Optional[Any] = values
            elif issubclass(type(v), Entity) or issubclass(type(v), DatabaseEntity):
                if not self._direction_checker:
                    raise ValueError(
                        f"Found null direction checker for entity [{entity_cls}] with "
                        f"relationship field [{field}]"
                    )
                is_backedge = self._direction_checker.is_back_edge(type(src), field)
                if is_backedge and not populate_back_edges:
                    continue
                next_src = v
                if is_backedge:
                    self._register_back_edge(src, next_src, field)
                    continue
                value = self._convert_forward(v, populate_back_edges)
            elif v is None:
                value = None
            elif is_enum(attribute):
                value = self._convert_enum(src, field, attribute)
            else:
                value = v

            setattr(dst_builder, field, value)

        if isinstance(dst_builder, BuildableAttr.Builder):
            dst = dst_builder.build()
        elif isinstance(dst_builder, DatabaseEntity):
            dst = dst_builder
        else:
            raise DatabaseConversionError(
                f"Unexpected type [{type(dst_builder)}] for dst_builder"
            )

        self._converted_map[src_id] = dst

        return dst

    def _lookup_edges(
        self, next_src_ids: List[SrcIdType]
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

    def _populate_direct_back_edges(self) -> None:
        """Fills direct parent back edges that have been identified during
        conversion for any objects that have been properly created. Back edges
        which point to entities which are not direct parents (but some more
        distant ancestor) are not included here.
        """

        not_found_back_edges: Dict[
            SrcIdType, Dict[FieldNameType, List[SrcIdType]]
        ] = defaultdict(lambda: defaultdict(list))

        for src_id, back_edges_map in self._back_edges.items():
            dst_object = self._converted_map[src_id]
            for field, next_src_ids in back_edges_map.items():
                next_dst_objects, not_found_next_src_ids = self._lookup_edges(
                    next_src_ids
                )

                if len(next_dst_objects) + len(not_found_next_src_ids) != len(
                    next_src_ids
                ):
                    raise DatabaseConversionError(
                        f"Expected to find {len(next_src_ids)} "
                        f"next_dst_objects or not_found_next_src_ids, instead "
                        f"found {len(next_dst_objects)} objects and "
                        f"{len(not_found_next_src_ids)} not found ids."
                    )

                v = getattr(dst_object, field)
                if isinstance(v, list):
                    existing = {id(obj) for obj in v}

                    v.extend(
                        [obj for obj in next_dst_objects if id(obj) not in existing]
                    )
                else:
                    if len(next_src_ids) > 1:
                        raise DatabaseConversionError(
                            f"Found [{len(next_src_ids)}] edges for non-list "
                            f"field [{field}] on object with class name "
                            f"{dst_object.__class__.__name__}"
                        )

                    if len(next_dst_objects) == 1:
                        next_dst_object = next_dst_objects[0]
                        setattr(dst_object, field, next_dst_object)

                        # If the dst_object stores a foreign key reference of the
                        # primary key of the next_dst_object, then set that value.
                        # Note: this will be a no-op in ingest when DB ids are not
                        # yet set.
                        if hasattr(dst_object, next_dst_object.get_class_id_name()):
                            setattr(
                                dst_object,
                                next_dst_object.get_class_id_name(),
                                next_dst_object.get_id(),
                            )

                for next_src_id in not_found_next_src_ids:
                    not_found_back_edges[src_id][field].append(next_src_id)

        self._back_edges = not_found_back_edges

    def _get_entity_class(self, obj: Union[SrcBaseType, DstBaseType]) -> Type[Entity]:
        if isinstance(obj, Entity):
            return obj.__class__
        return self._class_mapper.entity_cls_for_schema_cls(obj.__class__)

    def _get_schema_class(self, src: SrcBaseType) -> Type[DatabaseEntity]:
        if isinstance(src, DatabaseEntity):
            return src.__class__
        return self._class_mapper.schema_cls_for_entity_cls(src.__class__)

    @staticmethod
    def _convert_enum(src: SrcBaseType, field: str, attribute: attr.Attribute) -> Enum:
        if isinstance(src, DatabaseEntity):
            enum_cls = get_enum_cls(attribute)
            if enum_cls is None:
                raise ValueError(f"Could not retrieve enum class for {attribute}")
            return enum_cls(getattr(src, field))

        return getattr(src, field).value
