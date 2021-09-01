# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Defines a collection of objects that can collectively be used to build an abstract
syntax tree that can be used to convert an ingest view row into a hydrated entity
tree.
"""

import abc
import re
from typing import Dict, Generic, List, Type, TypeVar, Union

import attr

from recidiviz.common.constants.enum_parser import EnumParser
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_deserialize import EntityFactory, EntityT

ManifestNodeT = TypeVar("ManifestNodeT")


@attr.s(kw_only=True)
class ManifestNode(Generic[ManifestNodeT]):
    """Abstract interface for all nodes in the manifest abstract syntax tree. Subclasses
    may be leaf nodes (e.g. represent flat fields) or subtree root nodes (e.g. represent
    entity relationships).
    """

    @abc.abstractmethod
    def build_from_row(self, row: Dict[str, str]) -> ManifestNodeT:
        """Should be implemented by subclasses to return a recursively hydrated node
        in the entity tree, parsed out of the input row.
        """


@attr.s(kw_only=True)
class EntityTreeManifest(ManifestNode[EntityT]):
    """An abstract syntax tree describing how to convert an input ingest view row into
    all or part of the output entity tree.
    """

    # The class we will recursively hydrate from this entity manifest.
    entity_cls: Type[EntityT] = attr.ib()

    # The factory class for converting a collection of arguments into an instance of
    # the above class.
    entity_factory_cls: Type[EntityFactory[EntityT]] = attr.ib()

    # A collection of manifests for fields that should be recursively hydrated into
    # sub-trees. These may either be manifests for flat fields or relationship fields.
    field_manifests: Dict[str, ManifestNode] = attr.ib()

    # A map of arguments that should be applied to all parsed entities.
    common_args: Dict[str, Union[str, EnumParser]] = attr.ib()

    def build_from_row(self, row: Dict[str, str]) -> EntityT:
        """Builds a recursively hydrated entity from the given input row."""
        args = self.common_args.copy()

        for field_name, field_manifest in self.field_manifests.items():
            args[field_name] = field_manifest.build_from_row(row)

        entity = self.entity_factory_cls.deserialize(**args)

        if not isinstance(entity, self.entity_cls):
            raise ValueError(f"Unexpected type for entity: [{type(entity)}]")
        return entity


@attr.s(kw_only=True)
class ExpandableListItemManifest:
    """A wrapper around an EntityTreeManifest that describes a list item that can be
    expanded into 0 to N entity trees, based on the value of the input column.
    """

    # Key that denotes that a list item should be treated as an expandable list item.
    FOREACH_ITERATOR_KEY = "$foreach"

    # Variable "column name" hydrated with a single list item value. Can only be used
    # within the context of a $foreach loop.
    FOREACH_LOOP_VALUE_NAME = "$iter"

    # Default delimiter used to split list column values.
    DEFAULT_LIST_VALUE_DELIMITER = ","

    # Name of the column that should be treated as a list of values to expand into list
    # items.
    mapped_column: str = attr.ib()

    child_entity_manifest: EntityTreeManifest = attr.ib()

    def expand(self, row: Dict[str, str]) -> List[Entity]:
        column_value = row[self.mapped_column]
        if not column_value:
            return []

        # TODO(#8908): For now, we always split list column values on the default
        #  delimiter. Revisit whether the parser language needs be changed to allow the
        #  delimiter to be configurable.
        values = column_value.split(self.DEFAULT_LIST_VALUE_DELIMITER)
        result = []
        if self.FOREACH_LOOP_VALUE_NAME in row:
            raise ValueError(
                f"Unexpected {self.FOREACH_LOOP_VALUE_NAME} key value in row: {row}. "
                f"Nested loops not supported."
            )
        for value in values:
            row[self.FOREACH_LOOP_VALUE_NAME] = value
            entity = self.child_entity_manifest.build_from_row(row)
            del row[self.FOREACH_LOOP_VALUE_NAME]
            result.append(entity)
        return result


@attr.s(kw_only=True)
class ListRelationshipFieldManifest(ManifestNode[List[Entity]]):
    """Manifest describing a relationship field that will be hydrated with a list of
    entities that have been recursively hydrated based on the provided child tree
    manifests.
    """

    child_manifests: List[
        Union[ExpandableListItemManifest, EntityTreeManifest]
    ] = attr.ib()

    def build_from_row(self, row: Dict[str, str]) -> List[Entity]:
        child_entities = []
        for child_manifest in self.child_manifests:
            if isinstance(child_manifest, ExpandableListItemManifest):
                child_entities.extend(child_manifest.expand(row))
            elif isinstance(child_manifest, EntityTreeManifest):
                child_entities.append(child_manifest.build_from_row(row))
            else:
                raise ValueError(
                    f"Unexpected type for child manifest: {type(child_manifest)}"
                )
        return child_entities


@attr.s(kw_only=True)
class DirectMappingFieldManifest(ManifestNode[str]):
    """Manifest describing a flat field that will be hydrated with the value of a
    specific column.
    """

    mapped_column: str = attr.ib()

    def build_from_row(self, row: Dict[str, str]) -> str:
        return row[self.mapped_column]


@attr.s(kw_only=True)
class StringLiteralFieldManifest(ManifestNode[str]):
    """Manifest describing a flat field that will be hydrated with a string literal
    value for all input rows.
    """

    # String literals are denoted like $literal("MY_STR")
    STRING_LITERAL_VALUE_REGEX = re.compile(r"^\$literal\(\"(.+)\"\)$")

    literal_value: str = attr.ib()

    def build_from_row(self, row: Dict[str, str]) -> str:
        return self.literal_value
