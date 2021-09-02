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
from enum import Enum
from typing import Callable, Dict, Generic, List, Optional, Type, TypeVar, Union

import attr

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.enum_parser import EnumParser
from recidiviz.common.constants.strict_enum_parser import StrictEnumParser
from recidiviz.persistence.entity.base_entity import Entity, EnumEntity
from recidiviz.persistence.entity.entity_deserialize import EntityFactory, EntityT
from recidiviz.utils.yaml_dict import YAMLDict

ManifestNodeT = TypeVar("ManifestNodeT")


@attr.s(kw_only=True)
class ManifestNode(Generic[ManifestNodeT]):
    """Abstract interface for all nodes in the manifest abstract syntax tree. Subclasses
    may be leaf nodes (e.g. represent flat fields) or subtree root nodes (e.g. represent
    entity relationships).
    """

    @abc.abstractmethod
    def build_from_row(self, row: Dict[str, str]) -> Optional[ManifestNodeT]:
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

    # Optional predicate for filtering out hydrated entities. If returns True,
    # build_for_row() will return null instead of this entity (and any children
    # entities) will be excluded entirely from the result.
    #
    # Currently this is primarily used for enum entities. If the enum value is null or
    # ignored by the mappings, the entire enum entity will be filtered out.
    filter_predicate: Optional[Callable[[EntityT], bool]] = attr.ib(default=None)

    def build_from_row(self, row: Dict[str, str]) -> Optional[EntityT]:
        """Builds a recursively hydrated entity from the given input row."""
        args = self.common_args.copy()

        for field_name, field_manifest in self.field_manifests.items():
            field_value = field_manifest.build_from_row(row)
            if field_value:
                args[field_name] = field_value

        entity = self.entity_factory_cls.deserialize(**args)

        if not isinstance(entity, self.entity_cls):
            raise ValueError(f"Unexpected type for entity: [{type(entity)}]")

        if self.filter_predicate and self.filter_predicate(entity):
            return None

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
            if entity:
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
                child_entity = child_manifest.build_from_row(row)
                if child_entity:
                    child_entities.append(child_entity)
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


@attr.s(kw_only=True)
class EnumFieldManifest(ManifestNode[StrictEnumParser]):
    """Manifest describing a flat field that will be hydrated into a parsed enum value."""

    # Raw manifest key whose value describes where to look for the raw text value to
    # parse this enum from.
    RAW_TEXT_KEY = "$raw_text"

    # Raw manifest key whose value describes the direct string mappings for this enum
    # field.
    MAPPINGS_KEY = "$mappings"

    # Raw manifest key whose value describes the string raw text values that should
    # be ignored when parsing this enum field.
    IGNORES_KEY = "$ignore"

    enum_cls: Type[Enum] = attr.ib()
    mapped_raw_text_col: str = attr.ib()
    enum_overrides: EnumOverrides = attr.ib()
    raw_text_field_manifest: ManifestNode = attr.ib()

    def build_from_row(self, row: Dict[str, str]) -> StrictEnumParser:
        raw_text = row[self.mapped_raw_text_col]
        return StrictEnumParser(
            raw_text=raw_text,
            enum_cls=self.enum_cls,
            enum_overrides=self.enum_overrides,
        )

    @classmethod
    def raw_text_field_name(cls, enum_field_name: str) -> str:
        return f"{enum_field_name}{EnumEntity.RAW_TEXT_FIELD_SUFFIX}"

    @classmethod
    def from_raw_manifest(
        cls, *, enum_cls: Type[Enum], field_enum_mappings_manifest: YAMLDict
    ) -> "EnumFieldManifest":
        """Factory method for building an enum field manifest."""
        mapped_raw_text_col = field_enum_mappings_manifest.pop(
            EnumFieldManifest.RAW_TEXT_KEY, str
        )

        enum_overrides = cls._build_field_enum_overrides(
            enum_cls,
            ignores_list=field_enum_mappings_manifest.pop_list(
                EnumFieldManifest.IGNORES_KEY, str
            ),
            raw_mappings_manifest=field_enum_mappings_manifest.pop_dict(
                EnumFieldManifest.MAPPINGS_KEY
            ),
        )

        if len(field_enum_mappings_manifest):
            raise ValueError(
                f"Found unused keys in field enum mappings manifest: "
                f"{field_enum_mappings_manifest.keys()}"
            )
        return EnumFieldManifest(
            enum_cls=enum_cls,
            mapped_raw_text_col=mapped_raw_text_col,
            enum_overrides=enum_overrides,
            raw_text_field_manifest=DirectMappingFieldManifest(
                mapped_column=mapped_raw_text_col,
            ),
        )

    @staticmethod
    def _build_field_enum_overrides(
        enum_cls: Type[Enum], ignores_list: List[str], raw_mappings_manifest: YAMLDict
    ) -> EnumOverrides:
        """Builds the enum mappings object that should be used to parse the enum value."""

        enum_overrides_builder = EnumOverrides.Builder()

        for enum_value_str in raw_mappings_manifest.keys():
            enum_cls_name, enum_name = enum_value_str.split(".")
            if enum_cls_name != enum_cls.__name__:
                raise ValueError(
                    f"Declared enum class in manifest [{enum_cls_name}] does "
                    f"not match expected enum class type [{enum_cls.__name__}]."
                )
            value_manifest_type = raw_mappings_manifest.peek_type(enum_value_str)
            if value_manifest_type is str:
                mappings_raw_text_list: List[str] = [
                    raw_mappings_manifest.pop(enum_value_str, str)
                ]
            elif value_manifest_type is list:
                mappings_raw_text_list = []
                for raw_text in raw_mappings_manifest.pop(enum_value_str, list):
                    if not isinstance(raw_text, str) or not raw_text:
                        raise ValueError(f"Unexpected value for raw_text: {raw_text}")
                    mappings_raw_text_list.append(raw_text)
            else:
                raise ValueError(
                    f"Unexpected mapping values manifest type: {value_manifest_type}"
                )
            for raw_text in mappings_raw_text_list:
                enum_overrides_builder.add(
                    raw_text, enum_cls[enum_name], normalize_label=False
                )

        for raw_text_value in ignores_list:
            enum_overrides_builder.ignore(
                raw_text_value, enum_cls, normalize_label=False
            )

        return enum_overrides_builder.build()
