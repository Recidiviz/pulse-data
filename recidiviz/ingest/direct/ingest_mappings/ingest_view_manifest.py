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
import json
import re
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import attr
from more_itertools import one

from recidiviz.common.attr_mixins import (
    BuildableAttrFieldType,
    attr_field_attribute_for_field_name,
    attr_field_enum_cls_for_field_name,
    attr_field_type_for_field_name,
)
from recidiviz.common.attr_utils import get_non_flat_attribute_class_name
from recidiviz.common.constants.enum_overrides import EnumOverrides, EnumT
from recidiviz.common.constants.strict_enum_parser import StrictEnumParser
from recidiviz.ingest.direct.ingest_mappings.ingest_view_results_parser_delegate import (
    IngestViewResultsParserDelegate,
)
from recidiviz.persistence.entity.base_entity import Entity, EnumEntity
from recidiviz.persistence.entity.entity_deserialize import (
    DeserializableEntityFieldValue,
    EntityFactory,
    EntityT,
)
from recidiviz.utils.yaml_dict import YAMLDict

ManifestNodeT = TypeVar("ManifestNodeT")


@attr.s(kw_only=True)
class ManifestNode(Generic[ManifestNodeT]):
    """Abstract interface for all nodes in the manifest abstract syntax tree. Subclasses
    may be leaf nodes (e.g. represent flat fields) or subtree root nodes (e.g. represent
    entity relationships).
    """

    @property
    @abc.abstractmethod
    def result_type(self) -> Type[ManifestNodeT]:
        """Should be implemented by subclasses to return the type that this class
        returns from |build_from_row|.
        """

    @abc.abstractmethod
    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        """Allows additional fields to be set as a side effect of this manifest.

        The primary use case for this is to allow enum manifests to additionally set the
        raw text field that corresponds to the enum field that they hydrate.
        """

    @abc.abstractmethod
    def build_from_row(self, row: Dict[str, str]) -> Optional[ManifestNodeT]:
        """Should be implemented by subclasses to return a recursively hydrated node
        in the entity tree, parsed out of the input row.
        """

    def columns_referenced(self) -> Set[str]:
        """Returns a set of columns that this node references. Must be overridden by
        subclasses that do not have child nodes.
        """
        children = self.child_manifest_nodes()
        if not children:
            raise ValueError(
                f"Must override columns_referenced() for class [{self.__class__}] "
                f"which has no children."
            )

        return {col for child in children for col in child.columns_referenced()}

    def variables_referenced(self) -> Set[str]:
        """Returns a set of columns that this node references. Should not be overridden
        by subclasses other than the VariableManifestNode
        """
        children = self.child_manifest_nodes()
        return {var for child in children for var in child.variables_referenced()}

    @abc.abstractmethod
    def child_manifest_nodes(self) -> List["ManifestNode"]:
        """Should be implemented by subclasses to return a list of child ManifestNodes
        that this node references.
        """


@attr.s(kw_only=True)
class VariableManifestNode(ManifestNode[ManifestNodeT]):
    variable_name: str = attr.ib()
    value_manifest: ManifestNode[ManifestNodeT] = attr.ib()

    @property
    def result_type(self) -> Type[ManifestNodeT]:
        return self.value_manifest.result_type

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return self.value_manifest.additional_field_manifests(field_name)

    def build_from_row(self, row: Dict[str, str]) -> Optional[ManifestNodeT]:
        return self.value_manifest.build_from_row(row)

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return [self.value_manifest]

    def variables_referenced(self) -> Set[str]:
        return super().variables_referenced() | {self.variable_name}


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
    common_args: Dict[str, DeserializableEntityFieldValue] = attr.ib()

    # Optional predicate for filtering out hydrated entities. If returns True,
    # build_for_row() will return null instead of this entity (and any children
    # entities) will be excluded entirely from the result.
    #
    # Currently this is primarily used for enum entities. If the enum value is null or
    # ignored by the mappings, the entire enum entity will be filtered out.
    filter_predicate: Optional[Callable[[EntityT], bool]] = attr.ib(default=None)

    @property
    def result_type(self) -> Type[EntityT]:
        return self.entity_cls

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> Optional[EntityT]:
        """Builds a recursively hydrated entity from the given input row."""
        args: Dict[str, DeserializableEntityFieldValue] = self.common_args.copy()

        for field_name, field_manifest in self.field_manifests.items():
            field_value = field_manifest.build_from_row(row)
            if field_value is not None:
                args[field_name] = field_value

        entity = self.entity_factory_cls.deserialize(**args)

        if not isinstance(entity, self.entity_cls):
            raise ValueError(f"Unexpected type for entity: [{type(entity)}]")

        if self.filter_predicate and self.filter_predicate(entity):
            return None

        return entity

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return list(self.field_manifests.values())


class EntityTreeManifestFactory:
    """Factory class for building EntityTreeManifests."""

    @classmethod
    def from_raw_manifest(
        cls,
        *,
        raw_fields_manifest: YAMLDict,
        delegate: IngestViewResultsParserDelegate,
        variable_manifests: Dict[str, VariableManifestNode],
        entity_cls: Type[EntityT],
    ) -> "EntityTreeManifest":
        """Returns a single, recursively hydrated entity tree manifest, which can be
        used to translate a single input row into an entity tree.
        """

        field_manifests: Dict[str, ManifestNode] = {}
        for field_name in raw_fields_manifest.keys():
            field_type = attr_field_type_for_field_name(entity_cls, field_name)
            attribute = attr_field_attribute_for_field_name(entity_cls, field_name)
            if not attribute.type:
                raise ValueError(
                    f"Field attribute type is unexpectedly null for field "
                    f"[{field_name}] on class [{entity_cls.__name__}]"
                )
            if field_name == entity_cls.get_primary_key_column_name():
                error_message = (
                    f"Cannot set autogenerated database primary key field "
                    f"[{field_name}] in the ingest manifest."
                )
                if "external_id" in attr.fields_dict(entity_cls):
                    error_message += " Did you mean to set the 'external_id' field?"
                raise ValueError(error_message)

            field_manifest: ManifestNode
            if field_type is BuildableAttrFieldType.LIST:
                child_manifests: List[
                    Union[ExpandableListItemManifest, ManifestNode[Entity]]
                ] = []
                child_entity_cls_name = get_non_flat_attribute_class_name(attribute)
                if not child_entity_cls_name:
                    raise ValueError(
                        f"Child class type unexpectedly null for field [{field_name}] "
                        f"on [{entity_cls.__name__}]."
                    )
                for raw_child_manifest in raw_fields_manifest.pop_dicts(field_name):
                    child_manifest = build_manifest_from_raw(
                        raw_field_manifest=raw_child_manifest,
                        delegate=delegate,
                        variable_manifests=variable_manifests,
                        expected_result_type=delegate.get_entity_cls(
                            child_entity_cls_name
                        ),
                    )
                    if isinstance(child_manifest, ExpandableListItemManifest):
                        child_manifests.append(child_manifest)
                    elif issubclass(child_manifest.result_type, Entity):
                        child_manifests.append(child_manifest)
                    else:
                        raise ValueError(
                            f"Unexpected child_manifest type: [{type(child_manifest)}]"
                        )
                field_manifest = ListRelationshipFieldManifest(
                    child_manifests=child_manifests
                )
            elif field_type is BuildableAttrFieldType.FORWARD_REF:
                child_entity_cls_name = get_non_flat_attribute_class_name(attribute)
                if not child_entity_cls_name:
                    raise ValueError(
                        f"Child class type unexpectedly null for field [{field_name}] "
                        f"on [{entity_cls.__name__}]."
                    )
                field_manifest = build_manifest_from_raw_typed(
                    raw_field_manifest=raw_fields_manifest.pop_dict(field_name),
                    delegate=delegate,
                    variable_manifests=variable_manifests,
                    expected_result_type=delegate.get_entity_cls(child_entity_cls_name),
                )
            elif field_type is BuildableAttrFieldType.ENUM:
                expected_enum_cls = attr_field_enum_cls_for_field_name(
                    entity_cls, field_name
                )
                if not expected_enum_cls:
                    raise ValueError(
                        f"No enum class for field [{field_name}] in class "
                        f"[{entity_cls}]."
                    )

                field_manifest = build_manifest_from_raw_typed(
                    raw_field_manifest=pop_raw_flat_field_manifest(
                        field_name, raw_fields_manifest
                    ),
                    delegate=delegate,
                    variable_manifests=variable_manifests,
                    expected_result_type=expected_enum_cls,
                )

            elif field_type in (
                # These are flat fields and should be parsed into a ManifestNode[str],
                # since all values will be converted from string -> real value in the
                # deserializing entity factory.
                BuildableAttrFieldType.DATE,
                BuildableAttrFieldType.STRING,
                BuildableAttrFieldType.INTEGER,
            ):
                if field_name.endswith(EnumEntity.RAW_TEXT_FIELD_SUFFIX):
                    raise ValueError(
                        f"Enum raw text fields should not be mapped independently "
                        f"of their corresponding enum fields. Found direct mapping "
                        f"for field [{field_name}]."
                    )
                field_manifest = build_str_manifest_from_raw(
                    pop_raw_flat_field_manifest(field_name, raw_fields_manifest),
                    delegate,
                    variable_manifests=variable_manifests,
                )
            elif field_type is BuildableAttrFieldType.BOOLEAN:
                field_manifest = build_manifest_from_raw_typed(
                    raw_field_manifest=pop_raw_flat_field_manifest(
                        field_name, raw_fields_manifest
                    ),
                    delegate=delegate,
                    variable_manifests=variable_manifests,
                    expected_result_type=bool,
                )
            else:
                raise ValueError(
                    f"Unexpected field type [{field_type}] for field [{field_name}]"
                )

            if field_name in field_manifests:
                raise ValueError(
                    f"Field [{field_name}] already has a manifest "
                    f"defined. This field likely is automatically generated via another"
                    f"field manifest (e.g. the raw text for an enum mapping) and should"
                    f"not be manually defined in the YAML mappings file."
                )

            field_manifests[field_name] = field_manifest

            for (
                additional_field_name,
                additional_manifest,
            ) in field_manifest.additional_field_manifests(field_name).items():
                cls._validate_additional_field_manifest(
                    entity_cls=entity_cls,
                    additional_field_name=additional_field_name,
                    additional_manifest=additional_manifest,
                )
                if additional_field_name in field_manifests:
                    raise ValueError(
                        f"Field [{additional_field_name}] already has a manifest "
                        f"defined. This field should not be specified manually in the"
                        f"YAML mappings file."
                    )
                field_manifests[additional_field_name] = additional_manifest

        if len(raw_fields_manifest):
            raise ValueError(
                f"Found unused keys in fields manifest: {raw_fields_manifest.keys()}"
            )

        primary_filter_predicate = cls._get_filter_predicate(
            entity_cls, field_manifests
        )
        delegate_filter_predicate = delegate.get_filter_predicate(entity_cls)

        def union_filter_predicate(e: EntityT) -> bool:
            return (
                primary_filter_predicate is not None and primary_filter_predicate(e)
            ) or (
                delegate_filter_predicate is not None and delegate_filter_predicate(e)
            )

        entity_factory_cls = delegate.get_entity_factory_class(entity_cls.__name__)
        return EntityTreeManifest(
            entity_cls=entity_cls,
            entity_factory_cls=entity_factory_cls,
            common_args=delegate.get_common_args(),
            field_manifests=field_manifests,
            filter_predicate=union_filter_predicate,
        )

    @staticmethod
    def _validate_additional_field_manifest(
        *,
        entity_cls: Type[EntityT],
        additional_field_name: str,
        additional_manifest: ManifestNode,
    ) -> None:
        """Validates that the manifest for the provided additional field is valid."""
        if additional_manifest.additional_field_manifests(additional_field_name):
            raise ValueError(
                f"Manifest for additional field [{additional_field_name}] should not "
                f"also define additional field manifests."
            )

        additional_field_type = attr_field_type_for_field_name(
            entity_cls, additional_field_name
        )

        expected_result_type: Type
        if additional_field_type == BuildableAttrFieldType.STRING:
            expected_result_type = str
        elif additional_field_type == BuildableAttrFieldType.ENUM:
            expected_result_type = Enum
        elif additional_field_type == BuildableAttrFieldType.BOOLEAN:
            expected_result_type = bool
        else:
            raise ValueError(
                f"No support for additional fields with type [{additional_field_type}]"
            )

        if not issubclass(additional_manifest.result_type, expected_result_type):
            raise ValueError(
                f"Unexpected result type for enum manifest: "
                f"[{additional_manifest.result_type}]. Expected: [{expected_result_type}]"
            )

    # TODO(#8905): Consider using more general logic to build a filter predicate, like
    #  building a @required field annotation for fields that must be hydrated, otherwise
    #  the whole entity is filtered out.
    @staticmethod
    def _get_filter_predicate(
        entity_cls: Type[EntityT], field_manifests: Dict[str, ManifestNode]
    ) -> Optional[Callable[[EntityT], bool]]:
        """Returns a predicate function which can be used to fully filter the evaluated
        EntityTreeManifest from the result.
        """
        if issubclass(entity_cls, EnumEntity):
            enum_field_name = one(
                field_name
                for field_name, manifest in field_manifests.items()
                if issubclass(manifest.result_type, Enum)
            )

            def enum_entity_filter_predicate(e: EntityT) -> bool:
                return getattr(e, enum_field_name) is None

            return enum_entity_filter_predicate
        return None


@attr.s(kw_only=True)
class SplitCommaSeparatedListManifest(ManifestNode[List[str]]):
    """A node that reads the comma-separated string value from a given column and
    splits it into a list of strings.
    """

    # Default delimiter used to split list column values.
    DEFAULT_LIST_VALUE_DELIMITER = ","

    column_name: str = attr.ib()

    @property
    def result_type(self) -> Type[List[str]]:
        return List[str]

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> List[str]:
        column_value = row[self.column_name]
        if not column_value:
            return []
        # TODO(#8908): For now, we always split list column values on the default
        #  delimiter. Revisit whether the parser language needs be changed to allow the
        #  delimiter to be configurable.
        return column_value.split(self.DEFAULT_LIST_VALUE_DELIMITER)

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return []

    def columns_referenced(self) -> Set[str]:
        return {self.column_name}


@attr.s(kw_only=True)
class SplitJSONListManifest(ManifestNode[List[str]]):
    """A node that reads a JSON list string from a given column and outputs a list of
    strings containing the inner (still-serialized) JSON values.
    """

    SPLIT_JSON_LIST_KEY = "$split_json"

    # The name of the column containing the JSON string to split
    column_name: str = attr.ib()

    @property
    def result_type(self) -> Type[List[str]]:
        return List[str]

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> List[str]:
        column_value = row[self.column_name]
        if not column_value:
            return []

        return [json.dumps(item) for item in json.loads(column_value)]

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return []

    def columns_referenced(self) -> Set[str]:
        return {self.column_name}


@attr.s(kw_only=True)
class ExpandableListItemManifest(ManifestNode[List[Entity]]):
    """A manifest node that describes a list item that can be expanded into 0 to N
    entity trees, based on the value of the input column.
    """

    # Key for a function that produces 0-N items based on the values in an iterable.
    FOREACH_KEY = "$foreach"

    # Key for the argument that produces the iterable.
    FOREACH_ITERABLE_ARG_KEY = "$iterable"

    # Key for the manifest that will be used to produce one item in the result list.
    FOREACH_ITEM_RESULT_ARG_KEY = "$result"

    # Variable "column name" hydrated with a single list item value. Can only be used
    # within the context of a $foreach loop.
    FOREACH_LOOP_VALUE_NAME = "$iter_item"

    # Manifest that will produce the list of values to iterate over.
    values_manifest: ManifestNode[List[str]] = attr.ib()

    child_entity_manifest: ManifestNode[Entity] = attr.ib()

    @property
    def result_type(self) -> Type[List[Entity]]:
        return List[Entity]

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> List[Entity]:
        values = self.values_manifest.build_from_row(row)
        if values is None:
            raise ValueError("Unexpected null list value.")

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

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return [self.values_manifest, self.child_entity_manifest]

    def columns_referenced(self) -> Set[str]:
        return {
            col
            for col in super().columns_referenced()
            if col != self.FOREACH_LOOP_VALUE_NAME
        }

    @classmethod
    def from_raw_manifest(
        cls,
        *,
        raw_function_manifest: YAMLDict,
        delegate: IngestViewResultsParserDelegate,
        variable_manifests: Dict[str, VariableManifestNode],
    ) -> "ExpandableListItemManifest":
        return ExpandableListItemManifest(
            values_manifest=build_iterable_manifest_from_raw(
                raw_iterable_manifest=pop_raw_flat_field_manifest(
                    ExpandableListItemManifest.FOREACH_ITERABLE_ARG_KEY,
                    raw_function_manifest,
                )
            ),
            child_entity_manifest=build_manifest_from_raw_typed(
                raw_field_manifest=pop_raw_flat_field_manifest(
                    ExpandableListItemManifest.FOREACH_ITEM_RESULT_ARG_KEY,
                    raw_function_manifest,
                ),
                variable_manifests=variable_manifests,
                delegate=delegate,
                expected_result_type=Entity,
            ),
        )


@attr.s(kw_only=True)
class ListRelationshipFieldManifest(ManifestNode[List[Entity]]):
    """Manifest describing a relationship field that will be hydrated with a list of
    entities that have been recursively hydrated based on the provided child tree
    manifests.
    """

    child_manifests: List[
        Union[ExpandableListItemManifest, ManifestNode[Entity]]
    ] = attr.ib()

    @property
    def result_type(self) -> Type[List[Entity]]:
        return List[Entity]

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> List[Entity]:
        child_entities = []
        for child_manifest in self.child_manifests:
            if isinstance(child_manifest, ExpandableListItemManifest):
                child_entities.extend(child_manifest.build_from_row(row))
            else:
                child_entity = child_manifest.build_from_row(row)
                if child_entity:
                    child_entities.append(child_entity)
        return child_entities

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return self.child_manifests


@attr.s(kw_only=True)
class DirectMappingFieldManifest(ManifestNode[str]):
    """Manifest describing a flat field that will be hydrated with the value of a
    specific column.
    """

    mapped_column: str = attr.ib()

    @property
    def result_type(self) -> Type[str]:
        return str

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> str:
        return row[self.mapped_column]

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return []

    def columns_referenced(self) -> Set[str]:
        return {self.mapped_column}


@attr.s(kw_only=True)
class StringLiteralFieldManifest(ManifestNode[str]):
    """Manifest describing a flat field that will be hydrated with a string literal
    value for all input rows.
    """

    # String literals are denoted like $literal("MY_STR")
    STRING_LITERAL_VALUE_REGEX = re.compile(r"^\$literal\(\"(.+)\"\)$")

    literal_value: Optional[str] = attr.ib()

    @property
    def result_type(self) -> Type[str]:
        return str

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> Optional[str]:
        return self.literal_value

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return []

    def columns_referenced(self) -> Set[str]:
        return set()


def _raw_text_field_name(enum_field_name: str) -> str:
    return f"{enum_field_name}{EnumEntity.RAW_TEXT_FIELD_SUFFIX}"


@attr.s(kw_only=True)
class EnumLiteralFieldManifest(ManifestNode[EnumT]):
    """Manifest describing a flat field that will be hydrated into an enum value that
    always has the same value.
    """

    ENUM_LITERAL_VALUE_REGEX = re.compile(
        r"^\$literal_enum\((?P<enum_cls_name>[^.]+)\.(?P<enum_value_name>[^.]+)\)$"
    )

    enum_value: EnumT = attr.ib()

    @property
    def result_type(self) -> Type[EnumT]:
        return self.enum_value.__class__

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        # While this is effectively a no-op, it allows us to enforce that nobody
        # sets the associated raw text field manually.
        return {
            _raw_text_field_name(field_name): StringLiteralFieldManifest(
                literal_value=None
            )
        }

    def build_from_row(self, row: Dict[str, str]) -> EnumT:
        return self.enum_value

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return []

    def columns_referenced(self) -> Set[str]:
        return set()

    @classmethod
    def from_raw_manifest(
        cls,
        *,
        raw_manifest: str,
        delegate: IngestViewResultsParserDelegate,
    ) -> "EnumLiteralFieldManifest":
        match = re.match(
            EnumLiteralFieldManifest.ENUM_LITERAL_VALUE_REGEX,
            raw_manifest,
        )
        if not match:
            raise ValueError(
                f"Raw enum literal manifest does not match regex: [{raw_manifest}]."
            )

        enum_cls_name = match.group("enum_cls_name")
        enum_value_name = match.group("enum_value_name")

        enum_cls = delegate.get_enum_cls(enum_cls_name)
        return EnumLiteralFieldManifest(enum_value=enum_cls[enum_value_name])


@attr.s(kw_only=True)
class EnumMappingManifest(ManifestNode[EnumT]):
    """Manifest describing a flat field that will be hydrated into a parsed enum value."""

    # Key for an enum mappings manifest that will hydrate an enum field and its
    # associated raw text field.
    ENUM_MAPPING_KEY = "$enum_mapping"

    # Raw manifest key whose value describes where to look for the raw text value to
    # parse this enum from.
    RAW_TEXT_KEY = "$raw_text"

    # Raw manifest key whose value describes the direct string mappings for this enum
    # field. May only be present if the $custom_parser key is not present.
    MAPPINGS_KEY = "$mappings"

    # Raw manifest key whose value describes the custom parser function for this enum
    # field. May only be present if the $mappings key is not present.
    CUSTOM_PARSER_FUNCTION_KEY = "$custom_parser"

    CUSTOM_PARSER_RAW_TEXT_ARG_NAME = "raw_text"

    # Raw manifest key whose value describes the string raw text values that should
    # be ignored when parsing this enum field. If this is used with $custom_parser,
    # these values will never be passed to the custom parser function.
    IGNORES_KEY = "$ignore"

    enum_cls: Type[EnumT] = attr.ib()
    enum_overrides: EnumOverrides = attr.ib()
    raw_text_field_manifest: ManifestNode[str] = attr.ib()

    @property
    def result_type(self) -> Type[EnumT]:
        return self.enum_cls

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {_raw_text_field_name(field_name): self.raw_text_field_manifest}

    def build_from_row(self, row: Dict[str, str]) -> Optional[EnumT]:
        return StrictEnumParser(
            raw_text=self.raw_text_field_manifest.build_from_row(row),
            enum_cls=self.enum_cls,
            enum_overrides=self.enum_overrides,
        ).parse()

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return [self.raw_text_field_manifest]

    @classmethod
    def from_raw_manifest(
        cls,
        *,
        field_enum_mappings_manifest: YAMLDict,
        delegate: IngestViewResultsParserDelegate,
        variable_manifests: Dict[str, VariableManifestNode],
    ) -> "EnumMappingManifest":
        """Factory method for building an enum field manifest."""

        raw_text_field_manifest = build_str_manifest_from_raw(
            pop_raw_flat_field_manifest(
                EnumMappingManifest.RAW_TEXT_KEY, field_enum_mappings_manifest
            ),
            delegate,
            variable_manifests=variable_manifests,
        )

        enum_cls, enum_overrides = cls._build_field_enum_overrides(
            delegate,
            ignores_list=field_enum_mappings_manifest.pop_list_optional(
                EnumMappingManifest.IGNORES_KEY, str
            ),
            direct_mappings_manifest=field_enum_mappings_manifest.pop_dict_optional(
                EnumMappingManifest.MAPPINGS_KEY
            ),
            custom_parser_function_reference=field_enum_mappings_manifest.pop_optional(
                EnumMappingManifest.CUSTOM_PARSER_FUNCTION_KEY, str
            ),
        )

        if len(field_enum_mappings_manifest):
            raise ValueError(
                f"Found unused keys in field enum mappings manifest: "
                f"{field_enum_mappings_manifest.keys()}"
            )
        return EnumMappingManifest(
            enum_cls=enum_cls,
            enum_overrides=enum_overrides,
            raw_text_field_manifest=raw_text_field_manifest,
        )

    @classmethod
    def _build_field_enum_overrides(
        cls,
        delegate: IngestViewResultsParserDelegate,
        ignores_list: Optional[List[str]],
        direct_mappings_manifest: Optional[YAMLDict],
        custom_parser_function_reference: Optional[str],
    ) -> Tuple[Type[Enum], EnumOverrides]:
        """Builds the enum mappings object that should be used to parse the enum value."""

        enum_overrides_builder = EnumOverrides.Builder()

        if (
            direct_mappings_manifest is None
            and custom_parser_function_reference is None
        ):
            raise ValueError(
                f"Must define either [{cls.MAPPINGS_KEY}] or [{cls.CUSTOM_PARSER_FUNCTION_KEY}]."
            )

        if (
            direct_mappings_manifest is not None
            and custom_parser_function_reference is not None
        ):
            raise ValueError(
                f"Can only define either [{cls.MAPPINGS_KEY}] or "
                f"[{cls.CUSTOM_PARSER_FUNCTION_KEY}], but not both."
            )

        if direct_mappings_manifest is not None:
            if not direct_mappings_manifest:
                raise ValueError(
                    f"Found empty {cls.MAPPINGS_KEY}. If there are no direct string "
                    f"mappings, the key should be omitted entirely."
                )

            enum_cls: Type[Enum]
            seen_enum_classes: Set[Type[Enum]] = set()
            for enum_value_str in direct_mappings_manifest.keys():
                enum_cls_name, enum_name = enum_value_str.split(".")
                enum_cls = delegate.get_enum_cls(enum_cls_name)
                seen_enum_classes.add(enum_cls)

                if len(seen_enum_classes) > 1:
                    enum_cls_names = sorted(list(e.__name__ for e in seen_enum_classes))
                    raise ValueError(
                        f"Enum $mappings should only contain mappings for one enum "
                        f"type but found multiple: {enum_cls_names}"
                    )

                value_manifest_type = direct_mappings_manifest.peek_type(enum_value_str)
                mappings_raw_text_list: List[str]
                if value_manifest_type is str:
                    mappings_raw_text_list = [
                        direct_mappings_manifest.pop(enum_value_str, str)
                    ]
                elif value_manifest_type is list:
                    mappings_raw_text_list = []
                    for raw_text in direct_mappings_manifest.pop(enum_value_str, list):
                        if not isinstance(raw_text, str) or not raw_text:
                            raise ValueError(
                                f"Unexpected value for raw_text: {raw_text}"
                            )
                        mappings_raw_text_list.append(raw_text)
                else:
                    raise ValueError(
                        f"Unexpected mapping values manifest type: {value_manifest_type}"
                    )
                if not mappings_raw_text_list:
                    raise ValueError(
                        f"Mappings for value [{enum_value_str}] are empty. Either add "
                        f"mappings or remove this item."
                    )
                for raw_text in mappings_raw_text_list:
                    enum_overrides_builder.add(
                        raw_text, enum_cls[enum_name], normalize_label=False
                    )

        if custom_parser_function_reference:
            (
                fn,
                enum_cls,
            ) = delegate.get_custom_function_registry().get_custom_python_function(
                custom_parser_function_reference,
                {cls.CUSTOM_PARSER_RAW_TEXT_ARG_NAME: str},
                Enum,
            )
            enum_overrides_builder.add_mapper_fn(
                fn,
                enum_cls,
            )

        if ignores_list is not None:
            if not ignores_list:
                raise ValueError(
                    f"Found empty {cls.IGNORES_KEY} list. If there are no ignores, the "
                    f"key should be omitted entirely."
                )
            for raw_text_value in ignores_list:
                enum_overrides_builder.ignore(
                    raw_text_value, enum_cls, normalize_label=False
                )

        return enum_cls, enum_overrides_builder.build()


@attr.s(kw_only=True)
class CustomFunctionManifest(ManifestNode[ManifestNodeT]):
    """Manifest describing a value that is derived from a custom python function, whose
    inputs are described in the raw manifest.
    """

    CUSTOM_FUNCTION_KEY = "$custom"

    CUSTOM_FUNCTION_REFERENCE_KEY = "$function"
    CUSTOM_FUNCTION_ARGS_KEY = "$args"

    # Note: We should be able to type this better with ParamSpec when we update to
    # Python 3.10.
    function: Callable[..., Optional[ManifestNodeT]] = attr.ib()
    kwarg_manifests: Dict[str, ManifestNode[Any]] = attr.ib()

    function_return_type: Type[ManifestNodeT] = attr.ib()

    @property
    def result_type(self) -> Type[ManifestNodeT]:
        return self.function_return_type

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> Optional[ManifestNodeT]:
        kwargs = {
            key: manifest.build_from_row(row)
            for key, manifest in self.kwarg_manifests.items()
        }
        return self.function(**kwargs)

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return list(self.kwarg_manifests.values())

    @classmethod
    def from_raw_manifest(
        cls,
        *,
        raw_function_manifest: YAMLDict,
        delegate: IngestViewResultsParserDelegate,
        variable_manifests: Dict[str, VariableManifestNode],
        expected_return_type: Type[ManifestNodeT],
    ) -> "CustomFunctionManifest[ManifestNodeT]":
        """Builds a CustomParserManifest node from the provide raw manifest. Verifies
        that the function signature matches what is expected from the provided args.
        """
        custom_parser_function_reference = raw_function_manifest.pop(
            cls.CUSTOM_FUNCTION_REFERENCE_KEY, str
        )

        raw_args_manifests = raw_function_manifest.pop_dict(
            cls.CUSTOM_FUNCTION_ARGS_KEY
        ).get()
        kwarg_manifests: Dict[str, ManifestNode] = {}
        for arg, raw_manifest in raw_args_manifests.items():
            if isinstance(raw_manifest, str):
                kwarg_manifests[arg] = build_manifest_from_raw(
                    raw_field_manifest=raw_manifest,
                    delegate=delegate,
                    variable_manifests=variable_manifests,
                    expected_result_type=object,
                )
            elif isinstance(raw_manifest, dict):
                kwarg_manifests[arg] = build_manifest_from_raw(
                    raw_field_manifest=YAMLDict(raw_manifest),
                    delegate=delegate,
                    variable_manifests=variable_manifests,
                    expected_result_type=object,
                )
            else:
                raise ValueError(
                    f"Unexpected raw manifest type for arg [{arg}] in $args list: "
                    f"[{type(raw_manifest)}]"
                )

        function_registry = delegate.get_custom_function_registry()
        function, nonnull_return_type = function_registry.get_custom_python_function(
            custom_parser_function_reference,
            {arg: manifest.result_type for arg, manifest in kwarg_manifests.items()},
            expected_return_type,
        )
        return CustomFunctionManifest(
            function=function,
            function_return_type=nonnull_return_type,
            kwarg_manifests=kwarg_manifests,
        )


@attr.s(kw_only=True)
class SerializedJSONDictFieldManifest(ManifestNode[str]):
    """Manifest describing the value for a flat field that will be hydrated with
    serialized JSON, derived from the values in 1 or more columns.
    """

    # Function name used to identify raw manifests of this type.
    JSON_DICT_KEY = "$json_dict"

    # Maps JSON dict keys to values they should be hydrated with
    key_to_manifest_map: Dict[str, ManifestNode[str]] = attr.ib()

    # If all the dictionary values are empty, return None instead of serialized JSON.
    drop_all_empty: bool = attr.ib(default=False)

    @property
    def result_type(self) -> Type[str]:
        return str

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> Optional[str]:
        result_dict = {
            key: manifest.build_from_row(row)
            for key, manifest in self.key_to_manifest_map.items()
        }
        if self.drop_all_empty:
            has_non_empty_value = any(value for value in result_dict.values())
            if not has_non_empty_value:
                return None
        return json.dumps(result_dict, sort_keys=True)

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return list(self.key_to_manifest_map.values())


@attr.s(kw_only=True)
class JSONExtractKeyManifest(ManifestNode[str]):
    """Manifest describing the value for a flat field that will be hydrated with
    a value that has been extracted from a JSON string.
    """

    # Function name used to identify raw manifests of this type.
    JSON_EXTRACT_KEY = "$json_extract"

    JSON_STRING_ARG_KEY = "$json"
    JSON_KEY_ARG_KEY = "$key"

    json_manifest: ManifestNode[str] = attr.ib()
    json_key: str = attr.ib()

    @property
    def result_type(self) -> Type[str]:
        return str

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> str:
        json_str = self.json_manifest.build_from_row(row)
        if json_str is None:
            raise ValueError(f"Expected nonnull JSON string for row: {row}")
        json_dict = json.loads(json_str)
        return json_dict[self.json_key]

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return [self.json_manifest]

    @classmethod
    def from_raw_manifest(
        cls,
        *,
        raw_function_manifest: YAMLDict,
        delegate: IngestViewResultsParserDelegate,
        variable_manifests: Dict[str, VariableManifestNode],
    ) -> "JSONExtractKeyManifest":
        return JSONExtractKeyManifest(
            json_manifest=build_str_manifest_from_raw(
                pop_raw_flat_field_manifest(
                    cls.JSON_STRING_ARG_KEY, raw_function_manifest
                ),
                delegate,
                variable_manifests=variable_manifests,
            ),
            json_key=raw_function_manifest.pop(cls.JSON_KEY_ARG_KEY, str),
        )


@attr.s(kw_only=True)
class ConcatenatedStringsManifest(ManifestNode[str]):
    """Manifest describing a value that is hydrated by concatenating 0-N values, with
    a separator.
    """

    # Function name used to identify raw manifests of this type.
    CONCATENATE_KEY = "$concat"

    # Optional function argument key for string separator.
    SEPARATOR_ARG_KEY = "$separator"

    # Function argument key for the list of raw manifests for values to concatenate.
    VALUES_ARG_KEY = "$values"

    # Function argument key for a boolean indicator whether to include null or empty
    # values. If True, falsy values will be cast to the string "NONE" before
    # concatenation. Optional argument, defaults to True.
    INCLUDE_NULLS_ARG_KEY = "$include_nulls"

    # Separator that will be used by default when concatenating values, if one is not
    # specified.
    DEFAULT_SEPARATOR = "-"

    # List of manifest nodes that can be evaluated to get the list of values to
    # concatenate.
    value_manifests: List[ManifestNode[str]] = attr.ib()

    # The string separator that will be inserted between concatenated values.
    separator: str = attr.ib()

    # If True, falsy values will be cast to the string "NONE" before
    # concatenation. If False, they will omitted entirely.
    include_nulls: bool = attr.ib()

    @property
    def result_type(self) -> Type[str]:
        return str

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> str:
        unfiltered_values = [
            value_manifest.build_from_row(row)
            for value_manifest in self.value_manifests
        ]

        values = []
        for value in unfiltered_values:
            if value:
                values.append(value)
            elif self.include_nulls:
                values.append(str(None).upper())

        return self.separator.join(values)

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return self.value_manifests

    @classmethod
    def from_raw_manifest(
        cls,
        *,
        raw_function_manifest: YAMLDict,
        delegate: IngestViewResultsParserDelegate,
        variable_manifests: Dict[str, VariableManifestNode],
    ) -> "ConcatenatedStringsManifest":
        raw_concat_manifests = pop_raw_manifest_nodes_list(
            raw_function_manifest, cls.VALUES_ARG_KEY
        )
        separator = raw_function_manifest.pop_optional(cls.SEPARATOR_ARG_KEY, str)
        include_nulls = raw_function_manifest.pop_optional(
            cls.INCLUDE_NULLS_ARG_KEY, bool
        )
        return ConcatenatedStringsManifest(
            separator=(separator if separator is not None else cls.DEFAULT_SEPARATOR),
            value_manifests=[
                build_str_manifest_from_raw(
                    raw_manifest, delegate, variable_manifests=variable_manifests
                )
                for raw_manifest in raw_concat_manifests
            ],
            include_nulls=(include_nulls if include_nulls is not None else True),
        )


@attr.s(kw_only=True)
class PhysicalAddressManifest(ManifestNode[str]):
    """Manifest for building a physical address string from parts.
    Example result: "123 Main St, Apt 100, Everytown, CA 12345"
    """

    PHYSICAL_ADDRESS_KEY = "$physical_address"

    # Function argument key for the first line of the address. Required.
    ADDRESS_1_KEY = "$address_1"

    # Function argument key for the second line of the address. Optional.
    ADDRESS_2_KEY = "$address_2"

    # Function argument key for the city name. Required.
    CITY_KEY = "$city"

    # Function argument key for the US state code. Required.
    STATE_KEY = "$state"

    # Function argument key for the zip code. Required.
    ZIP_KEY = "$zip"

    address_1_manifest: ManifestNode[str] = attr.ib()
    address_2_manifest: ManifestNode[str] = attr.ib()
    city_manifest: ManifestNode[str] = attr.ib()
    state_manifest: ManifestNode[str] = attr.ib()
    zip_manifest: ManifestNode[str] = attr.ib()

    @property
    def result_type(self) -> Type[str]:
        return str

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> str:
        state_and_zip_parts = [
            self.state_manifest.build_from_row(row),
            self.zip_manifest.build_from_row(row),
        ]
        address_parts: List[Optional[str]] = [
            self.address_1_manifest.build_from_row(row),
            self.address_2_manifest.build_from_row(row),
            self.city_manifest.build_from_row(row),
            " ".join([s for s in state_and_zip_parts if s]),
        ]

        return ", ".join([s for s in address_parts if s])

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return [
            self.address_1_manifest,
            self.address_2_manifest,
            self.city_manifest,
            self.state_manifest,
            self.zip_manifest,
        ]

    @classmethod
    def from_raw_manifest(
        cls,
        *,
        raw_function_manifest: YAMLDict,
        delegate: IngestViewResultsParserDelegate,
        variable_manifests: Dict[str, VariableManifestNode],
    ) -> "PhysicalAddressManifest":
        raw_address_2_manifest = pop_raw_flat_field_manifest_optional(
            cls.ADDRESS_2_KEY, raw_function_manifest
        )
        return PhysicalAddressManifest(
            address_1_manifest=build_str_manifest_from_raw(
                pop_raw_flat_field_manifest(cls.ADDRESS_1_KEY, raw_function_manifest),
                delegate,
                variable_manifests=variable_manifests,
            ),
            address_2_manifest=build_str_manifest_from_raw(
                raw_address_2_manifest,
                delegate,
                variable_manifests=variable_manifests,
            )
            if raw_address_2_manifest
            else StringLiteralFieldManifest(literal_value=""),
            city_manifest=build_str_manifest_from_raw(
                pop_raw_flat_field_manifest(cls.CITY_KEY, raw_function_manifest),
                delegate,
                variable_manifests=variable_manifests,
            ),
            state_manifest=build_str_manifest_from_raw(
                pop_raw_flat_field_manifest(cls.STATE_KEY, raw_function_manifest),
                delegate,
                variable_manifests=variable_manifests,
            ),
            zip_manifest=build_str_manifest_from_raw(
                pop_raw_flat_field_manifest(cls.ZIP_KEY, raw_function_manifest),
                delegate,
                variable_manifests=variable_manifests,
            ),
        )


@attr.s(kw_only=True)
class PersonNameManifest(ManifestNode[str]):
    """Manifest node for building a JSON-serialized person name."""

    PERSON_NAME_KEY = "$person_name"

    name_json_manifest: SerializedJSONDictFieldManifest = attr.ib()

    # Function argument key for the person's given names. Required. Will populate the
    # 'given_names' JSON key.
    GIVEN_NAMES_MANIFEST_KEY = "$given_names"

    # Function argument key for the person's middle names. Optional. Will populate the
    # 'middle_names' JSON key.
    MIDDLE_NAMES_MANIFEST_KEY = "$middle_names"

    # Function argument key for the person's last name. Required. Will populate the
    # 'surname' JSON key.
    SURNAME_MANIFEST_KEY = "$surname"

    # Function argument key for the person's name suffix (e.g. Jr, III, etc). Optional.
    # Will populate the 'name_suffix' JSON key.
    NAME_SUFFIX_MANIFEST_KEY = "$name_suffix"

    # Map of manifest keys to whether they are required arguments
    NAME_MANIFEST_KEYS = {
        GIVEN_NAMES_MANIFEST_KEY: True,
        MIDDLE_NAMES_MANIFEST_KEY: False,
        SURNAME_MANIFEST_KEY: True,
        NAME_SUFFIX_MANIFEST_KEY: False,
    }

    @property
    def result_type(self) -> Type[str]:
        return str

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> Optional[str]:
        return self.name_json_manifest.build_from_row(row)

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return [self.name_json_manifest]

    @classmethod
    def from_raw_manifest(
        cls,
        *,
        raw_function_manifest: YAMLDict,
        delegate: IngestViewResultsParserDelegate,
        variable_manifests: Dict[str, VariableManifestNode],
    ) -> "PersonNameManifest":
        name_parts_to_manifest: Dict[str, ManifestNode[str]] = {}
        for manifest_key, is_required in cls.NAME_MANIFEST_KEYS.items():
            json_key = manifest_key.lstrip("$")

            raw_manifest = pop_raw_flat_field_manifest_optional(
                manifest_key, raw_function_manifest
            )

            if not raw_manifest and is_required:
                raise ValueError(f"Missing manifest for required key: {manifest_key}.")

            name_parts_to_manifest[json_key] = (
                build_str_manifest_from_raw(
                    raw_manifest, delegate, variable_manifests=variable_manifests
                )
                if raw_manifest
                else StringLiteralFieldManifest(literal_value="")
            )

        return PersonNameManifest(
            name_json_manifest=SerializedJSONDictFieldManifest(
                key_to_manifest_map=name_parts_to_manifest, drop_all_empty=True
            )
        )


@attr.s(kw_only=True)
class ContainsConditionManifest(ManifestNode[bool]):
    """Manifest node that returns a boolean based on whether a value is present in a set
    of options.
    """

    IN_CONDITION_KEY = "$in"

    # Function argument key for the value to check for inclusion in the set.
    VALUE_ARG_KEY = "$value"

    # Function argument key for the set of options to check the value against.
    OPTIONS_ARG_KEY = "$options"

    value_manifest: ManifestNode[str] = attr.ib()
    options_manifests: List[ManifestNode[str]] = attr.ib()

    @property
    def result_type(self) -> Type[bool]:
        return bool

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> bool:
        value = self.value_manifest.build_from_row(row)
        options = {m.build_from_row(row) for m in self.options_manifests}

        return value in options

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return [self.value_manifest, *self.options_manifests]

    @classmethod
    def from_raw_manifest(
        cls,
        *,
        raw_function_manifest: YAMLDict,
        delegate: IngestViewResultsParserDelegate,
        variable_manifests: Dict[str, VariableManifestNode],
    ) -> "ContainsConditionManifest":
        return ContainsConditionManifest(
            value_manifest=build_str_manifest_from_raw(
                pop_raw_flat_field_manifest(cls.VALUE_ARG_KEY, raw_function_manifest),
                delegate,
                variable_manifests=variable_manifests,
            ),
            options_manifests=[
                build_str_manifest_from_raw(
                    raw_manifest,
                    delegate,
                    variable_manifests=variable_manifests,
                )
                for raw_manifest in raw_function_manifest.pop(cls.OPTIONS_ARG_KEY, list)
            ],
        )


@attr.s(kw_only=True)
class IsNullConditionManifest(ManifestNode[bool]):
    """Manifest node that returns a boolean based on whether a value is null (or empty
    string).
    """

    IS_NULL_CONDITION_KEY = "$is_null"

    value_manifest: ManifestNode[str] = attr.ib()

    @property
    def result_type(self) -> Type[bool]:
        return bool

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> bool:
        value = self.value_manifest.build_from_row(row)
        return not bool(value)

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return [self.value_manifest]

    @classmethod
    def from_raw_manifest(
        cls,
        *,
        raw_function_manifest: Union[str, YAMLDict],
        delegate: IngestViewResultsParserDelegate,
        variable_manifests: Dict[str, VariableManifestNode],
    ) -> "IsNullConditionManifest":
        return IsNullConditionManifest(
            value_manifest=build_str_manifest_from_raw(
                raw_function_manifest, delegate, variable_manifests=variable_manifests
            ),
        )


@attr.s(kw_only=True)
class EqualsConditionManifest(ManifestNode[bool]):
    """Manifest node that returns a boolean based on whether a set of values are all
    equal.
    """

    EQUALS_CONDITION_KEY = "$equal"

    value_manifests: List[ManifestNode] = attr.ib()

    @value_manifests.validator
    def _check_value_manifests(
        self, _attribute: attr.Attribute, value_manifests: List[ManifestNode]
    ) -> None:
        if len(value_manifests) < 2:
            raise ValueError(
                f"Found only [{len(value_manifests)}] value manifests, expected at "
                f"least 2."
            )

    @property
    def result_type(self) -> Type[bool]:
        return bool

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> bool:
        first_value = self.value_manifests[0].build_from_row(row)
        return all(
            first_value == value_manifest.build_from_row(row)
            for value_manifest in self.value_manifests[1:]
        )

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return self.value_manifests

    @classmethod
    def from_raw_manifest(
        cls,
        *,
        raw_value_manifests: List[Union[str, YAMLDict]],
        delegate: IngestViewResultsParserDelegate,
        variable_manifests: Dict[str, VariableManifestNode],
    ) -> "EqualsConditionManifest":
        return EqualsConditionManifest(
            value_manifests=[
                build_manifest_from_raw(
                    raw_field_manifest=raw_value_manifest,
                    delegate=delegate,
                    variable_manifests=variable_manifests,
                    expected_result_type=object,
                )
                for raw_value_manifest in raw_value_manifests
            ],
        )


@attr.s(kw_only=True)
class AndConditionManifest(ManifestNode[bool]):
    """Manifest node that returns a boolean based on whether a set of values are all
    True.
    """

    AND_CONDITION_KEY = "$and"

    condition_manifests: List[ManifestNode[bool]] = attr.ib()

    @condition_manifests.validator
    def _check_condition_manifests(
        self, _attribute: attr.Attribute, condition_manifests: List[ManifestNode]
    ) -> None:
        if len(condition_manifests) < 2:
            raise ValueError(
                f"Found only [{len(condition_manifests)}] condition manifests, expected at "
                f"least 2."
            )

    @property
    def result_type(self) -> Type[bool]:
        return bool

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> bool:
        return all(
            value_manifest.build_from_row(row)
            for value_manifest in self.condition_manifests
        )

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return self.condition_manifests

    @classmethod
    def from_raw_manifest(
        cls,
        *,
        raw_condition_manifests: List[Union[str, YAMLDict]],
        delegate: IngestViewResultsParserDelegate,
        variable_manifests: Dict[str, VariableManifestNode],
    ) -> "AndConditionManifest":
        return AndConditionManifest(
            condition_manifests=[
                build_manifest_from_raw_typed(
                    raw_condition_manifest,
                    delegate,
                    variable_manifests=variable_manifests,
                    expected_result_type=bool,
                )
                for raw_condition_manifest in raw_condition_manifests
            ],
        )


@attr.s(kw_only=True)
class OrConditionManifest(ManifestNode[bool]):
    """Manifest node that returns a boolean based on whether any in a set of values is
    True.
    """

    OR_CONDITION_KEY = "$or"

    condition_manifests: List[ManifestNode[bool]] = attr.ib()

    @condition_manifests.validator
    def _check_condition_manifests(
        self, _attribute: attr.Attribute, condition_manifests: List[ManifestNode]
    ) -> None:
        if len(condition_manifests) < 2:
            raise ValueError(
                f"Found only [{len(condition_manifests)}] condition manifests, expected at "
                f"least 2."
            )

    @property
    def result_type(self) -> Type[bool]:
        return bool

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> bool:
        return any(
            value_manifest.build_from_row(row)
            for value_manifest in self.condition_manifests
        )

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return self.condition_manifests

    @classmethod
    def from_raw_manifest(
        cls,
        *,
        raw_condition_manifests: List[Union[str, YAMLDict]],
        delegate: IngestViewResultsParserDelegate,
        variable_manifests: Dict[str, VariableManifestNode],
    ) -> "OrConditionManifest":
        return OrConditionManifest(
            condition_manifests=[
                build_manifest_from_raw_typed(
                    raw_condition_manifest,
                    delegate,
                    variable_manifests=variable_manifests,
                    expected_result_type=bool,
                )
                for raw_condition_manifest in raw_condition_manifests
            ],
        )


@attr.s(kw_only=True)
class InvertConditionManifest(ManifestNode[bool]):

    # Manifest node key for inverted ContainsConditionManifest
    NOT_IN_CONDITION_KEY = "$not_in"

    # Manifest node key for inverted IsNullConditionManifest
    NOT_NULL_CONDITION_KEY = "$not_null"

    condition_manifest: ManifestNode[bool] = attr.ib()

    @property
    def result_type(self) -> Type[bool]:
        return bool

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> bool:
        return not self.condition_manifest.build_from_row(row)

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return [self.condition_manifest]


@attr.s(kw_only=True)
class BooleanLiteralManifest(ManifestNode[bool]):
    """Manifest that returns a boolean result that is always the same value for a given
    file, regardless of the contents of the row that is being parsed. Can be used for
    checking environmental data in mappings logic.
    """

    ENV_PROPERTY_KEY = "$env"

    value: bool = attr.ib()

    @property
    def result_type(self) -> Type[bool]:
        return bool

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        return {}

    def build_from_row(self, row: Dict[str, str]) -> bool:
        return self.value

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        return []

    def columns_referenced(self) -> Set[str]:
        return set()

    @classmethod
    def for_env_property(
        cls, raw_property_manifest: str, delegate: IngestViewResultsParserDelegate
    ) -> "BooleanLiteralManifest":
        return BooleanLiteralManifest(
            value=delegate.get_env_property(property_name=raw_property_manifest)
        )


@attr.s(kw_only=True)
class BooleanConditionManifest(ManifestNode[ManifestNodeT]):
    """Manifest node that evaluates one of two child manifest nodes based on the result
    of a boolean condition.
    """

    BOOLEAN_CONDITION_KEY = "$conditional"

    # Key for the boolean condition to evaluate.
    IF_CONDITION_ARG_KEY = "$if"

    # Key for the boolean condition to evaluate.
    ELSE_IF_CONDITION_ARG_KEY = "$else_if"

    # Key for the node to evaluate if the condition is True.
    THEN_ARG_KEY = "$then"

    # Key for the node to evaluate if all conditions in a list of conditions are False.
    ELSE_ARG_KEY = "$else"

    condition_manifest: ManifestNode[bool] = attr.ib()
    then_manifest: ManifestNode[ManifestNodeT] = attr.ib()
    else_manifest: Optional[ManifestNode[ManifestNodeT]] = attr.ib()

    @property
    def result_type(self) -> Type[ManifestNodeT]:
        return self.then_manifest.result_type

    def additional_field_manifests(self, field_name: str) -> Dict[str, "ManifestNode"]:
        # Get the additional fields set by each branch
        then_manifests = self.then_manifest.additional_field_manifests(field_name)
        else_manifests = (
            self.else_manifest.additional_field_manifests(field_name)
            if self.else_manifest
            else {}
        )

        additional_manifests: Dict[str, ManifestNode] = {}
        # Zip them together by field name, creating a new condition for each one.
        for additional_field_name in then_manifests.keys() | else_manifests.keys():
            then_manifest = then_manifests.get(additional_field_name)
            else_manifest = else_manifests.get(additional_field_name)

            if then_manifest:
                boolean_manifest = BooleanConditionManifest(
                    condition_manifest=self.condition_manifest,
                    then_manifest=then_manifest,
                    else_manifest=else_manifest,
                )
            elif else_manifest:
                boolean_manifest = BooleanConditionManifest(
                    condition_manifest=InvertConditionManifest(
                        condition_manifest=self.condition_manifest
                    ),
                    then_manifest=else_manifest,
                    else_manifest=then_manifest,
                )
            else:
                raise ValueError(
                    "Expected one of then_manifest and else_manifest to be nonnull."
                )

            additional_manifests[additional_field_name] = boolean_manifest
        return additional_manifests

    def build_from_row(self, row: Dict[str, str]) -> Optional[ManifestNodeT]:
        condition = self.condition_manifest.build_from_row(row)
        if condition is None:
            raise ValueError("Condition manifest should not return None.")

        if condition:
            return self.then_manifest.build_from_row(row)

        if not self.else_manifest:
            return None

        return self.else_manifest.build_from_row(row)

    def child_manifest_nodes(self) -> List["ManifestNode"]:
        manifests: List[ManifestNode] = [self.condition_manifest, self.then_manifest]
        if self.else_manifest:
            manifests.append(self.else_manifest)
        return manifests


class BooleanConditionManifestFactory:
    """Factory class for building BooleanConditionManifests."""

    @classmethod
    def from_raw_manifest(
        cls,
        *,
        raw_condition_manifests: List[YAMLDict],
        delegate: IngestViewResultsParserDelegate,
        variable_manifests: Dict[str, VariableManifestNode],
        expected_result_type: Type[ManifestNodeT],
    ) -> "BooleanConditionManifest[ManifestNodeT]":
        """Builds a BooleanConditionManifest from the provided raw manifest."""

        highest_level_boolean_manifest = None
        else_manifest = None

        # Reverse the order so we can build a nested if/else condition node from the
        # inside out.
        for reverse_index, raw_condition_manifest in enumerate(
            reversed(raw_condition_manifests)
        ):
            index = len(raw_condition_manifests) - reverse_index - 1
            else_manifest_raw = pop_raw_flat_field_manifest_optional(
                BooleanConditionManifest.ELSE_ARG_KEY, raw_condition_manifest
            )
            if else_manifest_raw:
                if reverse_index != 0:
                    raise ValueError(
                        f"Found $else statement in condition [{index}] of the "
                        f"$conditional statement. Only the final condition may have a "
                        f"$else clause."
                    )
                if len(raw_condition_manifests) == 1:
                    raise ValueError(
                        "Found only $else condition in $conditional statement."
                    )
                else_manifest = build_manifest_from_raw_typed(
                    else_manifest_raw,
                    delegate,
                    variable_manifests=variable_manifests,
                    expected_result_type=expected_result_type,
                )
                continue

            if index == 0:
                condition_manifest_raw = pop_raw_flat_field_manifest(
                    BooleanConditionManifest.IF_CONDITION_ARG_KEY,
                    raw_condition_manifest,
                )
            else:
                condition_manifest_raw = pop_raw_flat_field_manifest(
                    BooleanConditionManifest.ELSE_IF_CONDITION_ARG_KEY,
                    raw_condition_manifest,
                )

            condition_manifest = build_manifest_from_raw_typed(
                condition_manifest_raw,
                delegate,
                variable_manifests=variable_manifests,
                expected_result_type=bool,
            )

            then_manifest = build_manifest_from_raw_typed(
                pop_raw_flat_field_manifest(
                    BooleanConditionManifest.THEN_ARG_KEY, raw_condition_manifest
                ),
                delegate,
                variable_manifests=variable_manifests,
                expected_result_type=expected_result_type,
            )
            highest_level_boolean_manifest = BooleanConditionManifest(
                condition_manifest=condition_manifest,
                then_manifest=then_manifest,
                # Set the previous highest level manifest as the else-block, creating a
                # nested if-else chain.
                else_manifest=highest_level_boolean_manifest or else_manifest,
            )

            if len(raw_condition_manifest):
                raise ValueError(
                    f"Found unused keys in boolean condition item [{index}]: "
                    f"{raw_condition_manifest.keys()}"
                )

        if not highest_level_boolean_manifest:
            raise ValueError("Found empty conditions list for $conditional statement.")

        return highest_level_boolean_manifest


def pop_raw_flat_field_manifest_optional(
    field_name: str, raw_parent_manifest: YAMLDict
) -> Optional[Union[str, YAMLDict]]:
    if raw_parent_manifest.peek_optional(field_name, object) is None:
        return None

    return pop_raw_flat_field_manifest(field_name, raw_parent_manifest)


def pop_raw_flat_field_manifest(
    field_name: str, raw_parent_manifest: YAMLDict
) -> Union[str, YAMLDict]:
    raw_field_manifest_type = raw_parent_manifest.peek_type(field_name)
    if raw_field_manifest_type is dict:
        return raw_parent_manifest.pop_dict(field_name)
    if raw_field_manifest_type is str:
        return raw_parent_manifest.pop(field_name, str)
    raise ValueError(
        f"Unexpected field manifest type [{raw_field_manifest_type}] for "
        f"field [{field_name}]."
    )


def build_str_manifest_from_raw(
    raw_field_manifest: Union[str, YAMLDict],
    delegate: IngestViewResultsParserDelegate,
    variable_manifests: Dict[str, VariableManifestNode],
) -> ManifestNode[str]:
    """Builds a ManifestNode[str] from the provided raw manifest."""
    return build_manifest_from_raw_typed(
        raw_field_manifest,
        delegate,
        variable_manifests=variable_manifests,
        expected_result_type=str,
    )


def build_iterable_manifest_from_raw(
    raw_iterable_manifest: Union[str, YAMLDict]
) -> ManifestNode[List[str]]:
    """Builds a ManifestNode[List[str] from the provided raw manifest."""

    if isinstance(raw_iterable_manifest, str):
        return SplitCommaSeparatedListManifest(column_name=raw_iterable_manifest)

    if isinstance(raw_iterable_manifest, YAMLDict):
        manifest_node_name = one(raw_iterable_manifest.keys())

        if manifest_node_name == SplitJSONListManifest.SPLIT_JSON_LIST_KEY:
            return SplitJSONListManifest(
                column_name=raw_iterable_manifest.pop(manifest_node_name, str)
            )

    raise ValueError(
        f"Unexpected raw manifest type: [{type(raw_iterable_manifest)}]: "
        f"{raw_iterable_manifest}"
    )


def build_manifest_from_raw_typed(
    raw_field_manifest: Union[str, YAMLDict],
    delegate: IngestViewResultsParserDelegate,
    variable_manifests: Dict[str, VariableManifestNode],
    expected_result_type: Type[ManifestNodeT],
) -> ManifestNode[ManifestNodeT]:
    manifest = build_manifest_from_raw(
        raw_field_manifest=raw_field_manifest,
        delegate=delegate,
        variable_manifests=variable_manifests,
        expected_result_type=expected_result_type,
    )
    if not issubclass(manifest.result_type, expected_result_type):
        raise ValueError(
            f"Unexpected manifest node type: [{manifest.result_type}]. "
            f"Expected result_type: [{expected_result_type}]."
        )
    return manifest


VARIABLE_EXPRESSION_REGEX = re.compile(r"\$variable\(([a-z][a-z_]+)\)")


def build_manifest_from_raw(
    *,
    raw_field_manifest: Union[str, YAMLDict],
    delegate: IngestViewResultsParserDelegate,
    variable_manifests: Dict[str, VariableManifestNode],
    expected_result_type: Type[ManifestNodeT],
) -> ManifestNode:
    """Builds a ManifestNode from the provided raw manifest."""
    if isinstance(raw_field_manifest, str):
        # If the value in the manifest for this field is a string, it is either
        #  a) A literal string value to hydrate the field with, or
        #  b) The name of a column whose value we should hydrate the field with
        match = re.match(
            StringLiteralFieldManifest.STRING_LITERAL_VALUE_REGEX,
            raw_field_manifest,
        )
        if match:
            return StringLiteralFieldManifest(literal_value=match.group(1))

        match = re.match(
            EnumLiteralFieldManifest.ENUM_LITERAL_VALUE_REGEX,
            raw_field_manifest,
        )
        if match:
            return EnumLiteralFieldManifest.from_raw_manifest(
                raw_manifest=raw_field_manifest, delegate=delegate
            )

        match = re.match(VARIABLE_EXPRESSION_REGEX, raw_field_manifest)
        if match:
            variable_name = match.group(1)
            return variable_manifests[variable_name]

        return DirectMappingFieldManifest(mapped_column=raw_field_manifest)

    if isinstance(raw_field_manifest, YAMLDict):
        manifest_node_name = one(raw_field_manifest.keys())

        if manifest_node_name == EnumMappingManifest.ENUM_MAPPING_KEY:
            return EnumMappingManifest.from_raw_manifest(
                field_enum_mappings_manifest=raw_field_manifest.pop_dict(
                    manifest_node_name
                ),
                delegate=delegate,
                variable_manifests=variable_manifests,
            )

        if manifest_node_name == SerializedJSONDictFieldManifest.JSON_DICT_KEY:
            function_arguments = raw_field_manifest.pop_dict(manifest_node_name)
            return SerializedJSONDictFieldManifest(
                key_to_manifest_map={
                    key: build_str_manifest_from_raw(
                        pop_raw_flat_field_manifest(key, function_arguments),
                        delegate=delegate,
                        variable_manifests=variable_manifests,
                    )
                    for key in function_arguments.keys()
                }
            )
        if manifest_node_name == JSONExtractKeyManifest.JSON_EXTRACT_KEY:
            return JSONExtractKeyManifest.from_raw_manifest(
                raw_function_manifest=raw_field_manifest.pop_dict(manifest_node_name),
                delegate=delegate,
                variable_manifests=variable_manifests,
            )
        if manifest_node_name == ConcatenatedStringsManifest.CONCATENATE_KEY:
            return ConcatenatedStringsManifest.from_raw_manifest(
                raw_function_manifest=raw_field_manifest.pop_dict(manifest_node_name),
                delegate=delegate,
                variable_manifests=variable_manifests,
            )
        if manifest_node_name == PersonNameManifest.PERSON_NAME_KEY:
            return PersonNameManifest.from_raw_manifest(
                raw_function_manifest=raw_field_manifest.pop_dict(manifest_node_name),
                delegate=delegate,
                variable_manifests=variable_manifests,
            )
        if manifest_node_name == PhysicalAddressManifest.PHYSICAL_ADDRESS_KEY:
            return PhysicalAddressManifest.from_raw_manifest(
                raw_function_manifest=raw_field_manifest.pop_dict(manifest_node_name),
                delegate=delegate,
                variable_manifests=variable_manifests,
            )
        if manifest_node_name == BooleanConditionManifest.BOOLEAN_CONDITION_KEY:
            return BooleanConditionManifestFactory.from_raw_manifest(
                raw_condition_manifests=raw_field_manifest.pop_dicts(
                    manifest_node_name
                ),
                delegate=delegate,
                variable_manifests=variable_manifests,
                expected_result_type=expected_result_type,
            )
        if manifest_node_name == CustomFunctionManifest.CUSTOM_FUNCTION_KEY:
            return CustomFunctionManifest.from_raw_manifest(
                raw_function_manifest=raw_field_manifest.pop_dict(manifest_node_name),
                delegate=delegate,
                variable_manifests=variable_manifests,
                expected_return_type=expected_result_type,
            )
        if manifest_node_name == ContainsConditionManifest.IN_CONDITION_KEY:
            return ContainsConditionManifest.from_raw_manifest(
                raw_function_manifest=raw_field_manifest.pop_dict(manifest_node_name),
                delegate=delegate,
                variable_manifests=variable_manifests,
            )
        if manifest_node_name == InvertConditionManifest.NOT_IN_CONDITION_KEY:
            return InvertConditionManifest(
                condition_manifest=ContainsConditionManifest.from_raw_manifest(
                    raw_function_manifest=raw_field_manifest.pop_dict(
                        manifest_node_name
                    ),
                    delegate=delegate,
                    variable_manifests=variable_manifests,
                )
            )
        if manifest_node_name == IsNullConditionManifest.IS_NULL_CONDITION_KEY:
            return IsNullConditionManifest.from_raw_manifest(
                raw_function_manifest=pop_raw_flat_field_manifest(
                    manifest_node_name, raw_field_manifest
                ),
                delegate=delegate,
                variable_manifests=variable_manifests,
            )
        if manifest_node_name == InvertConditionManifest.NOT_NULL_CONDITION_KEY:
            return InvertConditionManifest(
                condition_manifest=IsNullConditionManifest.from_raw_manifest(
                    raw_function_manifest=pop_raw_flat_field_manifest(
                        manifest_node_name, raw_field_manifest
                    ),
                    delegate=delegate,
                    variable_manifests=variable_manifests,
                )
            )
        if manifest_node_name == AndConditionManifest.AND_CONDITION_KEY:
            return AndConditionManifest.from_raw_manifest(
                raw_condition_manifests=pop_raw_manifest_nodes_list(
                    raw_field_manifest, manifest_node_name
                ),
                delegate=delegate,
                variable_manifests=variable_manifests,
            )
        if manifest_node_name == OrConditionManifest.OR_CONDITION_KEY:
            return OrConditionManifest.from_raw_manifest(
                raw_condition_manifests=pop_raw_manifest_nodes_list(
                    raw_field_manifest, manifest_node_name
                ),
                delegate=delegate,
                variable_manifests=variable_manifests,
            )
        if manifest_node_name == EqualsConditionManifest.EQUALS_CONDITION_KEY:
            return EqualsConditionManifest.from_raw_manifest(
                raw_value_manifests=pop_raw_manifest_nodes_list(
                    raw_field_manifest, manifest_node_name
                ),
                delegate=delegate,
                variable_manifests=variable_manifests,
            )
        if manifest_node_name == BooleanLiteralManifest.ENV_PROPERTY_KEY:
            return BooleanLiteralManifest.for_env_property(
                raw_property_manifest=raw_field_manifest.pop(
                    BooleanLiteralManifest.ENV_PROPERTY_KEY, str
                ),
                delegate=delegate,
            )
        if manifest_node_name == ExpandableListItemManifest.FOREACH_KEY:
            return ExpandableListItemManifest.from_raw_manifest(
                raw_function_manifest=raw_field_manifest.pop_dict(manifest_node_name),
                delegate=delegate,
                variable_manifests=variable_manifests,
            )
        if issubclass(expected_result_type, Entity):
            entity_cls = delegate.get_entity_cls(entity_cls_name=manifest_node_name)
            return EntityTreeManifestFactory.from_raw_manifest(
                raw_fields_manifest=raw_field_manifest.pop_dict(manifest_node_name),
                delegate=delegate,
                variable_manifests=variable_manifests,
                entity_cls=entity_cls,
            )
        raise ValueError(
            f"Unexpected manifest name [{manifest_node_name}]: [{raw_field_manifest}]"
        )

    raise ValueError(
        f"Unexpected manifest type: [{type(raw_field_manifest)}]: {raw_field_manifest}"
    )


def pop_raw_manifest_nodes_list(
    parent_raw_manifest: YAMLDict, list_field_name: str
) -> List[Union[str, YAMLDict]]:
    manifests: List[Union[str, YAMLDict]] = []
    for raw_manifest in parent_raw_manifest.pop(list_field_name, list):
        if isinstance(raw_manifest, str):
            manifests.append(raw_manifest)
        elif isinstance(raw_manifest, dict):
            manifests.append(YAMLDict(raw_manifest))
        else:
            raise ValueError(
                f"Unexpected raw manifest type in list: [{type(raw_manifest)}]"
            )
    return manifests
