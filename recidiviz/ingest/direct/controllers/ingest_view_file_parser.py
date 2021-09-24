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
"""Class that parses ingest view file contents into entities based on the manifest file
 for this ingest view.
"""
import abc
import csv
import os
from enum import Enum, auto
from types import ModuleType
from typing import Callable, Dict, Iterator, List, Optional, Set, Tuple, Type, Union

import attr
from more_itertools import one

from recidiviz.cloud_storage.gcs_file_system import GcsfsFileContentsHandle
from recidiviz.common.attr_mixins import (
    BuildableAttrFieldType,
    attr_field_enum_cls_for_field_name,
    attr_field_type_for_field_name,
)
from recidiviz.common.common_utils import bidirectional_set_difference
from recidiviz.common.constants.enum_parser import EnumParser
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.ingest.direct.controllers.ingest_view_manifest import (
    CustomFunctionRegistry,
    EntityTreeManifest,
    EnumFieldManifest,
    ExpandableListItemManifest,
    ListRelationshipFieldManifest,
    ManifestNode,
    build_manifest_from_raw,
    pop_raw_flat_field_manifest,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.base_entity import Entity, EnumEntity
from recidiviz.persistence.entity.entity_deserialize import EntityFactory, EntityT
from recidiviz.persistence.entity.entity_utils import (
    get_entity_class_in_module_with_name,
)
from recidiviz.persistence.entity.state import (
    deserialize_entity_factories as state_deserialize_entity_factories,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.utils.regions import Region
from recidiviz.utils.yaml_dict import YAMLDict


class FileFormat(Enum):
    CSV = auto()


def ingest_view_manifest_dir(region: Region) -> str:
    """Returns the directory where all ingest view manifests for a given region live."""
    # TODO(#9059): Move ingest view manifests out of top level region dir
    return os.path.join(
        os.path.dirname(region.region_module.__file__),
        region.region_code.lower(),
    )


def yaml_mappings_filepath(region: Region, file_tag: str) -> str:
    return os.path.join(
        ingest_view_manifest_dir(region),
        f"{region.region_code.lower()}_{file_tag}.yaml",
    )


# This key tracks the version number for the actual mappings manifest structure,
# allowing us to gate any breaking changes in the file syntax etc.
MANIFEST_LANGUAGE_VERSION_KEY = "manifest_language"

# Minimum supported value in MANIFEST_LANGUAGE_VERSION_KEY field
MIN_LANGUAGE_VERSION = "1.0.0"

# Maximum supported value in MANIFEST_LANGUAGE_VERSION_KEY field
MAX_LANGUAGE_VERSION = "1.0.0"


class IngestViewFileParserDelegate:
    @abc.abstractmethod
    def get_ingest_view_manifest_path(self, file_tag: str) -> str:
        """Returns the path to the ingest view manifest for a given file tag."""

    @abc.abstractmethod
    def get_common_args(self) -> Dict[str, Optional[Union[str, EnumParser]]]:
        """Returns a dictionary containing any fields, with their corresponding values,
        that should be set on every entity produced by the parser.
        """

    @abc.abstractmethod
    def get_entity_factory_class(self, entity_cls_name: str) -> Type[EntityFactory]:
        """Returns the factory class that can be used to instantiate an entity with the
        provided class name.
        """

    @abc.abstractmethod
    def get_entity_cls(self, entity_cls_name: str) -> Type[Entity]:
        """Returns the class for a given entity name"""

    @abc.abstractmethod
    def get_custom_function_registry(self) -> CustomFunctionRegistry:
        """Returns an object that gives the parser access to custom python functions
        that can be used for parsing.
        """


class IngestViewFileParserDelegateImpl(
    IngestViewFileParserDelegate, ModuleCollectorMixin
):
    """Standard implementation of the IngestViewFileParserDelegate, for use in
    production code.
    """

    def __init__(self, region: Region, schema_type: SchemaType) -> None:
        self.region = region
        self.schema_type = schema_type
        self.entity_cls_cache: Dict[str, Type[Entity]] = {}

    def get_ingest_view_manifest_path(self, file_tag: str) -> str:
        return yaml_mappings_filepath(self.region, file_tag)

    def get_common_args(self) -> Dict[str, Optional[Union[str, EnumParser]]]:
        if self.schema_type == SchemaType.STATE:
            # All entities in the state schema have the state_code field - we add this
            # as a common argument so we don't have to specify it in the yaml mappings.
            return {"state_code": self.region.region_code}
        if self.schema_type == SchemaType.JAILS:
            return {}
        raise ValueError(f"Unexpected schema type [{self.schema_type}]")

    def _get_deserialize_factories_module(self) -> ModuleType:
        if self.schema_type == SchemaType.STATE:
            return state_deserialize_entity_factories
        raise ValueError(f"Unexpected schema type [{self.schema_type}]")

    def _get_entities_module(self) -> ModuleType:
        if self.schema_type == SchemaType.STATE:
            return state_entities
        raise ValueError(f"Unexpected schema type [{self.schema_type}]")

    def get_entity_factory_class(self, entity_cls_name: str) -> Type[EntityFactory]:
        factory_entity_name = f"{entity_cls_name}Factory"
        factory_cls = getattr(
            self._get_deserialize_factories_module(), factory_entity_name
        )
        return factory_cls

    def get_entity_cls(self, entity_cls_name: str) -> Type[Entity]:
        if entity_cls_name in self.entity_cls_cache:
            return self.entity_cls_cache[entity_cls_name]

        entity_cls = get_entity_class_in_module_with_name(
            self._get_entities_module(), entity_cls_name
        )
        self.entity_cls_cache[entity_cls_name] = entity_cls
        return entity_cls

    def get_custom_function_registry(self) -> CustomFunctionRegistry:
        region_code = self.region.region_code.lower()
        return CustomFunctionRegistry(
            custom_functions_root_module=self.get_relative_module(
                self.region.region_module, [region_code]
            )
        )


# TODO(#8980): Add support for (limited) custom python.
class IngestViewFileParser:
    """Class that parses ingest view file contents into entities based on the manifest
    file for this ingest view.
    """

    def __init__(self, delegate: IngestViewFileParserDelegate):
        self.delegate = delegate

    @staticmethod
    def _row_iterator(
        contents_handle: GcsfsFileContentsHandle, file_format: FileFormat
    ) -> Iterator[Dict]:
        if file_format == FileFormat.CSV:
            return csv.DictReader(contents_handle.get_contents_iterator())
        raise ValueError(f"Unsupported file format: [{file_format}].")

    def parse(
        self,
        *,
        file_tag: str,
        contents_handle: GcsfsFileContentsHandle,
        file_format: FileFormat,
    ) -> List[Entity]:
        """Parses ingest view file contents into entities based on the manifest file for
        this ingest view.
        """

        manifest_path = self.delegate.get_ingest_view_manifest_path(file_tag)
        output_manifest, expected_input_columns = self.parse_manifest(manifest_path)
        result = []
        for i, row in enumerate(self._row_iterator(contents_handle, file_format)):
            self._validate_row_columns(i, row, expected_input_columns)
            output_tree = output_manifest.build_from_row(row)
            if not output_tree:
                raise ValueError("Unexpected null output tree for row.")
            result.append(output_tree)
        return result

    @staticmethod
    def _validate_row_columns(
        row_number: int, row: Dict[str, str], expected_columns: Set[str]
    ) -> None:
        """Checks that columns in the row match the set of expected columns. Throws if
        there are missing or extra columns.
        """
        input_columns = set(row.keys())
        missing_from_manifest, missing_from_file = bidirectional_set_difference(
            input_columns, expected_columns
        )
        if missing_from_manifest:
            raise ValueError(
                f"Found columns in input file row [{row_number}] not present in manifest "
                f"|input_columns| list: {missing_from_manifest}"
            )
        if missing_from_file:
            raise ValueError(
                f"Found columns in manifest |input_columns| list that are missing from "
                f"file row [{row_number}]: {missing_from_file}"
            )

    def parse_manifest(self, manifest_path: str) -> Tuple[EntityTreeManifest, Set[str]]:
        manifest_dict = YAMLDict.from_path(manifest_path)

        version = manifest_dict.pop(MANIFEST_LANGUAGE_VERSION_KEY, str)
        if not MIN_LANGUAGE_VERSION <= version <= MAX_LANGUAGE_VERSION:
            raise ValueError(f"Unsupported language version: [{version}]")

        # TODO(#8981): Add logic to enforce that version changes are accompanied with
        #  proper migrations / reruns.
        input_columns = manifest_dict.pop("input_columns", list)
        unused_columns = manifest_dict.pop("unused_columns", list)

        output_manifest = self._build_entity_tree_manifest(
            raw_entity_manifest=manifest_dict.pop_dict("output")
        )

        if len(manifest_dict):
            raise ValueError(
                f"Found unused keys in ingest view manifest: {manifest_dict.keys()}"
            )

        self._validate_input_columns_lists(
            input_columns_list=input_columns,
            unused_columns_list=unused_columns,
            referenced_columns=output_manifest.columns_referenced(),
        )

        return output_manifest, set(input_columns)

    @staticmethod
    def _validate_input_columns_lists(
        input_columns_list: List[str],
        unused_columns_list: List[str],
        referenced_columns: Set[str],
    ) -> None:
        """Validates that the |input_columns| and |unused_columns| manifests lists
        conform to expected structure and contain exactly the set of columns that are
        referenced in the |output| section of the manifest.
        """
        input_columns = set()
        for input_col in input_columns_list:
            if input_col in input_columns:
                raise ValueError(
                    f"Found item listed multiple times in |input_columns|: [{input_col}]"
                )
            input_columns.add(input_col)

        unused_columns = set()
        for unused_col in unused_columns_list:
            if unused_col in unused_columns:
                raise ValueError(
                    f"Found item listed multiple times in |unused_columns|: [{unused_col}]"
                )
            unused_columns.add(unused_col)

        for column in input_columns:
            if column.startswith("$"):
                raise ValueError(
                    f"Found column [{column}] that starts with protected "
                    f"character '$'. Adjust ingest view output column "
                    f"naming to remove the '$'."
                )

        (
            expected_referenced_columns,
            unexpected_unused_columns,
        ) = bidirectional_set_difference(input_columns, unused_columns)

        if unexpected_unused_columns:
            raise ValueError(
                f"Found values listed in |unused_columns| that were not also listed in "
                f"|input_columns|: {unexpected_unused_columns}"
            )

        (
            unlisted_referenced_columns,
            unreferenced_columns,
        ) = bidirectional_set_difference(
            referenced_columns, expected_referenced_columns
        )

        if unlisted_referenced_columns:
            raise ValueError(
                f"Found columns referenced in |output| that are not listed in "
                f"|input_columns|: {unlisted_referenced_columns}"
            )

        if unreferenced_columns:
            raise ValueError(
                f"Found columns listed in |input_columns| that are not referenced "
                f"in |output| or listed in |unused_columns|: "
                f"{unreferenced_columns}"
            )

    def _build_entity_tree_manifest(
        self, *, raw_entity_manifest: YAMLDict
    ) -> EntityTreeManifest:
        """Returns a single, recursively hydrated entity tree manifest, which can be
        used to translate a single input row into an entity tree.
        """
        entity_cls_name = one(raw_entity_manifest.keys())
        entity_cls = self.delegate.get_entity_cls(entity_cls_name)

        raw_fields_manifest = raw_entity_manifest.pop_dict(entity_cls_name)

        if len(raw_entity_manifest):
            raise ValueError(
                f"Found unused keys in entity manifest: {raw_entity_manifest.keys()}"
            )

        field_manifests: Dict[str, ManifestNode] = {}
        for field_name in raw_fields_manifest.keys():
            field_type = attr_field_type_for_field_name(entity_cls, field_name)
            if field_name == entity_cls.get_primary_key_column_name():
                error_message = (
                    f"Cannot set autogenerated database primary key field "
                    f"[{field_name}] in the ingest manifest."
                )
                if "external_id" in attr.fields_dict(entity_cls):
                    error_message += " Did you mean to set the 'external_id' field?"
                raise ValueError(error_message)

            if field_type is BuildableAttrFieldType.LIST:
                child_manifests: List[
                    Union[ExpandableListItemManifest, EntityTreeManifest]
                ] = []
                for raw_child_manifest in raw_fields_manifest.pop_dicts(field_name):
                    child_manifests.append(
                        self._build_list_item_manifest(
                            list_item_manifest_raw=raw_child_manifest
                        )
                    )
                field_manifests[field_name] = ListRelationshipFieldManifest(
                    child_manifests=child_manifests
                )
            elif field_type is BuildableAttrFieldType.FORWARD_REF:
                field_manifests[field_name] = self._build_entity_tree_manifest(
                    raw_entity_manifest=raw_fields_manifest.pop_dict(field_name)
                )
            elif field_type is BuildableAttrFieldType.ENUM:
                field_manifests.update(
                    self._build_enum_field_manifests_dict(
                        entity_cls=entity_cls,
                        field_name=field_name,
                        field_enum_mappings_manifest=raw_fields_manifest.pop_dict(
                            field_name
                        ),
                    )
                )
            elif field_type in (
                # These are flat fields and should be parsed into a ManifestNode[str],
                # since all values will be converted from string -> real value in the
                # deserializing entity factory.
                BuildableAttrFieldType.BOOLEAN,
                BuildableAttrFieldType.DATE,
                BuildableAttrFieldType.STRING,
            ):
                if field_name.endswith(EnumEntity.RAW_TEXT_FIELD_SUFFIX):
                    raise ValueError(
                        f"Enum raw text fields should not be mapped independently "
                        f"of their corresponding enum fields. Found direct mapping "
                        f"for field [{field_name}]."
                    )
                field_manifests[field_name] = build_manifest_from_raw(
                    pop_raw_flat_field_manifest(field_name, raw_fields_manifest)
                )
            else:
                raise ValueError(
                    f"Unexpected field type [{field_type}] for field [{field_name}]"
                )

        if len(raw_fields_manifest):
            raise ValueError(
                f"Found unused keys in fields manifest: {raw_fields_manifest.keys()}"
            )

        entity_factory_cls = self.delegate.get_entity_factory_class(entity_cls_name)
        return EntityTreeManifest(
            entity_cls=entity_cls,
            entity_factory_cls=entity_factory_cls,
            common_args=self.delegate.get_common_args(),
            field_manifests=field_manifests,
            filter_predicate=self._get_filter_predicate(entity_cls, field_manifests),
        )

    # TODO(##9099): Make sure to add documentation about what gets filtered out.
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
                if isinstance(manifest, EnumFieldManifest)
            )
            enum_raw_field_name = EnumFieldManifest.raw_text_field_name(enum_field_name)

            def enum_entity_filter_predicate(e: EntityT) -> bool:
                return (
                    getattr(e, enum_field_name) is None
                    or getattr(e, enum_raw_field_name) is None
                )

            return enum_entity_filter_predicate
        return None

    def _build_enum_field_manifests_dict(
        self,
        entity_cls: Type[Entity],
        field_name: str,
        field_enum_mappings_manifest: YAMLDict,
    ) -> Dict[str, ManifestNode]:
        """Returns a dictionary (field_name -> FieldManifest) for this enum field and
        its associated raw text field.
        """
        field_manifests: Dict[str, ManifestNode] = {}
        enum_cls = attr_field_enum_cls_for_field_name(entity_cls, field_name)
        if not enum_cls:
            raise ValueError(
                f"No enum class for field [{field_name}] in class " f"[{entity_cls}]."
            )
        enum_field_manifest = EnumFieldManifest.from_raw_manifest(
            enum_cls=enum_cls,
            field_enum_mappings_manifest=field_enum_mappings_manifest,
            fn_registry=self.delegate.get_custom_function_registry(),
        )
        field_manifests[field_name] = enum_field_manifest
        field_manifests[
            EnumFieldManifest.raw_text_field_name(field_name)
        ] = enum_field_manifest.raw_text_field_manifest
        return field_manifests

    def _build_list_item_manifest(
        self,
        list_item_manifest_raw: YAMLDict,
    ) -> Union[EntityTreeManifest, ExpandableListItemManifest]:
        """Expands a list item into a list of entities based on the provided manifest."""
        list_col_name = list_item_manifest_raw.pop_optional(
            ExpandableListItemManifest.FOREACH_ITERATOR_KEY, str
        )

        child_entity_manifest = self._build_entity_tree_manifest(
            raw_entity_manifest=list_item_manifest_raw
        )

        # Check for special $foreach keyword arg which tells us that we should expand
        # this list item into N entities.
        #
        # Aside from the special $foreach key, this manifest should be shaped like a
        # normal entity tree manifest, a dictionary with a single class name -> fields
        # manifest mapping.
        if not list_col_name:
            return child_entity_manifest

        return ExpandableListItemManifest(
            mapped_column=list_col_name,
            child_entity_manifest=child_entity_manifest,
        )
