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
import re
from enum import Enum, auto
from types import ModuleType
from typing import Any, Dict, Iterator, List, Type, Union

from more_itertools import one

from recidiviz.cloud_storage.gcs_file_system import GcsfsFileContentsHandle
from recidiviz.common.constants.enum_parser import EnumParser
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_deserialize import EntityFactory
from recidiviz.persistence.entity.state import (
    deserialize_entity_factories as state_deserialize_entity_factories,
)
from recidiviz.utils.regions import Region
from recidiviz.utils.yaml_dict import YAMLDict


class FileFormat(Enum):
    CSV = auto()


def yaml_mappings_filepath(region: Region, file_tag: str) -> str:
    return os.path.join(
        os.path.dirname(region.region_module.__file__),
        region.region_code.lower(),
        f"{region.region_code.lower()}_{file_tag}.yaml",
    )


# This key tracks the version number for the actual mappings manifest structure,
# allowing us to gate any breaking changes in the file syntax etc.
MANIFEST_LANGUAGE_VERSION_KEY = "manifest_language"

# Key that denotes that a list item should be expanded into N values, one for each item
# in the list column.
FOREACH_ITERATOR_KEY = "$foreach"

# Variable "column name" hydrated with a single list item value. Can only be used within
# the context of a $foreach loop.
FOREACH_LOOP_VALUE_NAME = "$iter"

# Default delimiter used to split list column values
DEFAULT_LIST_VALUE_DELIMITER = ","

# Minimum supported value in MANIFEST_LANGUAGE_VERSION_KEY field
MIN_LANGUAGE_VERSION = "1.0.0"

# Maximum supported value in MANIFEST_LANGUAGE_VERSION_KEY field
MAX_LANGUAGE_VERSION = "1.0.0"

# String literals are denoted like \"MY_STR"
STRING_LITERAL_VALUE_REGEX = re.compile(r"^\\\"(.+)\"$")


class IngestViewFileParserDelegate:
    @abc.abstractmethod
    def get_ingest_view_manifest_path(self, file_tag: str) -> str:
        """Returns the path to the ingest view manifest for a given file tag."""

    @abc.abstractmethod
    def get_common_args(self) -> Dict[str, Union[str, EnumParser]]:
        """Returns a dictionary containing any fields, with their corresponding values,
        that should be set on every entity produced by the parser.
        """

    @abc.abstractmethod
    def get_entity_factory_class(self, entity_cls_name: str) -> Type[EntityFactory]:
        """Returns the factory class that can be used to instantiate an entity with the
        provided class name.
        """


class IngestViewFileParserDelegateImpl(IngestViewFileParserDelegate):
    """Standard implementation of the IngestViewFileParserDelegate, for use in
    production code.
    """

    def __init__(self, region: Region, schema_type: SchemaType) -> None:
        self.region = region
        self.schema_type = schema_type

    def get_ingest_view_manifest_path(self, file_tag: str) -> str:
        return yaml_mappings_filepath(self.region, file_tag)

    def get_common_args(self) -> Dict[str, Union[str, EnumParser]]:
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

    def get_entity_factory_class(self, entity_cls_name: str) -> Type[EntityFactory]:
        factory_entity_name = f"{entity_cls_name}Factory"
        factory_cls = getattr(
            self._get_deserialize_factories_module(), factory_entity_name
        )
        return factory_cls


# TODO(#8977): Add enum mappings support.
# TODO(#8979): Add support for building fields from concatenated columns.
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
        manifest_dict = YAMLDict.from_path(
            self.delegate.get_ingest_view_manifest_path(file_tag)
        )

        version = manifest_dict.pop(MANIFEST_LANGUAGE_VERSION_KEY, str)
        if not MIN_LANGUAGE_VERSION <= version <= MAX_LANGUAGE_VERSION:
            raise ValueError(f"Unsupported language version: [{version}]")

        # TODO(#8981): Add logic to enforce that version changes are accompanied with
        #  proper migrations / reruns.
        # TODO(#8957): Check that input + unused columns match what is in input file and
        #  match what is actually used.
        # TODO(#8957): Check that input columns do not start with $ (protected char)
        _ = manifest_dict.pop("input_columns", list)
        _ = manifest_dict.pop("unused_columns", list)
        row_output_manifest = manifest_dict.pop_dict("output")
        result: List[Entity] = []
        for row in self._row_iterator(contents_handle, file_format):
            entity = self._parse_entity_tree(
                entity_manifest=row_output_manifest.copy(),
                row=row,
            )
            result.append(entity)

        if len(manifest_dict):
            raise ValueError(
                f"Found unused keys in ingest view manifest: {manifest_dict.keys()}"
            )
        return result

    def _parse_entity_tree(
        self,
        *,
        entity_manifest: YAMLDict,
        row: Dict[str, str],
    ) -> Entity:
        """Returns a single, recursively hydrated entity. The entity is hydrated
        from the values in |row|, based on the provided entity class -> field manifest
        mappings dict.
        """
        entity_cls_name = one(entity_manifest.keys())

        fields_manifest = entity_manifest.pop_dict(entity_cls_name)

        if len(entity_manifest):
            raise ValueError(
                f"Found unused keys in entity manifest: {entity_manifest.keys()}"
            )

        args: Dict[str, Any] = self.delegate.get_common_args()
        for field_name in fields_manifest.keys():
            field_type = fields_manifest.peek_type(field_name)
            field_value: Any
            if field_type is list:
                field_value = []
                for list_item_manifest in fields_manifest.pop_dicts(field_name):
                    field_value.extend(
                        self._parse_entities_list_from_dict(
                            list_item_manifest=list_item_manifest,
                            row=row,
                        )
                    )
            elif field_type is dict:
                dict_field_manifest = fields_manifest.pop_dict(field_name)
                field_value = self._parse_entity_tree(
                    entity_manifest=dict_field_manifest,
                    row=row,
                )
            elif field_type is str:
                str_field_manifest = fields_manifest.pop(field_name, str)
                # If the value in the manifest for this field is a string, it is either
                #  a) A literal string value to hydrate the field with, or
                #  b) The name of a column whose value we should hydrate the field with
                match = re.match(STRING_LITERAL_VALUE_REGEX, str_field_manifest)
                if match:
                    field_value = match.group(1)
                else:
                    field_value = row[str_field_manifest]
            else:
                raise ValueError(
                    f"Unexpected field manifest type [{field_type}] for "
                    f"field [{field_name}]."
                )
            args[field_name] = field_value

        if len(fields_manifest):
            raise ValueError(
                f"Found unused keys in fields manifest: {fields_manifest.keys()}"
            )

        entity_factory_cls = self.delegate.get_entity_factory_class(entity_cls_name)
        return entity_factory_cls.deserialize(**args)

    def _parse_entities_list_from_dict(
        self, list_item_manifest: YAMLDict, row: Dict[str, str]
    ) -> List[Entity]:
        """Expands a list item into a list of entities based on the provided manifest."""
        list_col_name = list_item_manifest.pop_optional(FOREACH_ITERATOR_KEY, str)

        # Check for special $foreach keyword arg which tells us that we should expand
        # this list item into N entities. If not present, just return a single item
        # in a list.
        #
        # Aside from the special $foreach key, this manifest should be shaped like a
        # normal entity tree manifest, a dictionary with a single class name -> fields
        # manifest mapping.
        if not list_col_name:
            return [
                self._parse_entity_tree(
                    entity_manifest=list_item_manifest,
                    row=row,
                )
            ]

        list_col_value = row[list_col_name]
        if not list_col_value:
            return []

        # TODO(#8908): For now, we always split list column values on the default
        #  delimiter. Revisit whether the parser language needs be changed to allow the
        #  delimiter to be configurable.
        values = list_col_value.split(DEFAULT_LIST_VALUE_DELIMITER)
        result = []
        if FOREACH_LOOP_VALUE_NAME in row:
            raise ValueError(
                f"Unexpected {FOREACH_LOOP_VALUE_NAME} key value in row: {row}. Nested "
                f"loops not supported."
            )
        for value in values:
            row[FOREACH_LOOP_VALUE_NAME] = value
            entity = self._parse_entity_tree(
                entity_manifest=list_item_manifest.copy(),
                row=row,
            )
            del row[FOREACH_LOOP_VALUE_NAME]
            result.append(entity)
        return result
