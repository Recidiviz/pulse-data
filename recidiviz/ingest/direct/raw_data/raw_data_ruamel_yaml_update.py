#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Module to manage and upgrade YAML configurations, preserving original formatting and comments"""
from enum import Enum
from functools import partial
from pathlib import Path
from typing import Any, Type

import attr
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.representer import RoundTripRepresenter

from recidiviz.common.attr_utils import (
    get_inner_type_from_list_type,
    get_inner_type_from_optional_type,
    is_list_type,
    is_optional_type,
)
from recidiviz.common.constants.encoding import UTF_8
from recidiviz.ingest.direct.raw_data.raw_file_config_utils import (
    LIST_ITEM_IDENTIFIER_TAG,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRawFileDefaultConfig,
)
from recidiviz.tools.ingest.development.raw_data_yaml_utils import (
    raw_data_yaml_attribute_filter,
)

COLUMNS_ATTRIBUTE_NAME = "_columns"
COLUMNS_YAML_KEY = COLUMNS_ATTRIBUTE_NAME.lstrip("_")


def load_ruaml_yaml(file_path: str | Path) -> CommentedMap:
    """loads yaml object based on file path"""
    yaml = YAML()
    yaml.preserve_quotes = True
    file_path = Path(file_path)
    with file_path.open("r", encoding="utf-8") as f:
        return yaml.load(f)


def yaml_value_serializer(_instance: Any, _attribute: Any, value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: yaml_value_serializer(None, None, value)
            for key, value in value.items()
        }
    if isinstance(value, list):
        return [yaml_value_serializer(None, None, item) for item in value]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, tuple):
        raise RuntimeError("tuple not supported")
    if isinstance(value, set):
        raise RuntimeError("set not supported")
    return value


def write_schema_pragma_to_yaml_if_not_present(file_path: Path) -> None:
    """Writes the YAML schema pragma to the top of the specified file if not present."""
    schema_pragma = (
        "# yaml-language-server: $schema=./../../../raw_data/yaml_schema/schema.json\n"
    )

    with file_path.open("r", encoding=UTF_8) as f:
        first_line = f.readline()
        if first_line == schema_pragma:
            return
        # If schema pragma not present, read the rest of the file
        rest = f.read()

    # Rewrite file with pragma prepended
    file_path.write_text(schema_pragma + first_line + rest, encoding=UTF_8)


def write_ruamel_yaml_to_file(file_path: Path, data: CommentedMap) -> None:
    """Saves a YAML configuration object back to a file."""
    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.preserve_quotes = True
    yaml.width = 4096  # Set a large width to avoid line breaks in the YAML output

    # Add custom bool representer to ensure booleans are capitalized
    # to match existing style in our raw data YAML files
    def bool_representer(representer: Any, data: Any) -> Any:
        return representer.represent_scalar(
            "tag:yaml.org,2002:bool", "True" if data else "False"
        )

    yaml.Representer.add_representer(bool, bool_representer)

    file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as f:
        yaml.dump(data, f)

    write_schema_pragma_to_yaml_if_not_present(file_path)

    # Reset bool representer because during the pytest suite state is sometimes being
    # carried over to other tests causing unexpected behavior
    yaml.representer.add_representer(bool, RoundTripRepresenter.represent_bool)


def convert_raw_file_config_to_dict(
    file_config: DirectIngestRawFileConfig,
    default_region_config: DirectIngestRawFileDefaultConfig,
) -> dict[str, Any]:
    """Converts a DirectIngestRawFileConfig object to a dictionary for YAML serialization by filtering out default values"""
    filtered = attr.asdict(
        file_config,
        recurse=True,
        filter=partial(
            raw_data_yaml_attribute_filter, default_region_config=default_region_config
        ),
        value_serializer=yaml_value_serializer,
    )

    # _columns attribute is private to avoid accidentally referencing deleted columns
    # but all columns including deleted columns are found in the raw file config yaml
    # with the key `columns`, so the key needs to be updated here
    filtered[COLUMNS_YAML_KEY] = filtered.pop(COLUMNS_ATTRIBUTE_NAME, [])

    return filtered


def get_identifier_field_name(cls: Type) -> str | None:
    """Find the name of the field in an attr defined class marked with list_item_identifier=True."""
    if not attr.has(cls):
        raise ValueError(f"Class [{cls}] is not an attr defined class")
    # TODO(#45340) Enforce that there is exactly one unique identifier field once we have
    # reworked RawTableRelationshipInfo to have a unique identifier
    for field in attr.fields(cls):
        if field.metadata.get(LIST_ITEM_IDENTIFIER_TAG):
            return field.name

    return None


def collect_identifier_attr_map(
    cls: Type, seen: set[Type] | None = None
) -> dict[str, str]:
    """
    Recursively collect a map {parent_attr_name: child_identifier_field_name}
    for all attrs classes reachable from `cls`.
    """
    if seen is None:
        seen = set()
    if cls in seen:
        return {}
    seen.add(cls)

    identifier_map: dict[str, str] = {}

    if not attr.has(cls):
        return identifier_map

    for field in attr.fields(cls):
        field_type = field.type

        referenced_class = None
        if is_list_type(field_type):
            referenced_class = get_inner_type_from_list_type(field_type)
        if is_optional_type(field_type):
            field_type = get_inner_type_from_optional_type(field_type)
            if is_list_type(field_type):
                referenced_class = get_inner_type_from_list_type(field_type)

        # Check if field is a collection of an attrs class
        if referenced_class and attr.has(referenced_class):
            ident_field = get_identifier_field_name(referenced_class)
            if ident_field:
                identifier_map[field.name] = ident_field
            identifier_map.update(collect_identifier_attr_map(referenced_class, seen))

        # Check if field is a nested attrs class
        elif attr.has(field_type):
            ident_field = get_identifier_field_name(field_type)
            if ident_field:
                identifier_map[field.name] = ident_field
            identifier_map.update(collect_identifier_attr_map(field_type, seen))

    return identifier_map


def get_raw_file_config_attribute_to_list_item_identifier_map() -> dict[str, str]:
    """Returns a map of attribute names to their unique identifier field names for list items."""
    attribute_to_list_item_identifier = collect_identifier_attr_map(
        DirectIngestRawFileConfig
    )
    attribute_to_list_item_identifier[
        COLUMNS_YAML_KEY
    ] = attribute_to_list_item_identifier.pop(COLUMNS_ATTRIBUTE_NAME)
    return attribute_to_list_item_identifier


def update_ruamel_dict(
    ruamel_data: dict[str, Any],
    original_dict: dict[str, Any],
    updated_dict: dict[str, Any],
    identifier_map: dict[str, str],
) -> None:
    """Recursively updates a ruamel dict in place by comparing changes between updated_dict and original_dict"""

    for key, updated_val in updated_dict.items():
        original_val = original_dict.get(key)
        if updated_val == original_val:
            continue
        if isinstance(updated_val, dict) and isinstance(original_val, dict):
            update_ruamel_dict(
                ruamel_data[key], original_val, updated_val, identifier_map
            )
        elif isinstance(updated_val, list) and isinstance(original_val, list):
            update_ruamel_list(
                attribute_name=key,
                ruamel_list=ruamel_data[key],
                original_list=original_val,
                updated_list=updated_val,
                identifier_map=identifier_map,
            )
        else:
            ruamel_data[key] = updated_val
    for key in original_dict:
        if key not in updated_dict:
            ruamel_data.pop(key, None)


def update_ruamel_list(
    attribute_name: str,
    ruamel_list: list[Any],
    original_list: list[Any],
    updated_list: list[Any],
    identifier_map: dict[str, str],
) -> None:
    """Recursively updates a ruamel YAML list in place to match updated_list."""
    unique_identifier_key = identifier_map.get(attribute_name)

    def get_identifier(item: Any) -> Any:
        """Extract unique identifier from an item."""
        if isinstance(item, dict):
            return item.get(unique_identifier_key)
        return item

    def items_by_identifier(items: list[Any]) -> dict[Any, Any]:
        """Create a mapping of identifier -> item."""
        return {get_identifier(item): item for item in items}

    def find_insertion_position(
        updated_list: list[Any],
        current_index: int,
        existing_identifiers: list[str],
    ) -> int:
        """Find the correct position to insert a new item to maintain order."""
        # Look backwards for the last item that exists in the current list
        for j in range(current_index - 1, -1, -1):
            prev_ident = get_identifier(updated_list[j])
            if prev_ident in existing_identifiers:
                return existing_identifiers.index(prev_ident) + 1

        # If no previous item found, insert at the beginning
        return 0

    original_items = items_by_identifier(original_list)
    updated_items = items_by_identifier(updated_list)

    # Leave ruamel_list as is if lists are equivalent (same items, potentially different order)
    if sorted(original_items.items()) == sorted(updated_items.items()):
        return

    # Step 1: Update or remove existing items
    idents_to_remove = []
    for idx, ruamel_item in enumerate(ruamel_list):
        ident = get_identifier(ruamel_item)

        if ident in updated_items:
            # Item exists in updated list - check if it needs updating
            updated_item = updated_items[ident]
            original_item = original_items[ident]

            if updated_item != original_item:
                if isinstance(ruamel_item, list) and isinstance(updated_item, list):
                    raise NotImplementedError(
                        "Updating lists of lists within ruamel YAML is not implemented."
                    )
                if isinstance(ruamel_item, dict) and isinstance(updated_item, dict):
                    update_ruamel_dict(
                        ruamel_item, original_item, updated_item, identifier_map
                    )
                else:
                    ruamel_list[idx] = updated_item
        else:
            # Item was removed in updated list
            idents_to_remove.append(ident)

    # Remove deleted items
    ruamel_list[:] = [
        item
        for item in ruamel_list
        if get_identifier(item) not in set(idents_to_remove)
    ]

    # Step 2: Insert new items in correct positions
    current_identifiers = [get_identifier(item) for item in ruamel_list]

    for i, updated_item in enumerate(updated_list):
        ident = get_identifier(updated_item)

        if ident not in current_identifiers:
            # Find insertion position by looking for the previous existing item
            insert_pos = find_insertion_position(updated_list, i, current_identifiers)

            ruamel_list.insert(insert_pos, updated_item)
            current_identifiers.insert(insert_pos, ident)


def update_ruamel_for_raw_file(
    ruamel_yaml: CommentedMap,
    original_config_dict: dict[str, DirectIngestRawFileConfig],
    updated_config_dict: dict[str, DirectIngestRawFileConfig],
) -> CommentedMap:
    identifier_map = get_raw_file_config_attribute_to_list_item_identifier_map()

    update_ruamel_dict(
        ruamel_yaml, original_config_dict, updated_config_dict, identifier_map
    )
    return ruamel_yaml
