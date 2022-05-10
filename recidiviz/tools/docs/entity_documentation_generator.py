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
# pylint: disable=unused-import

"""A script which will be called using a pre-commit githook to generate entity documentation.

Can be run on-demand via:
    $ pipenv run python -m recidiviz.tools.docs.entity_documentation_generator
"""

import logging
import os
import re
import sys
from functools import lru_cache
from typing import List, Set, Type

import sqlalchemy
from pytablewriter import MarkdownTableWriter

from recidiviz.common.constants.state.enum_canonical_strings import (
    SHARED_ENUM_VALUE_DESCRIPTIONS,
)
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum
from recidiviz.common.str_field_utils import snake_to_camel, to_snake_case
from recidiviz.persistence.database.schema_utils import get_state_table_classes
from recidiviz.persistence.entity.entity_utils import get_all_enum_classes_in_module
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.tools.docs.summary_file_generator import update_summary_file
from recidiviz.tools.docs.utils import DOCS_ROOT_PATH, persist_file_contents

SCHEMA_DOCS_ROOT = os.path.join(DOCS_ROOT_PATH, "schema")
ENTITIES_PACKAGE_NAME = "entities"
ENTITY_DOCS_ROOT = os.path.join(SCHEMA_DOCS_ROOT, ENTITIES_PACKAGE_NAME)
ENUMS_PACKAGE_NAME = "enums"
ENUM_DOCS_ROOT = os.path.join(SCHEMA_DOCS_ROOT, ENUMS_PACKAGE_NAME)


@lru_cache(maxsize=None)
def _get_all_entity_enum_classes() -> Set[Type[StateEntityEnum]]:
    """Returns the set of StateEntityEnums used in
    recidiviz/persistence/entity/state/entities.py"""
    enum_classes = get_all_enum_classes_in_module(state_entities)

    state_enum_classes: Set[Type[StateEntityEnum]] = set()

    for enum_class in enum_classes:
        if not issubclass(enum_class, StateEntityEnum):
            raise TypeError(
                "Found enum class used in state/entities.py that is not of "
                f"type StateEntityEnum: {enum_class}."
            )
        state_enum_classes.add(enum_class)

    return state_enum_classes


def generate_entity_documentation() -> bool:
    """
    Parses `persistence/entity/state/entities_docs.yaml` to produce documentation. Overwrites or creates the
    corresponding Markdown file.

    Returns True if files were modified, False otherwise.
    """
    all_enum_classes = _get_all_entity_enum_classes()
    all_enum_names = [enum_class.__name__ for enum_class in all_enum_classes]

    def _camel_case_enum_name_from_field(field: sqlalchemy.Column) -> str:
        field_name = field.type.name
        enum_name = snake_to_camel(field_name, capitalize_first_letter=True)

        if enum_name not in all_enum_names:
            raise ValueError(f"Enum not found in list of state enums: {enum_name}")

        return enum_name

    def _get_fields(fields: List[sqlalchemy.Column]) -> str:
        """Returns a table of the entity's fields and their corresponding descriptions."""
        if fields is None:
            return "No Fields"
        if not fields:
            return "<No columns>"

        table_matrix = []
        for field in fields:
            if field.comment is None:
                raise ValueError(
                    f"Every entity field must have an associated comment. "
                    f"Field {field.name} has no comment."
                )

            if hasattr(field.type, "enums"):
                enum_name = _camel_case_enum_name_from_field(field)
                entity_type_contents = (
                    f"[{enum_name}](../{ENUMS_PACKAGE_NAME}/{enum_name}.md)"
                )

            else:
                entity_type_contents = field.type.python_type.__name__.upper()

            field_values = [
                field.name,
                field.comment,
                entity_type_contents,
            ]
            table_matrix.append(field_values)

        writer = MarkdownTableWriter(
            headers=[
                "Entity Field",
                "Entity Description",
                "Entity Type",
            ],
            value_matrix=table_matrix,
            margin=0,
        )
        return writer.dumps()

    if not os.path.isdir(ENTITY_DOCS_ROOT):
        os.mkdir(ENTITY_DOCS_ROOT)
    old_file_names = set(os.listdir(ENTITY_DOCS_ROOT))
    generated_file_names = set()
    anything_modified = False
    for t in get_state_table_classes():
        if t.comment is None:
            raise ValueError(
                f"Every entity must have an associated comment. "
                f"Entity {t.name} has no comment."
            )
        documentation = f"## {t.name}\n\n"
        documentation += f"{t.comment}\n\n"
        documentation += f"{_get_fields(t.columns)}\n\n"
        markdown_file_name = f"{t.name}.md"
        markdown_file_path = os.path.join(ENTITY_DOCS_ROOT, markdown_file_name)
        anything_modified |= persist_file_contents(documentation, markdown_file_path)
        generated_file_names.add(markdown_file_name)

    extra_files = old_file_names.difference(generated_file_names)
    for extra_file in extra_files:
        anything_modified |= True
        os.remove(os.path.join(ENTITY_DOCS_ROOT, extra_file))

    return anything_modified


def _add_entity_and_enum_links_to_enum_description(
    text: str, all_enum_names: List[str], all_entity_names: List[str]
) -> str:
    """Updates the provided |text| string to have hyperlinks to other enum and entity
    docs for any enum/entity references in the string. Handles references in the
    following formats:
        - `StateEntityEnum.VALUE`
        - `StateEntityEnum`
        - `StateEntityExample`
        - `StateEntityExamples`
    """
    enum_refs = re.findall(r"`(State[a-zA-Z]*)\.([A-Z_]*)`", text)

    for enum_ref in enum_refs:
        if enum_ref[0] not in all_enum_names:
            # Not a reference to an enum class
            continue

        enum_ref_string = f"`{enum_ref[0]}.{enum_ref[1]}`"
        text = text.replace(enum_ref_string, f"[{enum_ref_string}]({enum_ref[0]}.md)")

    other_refs = re.findall(r"`(State[a-zA-Z]*)`", text)

    for ref in other_refs:
        if ref in all_enum_names:
            file_path = ref
        elif ref.endswith("s") and ref[:-1] in all_enum_names:
            file_path = f"{ref[:-1]}"
        elif ref in all_entity_names:
            file_path = f"../{ENTITIES_PACKAGE_NAME}/{to_snake_case(ref)}"
        elif ref.endswith("s") and ref[:-1] in all_entity_names:
            file_path = f"../{ENTITIES_PACKAGE_NAME}/{to_snake_case(ref[:-1])}"
        else:
            # Not a reference to an enum or entity class
            continue

        ref_string = f"`{ref}`"
        text = text.replace(ref_string, f"[{ref_string}]({file_path}.md)")

    return text


def generate_enum_documentation() -> bool:
    """
    Parses enum files to produce documentation. Overwrites or creates the
    corresponding Markdown file.

    Returns True if files were modified, False otherwise.
    """
    all_enum_classes = _get_all_entity_enum_classes()
    all_enum_names = [enum_class.__name__ for enum_class in all_enum_classes]
    all_entity_names = [
        snake_to_camel(t.name, capitalize_first_letter=True)
        for t in get_state_table_classes()
    ]

    def _get_value_descriptions(enum_type: Type[StateEntityEnum]) -> str:
        table_matrix = []

        enum_descriptions = {
            **enum_type.get_value_descriptions(),
            **{
                enum_value: SHARED_ENUM_VALUE_DESCRIPTIONS[enum_value.value]
                for enum_value in enum_type
                if SHARED_ENUM_VALUE_DESCRIPTIONS.get(enum_value.value)
            },
        }

        sorted_values: List[StateEntityEnum] = sorted(
            list(enum_type), key=lambda e: e.value
        )

        for enum_value in sorted_values:
            try:
                enum_value_description = enum_descriptions[enum_value]
            except KeyError as e:
                raise KeyError(
                    "Must implement get_value_descriptions() for enum "
                    f"class {enum_type.__name__} with descriptions "
                    f"for each enum value. Missing description for "
                    f"value: [{enum_value}]."
                ) from e

            enum_value_description = _add_entity_and_enum_links_to_enum_description(
                enum_value_description, all_enum_names, all_entity_names
            )

            table_row = [enum_value.value, enum_value_description]
            table_matrix.append(table_row)

        writer = MarkdownTableWriter(
            headers=[
                "Enum Value",
                "Description",
            ],
            value_matrix=table_matrix,
            margin=0,
        )
        return writer.dumps()

    if not os.path.isdir(ENUM_DOCS_ROOT):
        os.mkdir(ENUM_DOCS_ROOT)
    old_file_names = set(os.listdir(ENUM_DOCS_ROOT))
    generated_file_names = set()
    anything_modified = False
    for enum_class in _get_all_entity_enum_classes():
        documentation = f"## {enum_class.__name__}\n\n"
        documentation += f"{_add_entity_and_enum_links_to_enum_description(enum_class.get_enum_description(),all_enum_names, all_entity_names)}\n\n"
        documentation += f"{_get_value_descriptions(enum_class)}\n\n"
        markdown_file_name = f"{enum_class.__name__}.md"
        markdown_file_path = os.path.join(ENUM_DOCS_ROOT, markdown_file_name)
        anything_modified |= persist_file_contents(documentation, markdown_file_path)
        generated_file_names.add(markdown_file_name)

    extra_files = old_file_names.difference(generated_file_names)
    for extra_file in extra_files:
        anything_modified |= True
        os.remove(os.path.join(ENUM_DOCS_ROOT, extra_file))

    return anything_modified


def main() -> int:
    """Generates entity documentation, cleaning up any obsolete docs files."""

    def _generate_entity_documentation_summary() -> str:
        list_of_tables: str = "\n".join(
            sorted(
                f"\t - [{entity_table}](schema/{ENTITIES_PACKAGE_NAME}/{entity_table}.md)"
                for entity_table in get_state_table_classes()
            )
        )
        list_of_tables += "\n"
        return list_of_tables

    def _generate_enum_documentation_summary() -> str:
        list_of_enums: str = "\n".join(
            sorted(
                f"\t - [{enum_class.__name__}](schema/{ENUMS_PACKAGE_NAME}/"
                f"{enum_class.__name__}.md)"
                for enum_class in get_all_enum_classes_in_module(state_entities)
            )
        )
        list_of_enums += "\n"
        return list_of_enums

    def _generate_summary_strings() -> List[str]:
        entity_documentation_summary = ["## Schema Catalog\n\n"]
        entity_documentation_summary.extend([f"- {ENTITIES_PACKAGE_NAME}\n"])
        entity_documentation_summary.extend(_generate_entity_documentation_summary())
        entity_documentation_summary.extend([f"- {ENUMS_PACKAGE_NAME}\n"])
        entity_documentation_summary.extend(_generate_enum_documentation_summary())
        return entity_documentation_summary

    modified = generate_entity_documentation()
    modified |= generate_enum_documentation()

    if modified:
        update_summary_file(
            _generate_summary_strings(),
            "## Schema Catalog",
        )

    return 1 if modified else 0


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    sys.exit(main())
