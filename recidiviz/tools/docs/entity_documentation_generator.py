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
    $ uv run python -m recidiviz.tools.docs.entity_documentation_generator

"""

import logging
import os
import re
import sys
import textwrap
from types import ModuleType

import pandas as pd
from google.cloud.bigquery import SchemaField

from recidiviz.common.attr_mixins import (
    CachedAttributeInfo,
    CachedClassAttributeReference,
    attribute_field_type_reference_for_class,
)
from recidiviz.common.constants.state.enum_canonical_strings import (
    SHARED_ENUM_VALUE_DESCRIPTIONS,
)
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum
from recidiviz.common.str_field_utils import to_snake_case
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.entity_utils import (
    get_all_entity_classes_in_module,
    get_all_enum_classes_in_module,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.tools.docs.summary_file_generator import update_summary_file
from recidiviz.tools.docs.utils import (
    DOCS_ROOT_PATH,
    hyperlink_todos,
    persist_file_contents,
)
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type

SCHEMA_DOCS_ROOT = os.path.join(DOCS_ROOT_PATH, "schema")
ENTITIES_PACKAGE_NAME = "entities"
ENTITY_DOCS_ROOT = os.path.join(SCHEMA_DOCS_ROOT, ENTITIES_PACKAGE_NAME)
NORMALIZED_ENTITIES_PACKAGE_NAME = "normalized_entities"
NORMALIZED_ENTITY_DOCS_ROOT = os.path.join(
    SCHEMA_DOCS_ROOT, NORMALIZED_ENTITIES_PACKAGE_NAME
)
ENUMS_PACKAGE_NAME = "enums"
ENUM_DOCS_ROOT = os.path.join(SCHEMA_DOCS_ROOT, ENUMS_PACKAGE_NAME)

ENTITY_DOC_TEMPLATE = textwrap.dedent(
    """
    ## {table_name}
    {table_description}
 
    ### Table Schema
    {field_table}
    """
)

ENUM_DOC_TEMPLATE = textwrap.dedent(
    """
    ## {enum_name}
    {enum_description}
 
    ### Entity Values
    {value_table}
    """
)


class ModuleSchemaInformation:
    """
    Instantiate this class to have references to the different ways we view our schema.

    bq_schema: A dict mapping the table name to the SchemaFields in that table.
    table_to_entity_info: A dict mapping the table name to our cached entity information.
    enum_classes: A set of StateEntityEnum classes
    """

    def __init__(self, module: ModuleType):
        self.bq_schema = get_bq_schema_for_entities_module(module)
        self.table_to_entity_info: dict[str, CachedClassAttributeReference] = {}
        self.enum_classes: set[type[StateEntityEnum]] = set()
        for entity_class in get_all_entity_classes_in_module(module):
            class_reference = attribute_field_type_reference_for_class(entity_class)
            self.table_to_entity_info[entity_class.get_table_id()] = class_reference
            for field in class_reference.fields:
                field_info = class_reference.get_field_info(field)
                if not (enum_class := field_info.enum_cls):
                    continue
                if not issubclass(enum_class, StateEntityEnum):
                    raise TypeError(
                        "Found enum class used in state/entities.py that is not of "
                        f"type StateEntityEnum: {enum_class}."
                    )
                self.enum_classes.add(enum_class)


def _add_entity_links(
    row: pd.Series, field_refs: dict[str, CachedAttributeInfo]
) -> pd.Series:
    """Updates the Type and Description columns of table field info with links."""
    if match := re.match(r"Foreign key reference to (\w+)", row.Description):
        linked_table = match.group(1)
        row.Description = re.sub(
            linked_table, f"[{linked_table}](./{linked_table}.md)", row.Description
        )
    elif enum_cls := field_refs[row.Name].enum_cls:
        enum_name = enum_cls.__name__
        row.Type = f"[{enum_name}](../{ENUMS_PACKAGE_NAME}/{enum_name}.md)"
    return row


def document_table(
    table_name: str,
    table_schema: list[SchemaField],
    table_to_entity_info: dict[str, CachedClassAttributeReference],
) -> str:
    """Creates the markdown content to document a single BigQuery table."""
    df = pd.DataFrame([field.to_api_repr() for field in table_schema])
    df.columns = [c.title() for c in df.columns]
    # TODO(#30204) Include 'mode' when schema has required columns
    df = df.drop(columns=["Mode"])
    table_description = ""
    if "association" not in table_name:
        entity_info = table_to_entity_info[table_name]
        df = df.apply(
            _add_entity_links, axis=1, field_refs=entity_info.field_to_attribute_info
        )
        table_description = textwrap.dedent(
            hyperlink_todos(entity_info.attr_cls.__doc__ or "")
        )
    return StrictStringFormatter().format(
        ENTITY_DOC_TEMPLATE,
        table_name=table_name,
        table_description=table_description,
        field_table=df.to_markdown(index=False),
    )


def remove_extra_files(doc_path: str, generated_file_names: set[str]) -> bool:
    anything_modified = False
    extra_files = set(os.listdir(doc_path)).difference(generated_file_names)
    for extra_file in extra_files:
        anything_modified |= True
        os.remove(os.path.join(doc_path, extra_file))
    return anything_modified


def generate_entity_documentation(
    module_schema_info: ModuleSchemaInformation, root_path_str: str
) -> bool:
    generated_file_names = set()
    anything_modified = False
    for table_name, table_schema in module_schema_info.bq_schema.items():
        documentation = document_table(
            table_name, table_schema, module_schema_info.table_to_entity_info
        )
        anything_modified |= persist_file_contents(
            documentation, os.path.join(root_path_str, f"{table_name}.md")
        )
        generated_file_names.add(f"{table_name}.md")
    anything_modified |= remove_extra_files(root_path_str, generated_file_names)
    return anything_modified


def _add_entity_and_enum_links_to_enum_description(
    text: str, all_enum_names: set[str], all_entity_names: set[str]
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


def _get_enum_value_descriptions(enum_type: type[StateEntityEnum]) -> str:
    data = []
    value_descriptions = enum_type.get_value_descriptions()
    for en in sorted(list(enum_type), key=lambda e: e.value):
        try:
            if not (description := value_descriptions.get(en)):
                description = SHARED_ENUM_VALUE_DESCRIPTIONS[en.value]
        except KeyError as e:
            raise KeyError(
                "Must implement get_value_descriptions() for enum "
                f"class {enum_type.__name__} with descriptions "
                f"for each enum value. Missing description for "
                f"value: [{en.value}]."
            ) from e
        data.append(
            {
                "Value": en.value,
                "Description": description,
            }
        )
    return assert_type(pd.DataFrame(data).to_markdown(index=False), str)


def generate_enum_documentation(module_schema_info: ModuleSchemaInformation) -> bool:
    """
    Parses enum files to produce documentation. Overwrites or creates the
    corresponding Markdown file.

    Returns True if files were modified, False otherwise.
    """
    all_enum_names = {
        enum_class.__name__ for enum_class in module_schema_info.enum_classes
    }
    all_entity_names = {
        t for t in module_schema_info.bq_schema if "association" not in t
    }

    generated_file_names = set()
    anything_modified = False
    for enum_class in module_schema_info.enum_classes:
        documentation = StrictStringFormatter().format(
            ENUM_DOC_TEMPLATE,
            enum_name=enum_class.__name__,
            enum_description=textwrap.dedent(
                hyperlink_todos(enum_class.get_enum_description())
            ),
            value_table=_get_enum_value_descriptions(enum_class),
        )
        documentation = _add_entity_and_enum_links_to_enum_description(
            documentation, all_enum_names, all_entity_names
        )
        markdown_file_path = os.path.join(ENUM_DOCS_ROOT, f"{enum_class.__name__}.md")
        anything_modified |= persist_file_contents(documentation, markdown_file_path)
        generated_file_names.add(f"{enum_class.__name__}.md")

    anything_modified |= remove_extra_files(ENUM_DOCS_ROOT, generated_file_names)
    return anything_modified


def _update_schema_catalog_page(
    entities_tables: list[str], normalized_entities_tables: list[str]
) -> None:
    entities_list: str = "\n".join(
        sorted(
            f"\t - [{entity_table}](schema/{ENTITIES_PACKAGE_NAME}/{entity_table}.md)"
            for entity_table in entities_tables
        )
    )
    entities_list += "\n"

    normalized_list: str = "\n".join(
        sorted(
            f"\t - [{entity_table}](schema/{NORMALIZED_ENTITIES_PACKAGE_NAME}/{entity_table}.md)"
            for entity_table in normalized_entities_tables
        )
    )
    normalized_list += "\n"

    list_of_enums: str = "\n".join(
        sorted(
            f"\t - [{enum_class.__name__}](schema/{ENUMS_PACKAGE_NAME}/"
            f"{enum_class.__name__}.md)"
            for enum_class in get_all_enum_classes_in_module(state_entities)
        )
    )
    list_of_enums += "\n"

    update_summary_file(
        [
            "## Schema Catalog\n\n",
            f"- {ENTITIES_PACKAGE_NAME}\n",
            *entities_list,
            f"- {NORMALIZED_ENTITIES_PACKAGE_NAME}\n",
            *normalized_list,
            f"- {ENUMS_PACKAGE_NAME}\n",
            *list_of_enums,
        ],
        "## Schema Catalog",
    )


def _make_schema_doc_directories() -> None:
    for path in [ENTITY_DOCS_ROOT, ENTITY_DOCS_ROOT, NORMALIZED_ENTITY_DOCS_ROOT]:
        if not os.path.isdir(path):
            os.mkdir(path)


def main() -> int:
    """Generates entity documentation, cleaning up any obsolete docs files."""
    _make_schema_doc_directories()
    state_schema_info = ModuleSchemaInformation(state_entities)
    normalized_state_schema_info = ModuleSchemaInformation(normalized_entities)

    modified = generate_entity_documentation(state_schema_info, ENTITY_DOCS_ROOT)
    modified |= generate_enum_documentation(state_schema_info)
    modified |= generate_entity_documentation(
        normalized_state_schema_info, NORMALIZED_ENTITY_DOCS_ROOT
    )
    if modified:
        _update_schema_catalog_page(
            list(state_schema_info.bq_schema.keys()),
            list(normalized_state_schema_info.bq_schema.keys()),
        )
        return 1
    return 0


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    sys.exit(main())
