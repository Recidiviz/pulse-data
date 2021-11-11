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
"""Generates Markdown documentation files for the ingest mappings YAML schema."""

import json
import logging
import os
import re
import sys
from enum import Enum
from io import StringIO
from typing import Any, Dict

import ruamel.yaml
from more_itertools import one
from pytablewriter import MarkdownTableWriter

import recidiviz
from recidiviz.ingest.direct.ingest_mappings import yaml_schema
from recidiviz.tools.docs.utils import persist_file_contents
from recidiviz.utils.string import StrictStringFormatter

ALL_SCHEMAS_PATH = os.path.dirname(yaml_schema.__file__)

DOCS_DIR_PATH = os.path.join(
    os.path.dirname(recidiviz.__file__),
    "..",
    "docs",
    "engineering",
    "ingest_mapping_schema",
)

# JSON schema keywords (defined in http://json-schema.org/draft-07/schema)
DEFINITIONS_KEY = "definitions"
DESCRIPTION_KEY = "description"
EXAMPLES_KEY = "examples"
ONE_OF_KEY = "oneOf"
PATTERN_PROPERTIES_KEY = "patternProperties"
PROPERTIES_KEY = "properties"
# This is a JSON schema key, even though it starts with a dollar sign like the
# ingest mappings schema keywords.
REFERENCE_KEY = "$ref"
REQUIRED_KEY = "required"
TITLE_KEY = "title"
TYPE_KEY = "type"

ARRAY_TYPE = "array"
BOOLEAN_TYPE = "boolean"
OBJECT_TYPE = "object"
STRING_TYPE = "string"

# Additional custom keys used by our JSON schema
CONTEXTUALIZED_EXAMPLES_KEY = "#contextualized_examples"

# Special ingest mappings JSON schema file names
ROOT_SCHEMA_FILE_NAME = "schema.json"
TYPES_FILE_NAME = "index.json"
ENTITY_TREE_SCHEMA_FILE_NAME = "entity_tree.json"

# Ingest mappings schema keywords
CONDITIONAL_FUNCTION_NAME = "$conditional"

# Docs templates
SCHEMA_PAGE_TEMPLATE = """## {schema_page_title}
{top_level_description}

### Expression type
{type}

{page_specific_contents}

{examples_section}
"""

SIMPLE_EXAMPLES_SECTION = """### Examples
{formatted_examples}"""

EXAMPLES_SECTION_WITH_CONTEXTUALIZED_EXAMPLES = """### Example expression structure
{formatted_structure_examples}

### Contextualized examples
Here is how the {expression_name} expression might be used in the context of other expressions:
{formatted_contextualized_examples}"""

INDEX_PAGE_TEMPLATE = """## {type_name} expressions
{top_level_description}

### Valid expressions:
{types_list}
"""

SINGLE_EXAMPLE_TEMPLATE = """```\n{example_yaml}\n```\n"""


ENTITY_TREE_STRUCTURE_NOTE = (
    "Some non-string flat fields may be hydrated with expressions that "
    "produce a string:\n"
    "  * A date field may be hydrated with a string representation of a date "
    "(e.g. '20211012' or '10/12/2021')\n"
    "  * An integer field may be hydrated with a string representation of an integer "
    "(e.g. '12345' or '0').\n"
    "  * An boolean field may be hydrated with a string representation of a boolean "
    "(i.e. case insensitive 't', 'true', 'f', or 'false').\n"
    "When a string value is provided for a non-string field, the string value will be "
    "cast to the appropriate type during parsing.\n\n"
    "If a relationship field child entity is of type EnumEntity and the enum field on"
    "that enum entity evaluates to `null`, then the whole EnumEntity will be filtered "
    "out from the result entity tree."
)
ENTITY_TREE_STRUCTURE_MD = f"""### Structure
Each entity tree follows this structure: 
```
MyEntityName:
  my_enum_field: <enum expression>
  my_boolean_field: <boolean expression>
  my_other_flat_field: <string expression>
  my_list_relationship_field:
    - <entity expression>
  my_single_relationship_field: <entity expression>
  ...
```
{ENTITY_TREE_STRUCTURE_NOTE}"""


IF_DESCRIPTION = (
    "If the boolean expression in the `$if` value is true, then the result "
    "of the `$then` expression is returned. Otherwise, the next branch is evaluated, "
    "if one exists."
)
ELSE_IF_DESCRIPTION = (
    "If the boolean expression in the `$else_if` value is true, then the result "
    "of the `$then` expression is returned. Otherwise, the next branch is evaluated, "
    "if one exists."
)
ELSE_DESCRIPTION = (
    "The result of this expression is returned if none of the above boolean "
    "expressions evaluate to True. Can only be used if an `$if` or `$else_if` "
    "block is also used."
)
CONDITIONAL_PROPERTIES_MD = f"""### Structure
Each list item in the conditional statement can be a YAML object following one three formats:

| Required properties |  Position (out of N) | Description          |
| ------------------- | -------------------- | ---------------------|
| `$if`,`$then`       | `0`                  | {IF_DESCRIPTION}     |
| `$else_if`,`$then`  | `1` through `N-2`    | {ELSE_IF_DESCRIPTION}|
| `$else`             | `N-1`                | {ELSE_DESCRIPTION}   |
"""


class SchemaFileType(Enum):
    ROOT = "ROOT"
    TYPE_INDEX = "TYPE_INDEX"
    LEAF_NODE = "LEAF_NODE"
    FUNCTION = "FUNCTION"
    ENTITY_TREE = "ENTITY_TREE"


def read_json_file(file_path: str) -> Dict[str, Any]:
    """Loads a JSON file into a Python dictionary."""
    with open(file_path, encoding="utf-8") as f:
        return json.load(f)


def json_to_yaml(json_obj: Dict[str, Any]) -> str:
    """Translates a JSON object to a YAML string, preserving all dictionary key
    ordering.
    """
    yml = ruamel.yaml.YAML()
    yml.indent(mapping=2, sequence=4, offset=2)
    stream = StringIO()
    yml.dump(json_obj, stream)
    return stream.getvalue().rstrip("\n")


class SingleSchemaFile:
    """Class that caches info about a particular schema file."""

    def __init__(self, file_path: str):
        self.path: str = file_path
        self.json: Dict[str, Any] = read_json_file(file_path)
        _, file_name = os.path.split(file_path)
        self.file_name = file_name
        self.file_type = self._get_file_type(self.file_name, self.json)
        if self.file_type == SchemaFileType.ROOT and "$id" not in self.json:
            raise ValueError(f"Bad root schema file. Expected $id tag: {self.json}")
        self.docs_output_path = self._get_docs_output_path(self.path)
        self.file_expression_name = self._get_file_expression_name(
            file_type=self.file_type, file_json=self.json, schema_path=self.path
        )

    @staticmethod
    def _get_docs_output_path(schema_path: str) -> str:
        """Returns the absolute path that docs for this schema file should be output to."""
        relative_to_root = os.path.relpath(schema_path, ALL_SCHEMAS_PATH)
        path_no_ext, _ext = os.path.splitext(
            os.path.normpath(os.path.join(DOCS_DIR_PATH, relative_to_root))
        )
        return f"{path_no_ext}.md"

    EXPRESSION_TYPE_FOLDER_NAME_PATTERN = re.compile(r"^([a-z][a-z_]+)_expressions$")

    @classmethod
    def _get_file_expression_name(
        cls, file_type: SchemaFileType, schema_path: str, file_json: Dict[str, Any]
    ) -> str:
        """Returns the name of the expression associated with this schema file."""

        if file_type == SchemaFileType.FUNCTION:
            return one(file_json[PROPERTIES_KEY])
        if file_type == SchemaFileType.TYPE_INDEX:
            _, expression_type_dir = os.path.split(os.path.dirname(schema_path))
            match = re.match(
                cls.EXPRESSION_TYPE_FOLDER_NAME_PATTERN, expression_type_dir
            )
            if not match:
                raise ValueError(
                    f"Expression type directory name [{expression_type_dir}] does not match "
                    f"expected pattern."
                )

            return match.group(1).replace("_", " ").capitalize()
        return file_json[TITLE_KEY]

    @staticmethod
    def _get_file_type(file_name: str, file_json: Dict[str, Any]) -> SchemaFileType:
        if file_name == ROOT_SCHEMA_FILE_NAME:
            return SchemaFileType.ROOT
        if file_name == TYPES_FILE_NAME:
            return SchemaFileType.TYPE_INDEX
        if file_name == ENTITY_TREE_SCHEMA_FILE_NAME:
            return SchemaFileType.ENTITY_TREE

        if file_json[TYPE_KEY] == OBJECT_TYPE:
            return SchemaFileType.FUNCTION
        if file_json[TYPE_KEY] in (STRING_TYPE, BOOLEAN_TYPE):
            return SchemaFileType.LEAF_NODE

        raise ValueError(f"Unknown schema type for file: {file_name}")


class DocsUrlFormatter:
    """Class that formats Markdown URLs for the docs page for a particular schema file."""

    def __init__(
        self,
        schema_file: SingleSchemaFile,
        all_schema_files: Dict[str, SingleSchemaFile],
    ):
        self.schema_file_path = schema_file.path
        self.docs_file_path = schema_file.docs_output_path
        self.all_schema_files = all_schema_files

    def formatted_url_from_abs_path(self, linked_schema_file_path: str) -> str:
        linked_schema_file = self.all_schema_files[linked_schema_file_path]

        relative_to_current = os.path.relpath(
            linked_schema_file.docs_output_path,
            os.path.dirname(self.docs_file_path),
        )
        expression_name = linked_schema_file.file_expression_name
        if expression_name.startswith("$"):
            expression_name = f"`{expression_name}`"
        return f"[{expression_name}]({relative_to_current})"

    def formatted_url_from_ref_path(self, ref_relative_path: str) -> str:
        linked_schema_file_path = os.path.normpath(
            os.path.join(os.path.dirname(self.schema_file_path), ref_relative_path)
        )
        return self.formatted_url_from_abs_path(linked_schema_file_path)


class SingleSchemaFileDocsGenerator:
    """Class that can generate a single markdown documentation file for a single
    ingest mappings JSON schema file, with URL hyperlinks to other relevant files.
    """

    def __init__(
        self,
        schema_file: SingleSchemaFile,
        all_schema_files: Dict[str, SingleSchemaFile],
    ):
        self.schema_file: SingleSchemaFile = schema_file
        self.url_formatter = DocsUrlFormatter(
            self.schema_file,
            all_schema_files,
        )

    def generate_file_markdown(self) -> str:
        """Generates a single markdown documentation file for a single ingest mappings
        JSON schema file, with URL hyperlinks to other relevant files.
        """
        if self.schema_file.file_type is SchemaFileType.ROOT:
            return self._docs_for_root_schema()
        if self.schema_file.file_type is SchemaFileType.ENTITY_TREE:
            return self._docs_for_entity_tree_schema()
        if self.schema_file.file_type is SchemaFileType.TYPE_INDEX:
            return self._docs_for_type_index()
        if self.schema_file.file_type is SchemaFileType.FUNCTION:
            return self._docs_for_function_type()
        if self.schema_file.file_type is SchemaFileType.LEAF_NODE:
            return self._docs_for_leaf_node_type()

        raise ValueError(f"Unexpected schema file type: {self.schema_file.file_type}")

    def _docs_for_root_schema(self) -> str:
        """Generates the file Markdown for the root ingest mappings schema file."""
        examples_section = StrictStringFormatter().format(
            SIMPLE_EXAMPLES_SECTION,
            formatted_examples=self._get_examples_markdown(EXAMPLES_KEY),
        )

        return StrictStringFormatter().format(
            SCHEMA_PAGE_TEMPLATE,
            schema_page_title=self.schema_file.json[TITLE_KEY],
            top_level_description=self.schema_file.json[DESCRIPTION_KEY],
            type="Top-level schema",
            page_specific_contents=self._get_properties_section_markdown(
                self.schema_file.json
            ),
            examples_section=examples_section,
        )

    def _docs_for_entity_tree_schema(self) -> str:
        """Generates the file Markdown for a schema file that defines the schema for an
        entity tree expression.
        """
        examples_section = StrictStringFormatter().format(
            SIMPLE_EXAMPLES_SECTION,
            formatted_examples=self._get_examples_markdown(EXAMPLES_KEY),
        )
        return StrictStringFormatter().format(
            SCHEMA_PAGE_TEMPLATE,
            schema_page_title=self.schema_file.file_expression_name,
            top_level_description=self.schema_file.json[DESCRIPTION_KEY],
            type=self._get_type_index_url(),
            page_specific_contents=ENTITY_TREE_STRUCTURE_MD,
            examples_section=examples_section,
        )

    def _docs_for_type_index(self) -> str:
        """Generates the file Markdown for a schema file that enumerates all expressions
        of a particular type.
        """
        types_list = "\n".join(
            sorted(
                f"* {self.url_formatter.formatted_url_from_ref_path(ref_schema[REFERENCE_KEY])}"
                for ref_schema in self.schema_file.json[ONE_OF_KEY]
            )
        )
        return StrictStringFormatter().format(
            INDEX_PAGE_TEMPLATE,
            type_name=self.schema_file.file_expression_name,
            top_level_description=self.schema_file.json[DESCRIPTION_KEY],
            types_list=types_list,
        )

    def _docs_for_function_type(self) -> str:
        """Generates the file Markdown for a schema file that defines the schema for a
        function expression.
        """
        function_name = one(self.schema_file.json[PROPERTIES_KEY])
        function_schema = self.schema_file.json[PROPERTIES_KEY][function_name]

        if DESCRIPTION_KEY in function_schema:
            raise ValueError(f"Unused description: {function_schema[DESCRIPTION_KEY]}")
        if (
            PROPERTIES_KEY in function_schema
            or PATTERN_PROPERTIES_KEY in function_schema
        ):
            properties_md = self._get_properties_section_markdown(function_schema)
        elif function_schema.get(TYPE_KEY, None) == ARRAY_TYPE:
            if function_name == CONDITIONAL_FUNCTION_NAME:
                properties_md = CONDITIONAL_PROPERTIES_MD
            else:
                items_schema = function_schema["items"]
                if REFERENCE_KEY not in items_schema:
                    raise ValueError(
                        "Expected all list function args to be reference types"
                    )
                item_type_url = self.url_formatter.formatted_url_from_ref_path(
                    items_schema[REFERENCE_KEY]
                )
                properties_md = (
                    f"### Arguments list\nEach item in the arguments list must be of "
                    f"type {item_type_url}"
                )
        elif function_schema.get(TYPE_KEY, None) == STRING_TYPE:
            properties_md = (
                "### Argument\nThis expression takes a single `string` argument."
            )
        elif function_schema.get(REFERENCE_KEY, None):
            type_url = self.url_formatter.formatted_url_from_ref_path(
                function_schema[REFERENCE_KEY]
            )
            properties_md = (
                f"### Argument\nThis expression takes a single {type_url} argument."
            )
        else:
            raise ValueError(f"Unexpected function schema: {function_schema}")

        examples_section = StrictStringFormatter().format(
            EXAMPLES_SECTION_WITH_CONTEXTUALIZED_EXAMPLES,
            expression_name=f"`{self.schema_file.file_expression_name}`",
            formatted_structure_examples=self._get_examples_markdown(EXAMPLES_KEY),
            formatted_contextualized_examples=self._get_examples_markdown(
                CONTEXTUALIZED_EXAMPLES_KEY
            ),
        )
        return StrictStringFormatter().format(
            SCHEMA_PAGE_TEMPLATE,
            schema_page_title=f"`{function_name}`",
            top_level_description=self.schema_file.json[DESCRIPTION_KEY],
            type=self._get_type_index_url(),
            page_specific_contents=properties_md,
            examples_section=examples_section,
        )

    def _docs_for_leaf_node_type(self) -> str:
        """Generates the file Markdown for a schema file that defines the schema for a
        leaf node expression.
        """
        if "const" in self.schema_file.json:
            page_specific_contents = ""
        else:
            page_specific_contents = (
                f"### Expected pattern\n`{self.schema_file.json['pattern']}`"
            )

        examples_section = StrictStringFormatter().format(
            EXAMPLES_SECTION_WITH_CONTEXTUALIZED_EXAMPLES,
            expression_name=f"`{self.schema_file.file_expression_name}`",
            formatted_structure_examples=self._get_examples_markdown(EXAMPLES_KEY),
            formatted_contextualized_examples=self._get_examples_markdown(
                CONTEXTUALIZED_EXAMPLES_KEY
            ),
        )
        return StrictStringFormatter().format(
            SCHEMA_PAGE_TEMPLATE,
            schema_page_title=self.schema_file.file_expression_name,
            top_level_description=self.schema_file.json[DESCRIPTION_KEY],
            type=self._get_type_index_url(),
            page_specific_contents=page_specific_contents,
            examples_section=examples_section,
        )

    def _get_type_index_url(self) -> str:
        if self.schema_file.file_type == SchemaFileType.ROOT:
            raise ValueError("No type index for the root schema file.")

        type_index_schema_file_path = os.path.join(
            os.path.dirname(self.schema_file.path), TYPES_FILE_NAME
        )
        return self.url_formatter.formatted_url_from_abs_path(
            type_index_schema_file_path
        )

    def _get_referenced_expression_type_markdown(
        self, expression_json: Dict[str, Any]
    ) -> str:
        if TYPE_KEY in expression_json and REFERENCE_KEY in expression_json:
            raise ValueError(
                f"Found both [{TYPE_KEY}] and [{REFERENCE_KEY}] referenced in schema: {expression_json}"
            )

        if TYPE_KEY in expression_json:
            t = expression_json[TYPE_KEY]
            if t == ARRAY_TYPE:
                return f"list of {self._get_referenced_expression_type_markdown(expression_json['items'])}"
            return f"`{t}`"
        if REFERENCE_KEY in expression_json:
            return self.url_formatter.formatted_url_from_ref_path(
                expression_json[REFERENCE_KEY]
            )

        raise ValueError(f"Cannot parse type from schema object: {expression_json}")

    def _get_properties_section_markdown(
        self, json_with_properties: Dict[str, Any]
    ) -> str:
        if PROPERTIES_KEY in json_with_properties:
            table_md = self._get_properties_table_markdown(json_with_properties)
        elif PATTERN_PROPERTIES_KEY in json_with_properties:
            table_md = self._get_pattern_properties_table_markdown(json_with_properties)
        else:
            raise ValueError(
                f"Unexpected properties structure for JSON: {json_with_properties}"
            )

        return f"### Properties\n{table_md}"

    def _get_pattern_properties_table_markdown(
        self, json_with_properties: Dict[str, Any]
    ) -> str:
        table_rows = []
        for property_pattern, property_info in json_with_properties[
            PATTERN_PROPERTIES_KEY
        ].items():
            table_rows.append(
                (
                    f"`{property_pattern}`",
                    self._get_referenced_expression_type_markdown(property_info),
                    property_info[DESCRIPTION_KEY],
                )
            )

        writer = MarkdownTableWriter(
            headers=["Property pattern", "Type", "Description"],
            value_matrix=table_rows,
            margin=1,
        )
        return writer.dumps()

    def _get_properties_table_markdown(
        self, json_with_properties: Dict[str, Any]
    ) -> str:
        table_rows = []
        for property_name, property_info in json_with_properties[
            PROPERTIES_KEY
        ].items():
            table_rows.append(
                (
                    f"`{property_name}`",
                    self._get_referenced_expression_type_markdown(property_info),
                    property_name in json_with_properties[REQUIRED_KEY],
                    property_info[DESCRIPTION_KEY],
                )
            )

        writer = MarkdownTableWriter(
            headers=["Property", "Type", "Required", "Description"],
            value_matrix=table_rows,
            margin=1,
        )
        return writer.dumps()

    def _get_examples_markdown(self, examples_key: str) -> str:
        """Formats the schema "examples" as Markdown code blocks. If |interpret_as_json|
        is true, then the example will be parsed as JSON and translated to YAML.
        Otherwise, it will not be modified.
        """
        serialized_examples = self.schema_file.json[examples_key]
        formatted_examples = []
        for example in serialized_examples:
            if isinstance(example, str):
                example_yaml = example
            elif isinstance(example, dict):
                example_yaml = json_to_yaml(example)
            else:
                raise ValueError(
                    f"Unexpected example type [{type(example)}]: {example}"
                )
            formatted_examples.append(
                StrictStringFormatter().format(
                    SINGLE_EXAMPLE_TEMPLATE, example_yaml=example_yaml
                )
            )
        return "".join(formatted_examples)


def generate_documentation() -> int:
    """Generates documentation for the ingest mappings yaml schema. Returns 1 if any
    changes were made to existing docs, returns 0 if no changes were made.
    """
    versions = [
        v
        for v in os.listdir(ALL_SCHEMAS_PATH)
        # Filter out hidden files like .DS_Store / __pycache__
        if not v.startswith(".") and not v.startswith("__")
    ]
    modified = False
    for version in versions:
        schema_version_dir = os.path.join(ALL_SCHEMAS_PATH, version)
        all_schema_files = {}
        for directory, _directories, files in os.walk(schema_version_dir):
            for f in files:
                schema_file_path = os.path.join(directory, f)
                all_schema_files[schema_file_path] = SingleSchemaFile(schema_file_path)

        for schema_file in all_schema_files.values():
            docs_md = SingleSchemaFileDocsGenerator(
                schema_file, all_schema_files
            ).generate_file_markdown()
            modified |= persist_file_contents(docs_md, schema_file.docs_output_path)

    return 1 if modified else 0


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    sys.exit(generate_documentation())
