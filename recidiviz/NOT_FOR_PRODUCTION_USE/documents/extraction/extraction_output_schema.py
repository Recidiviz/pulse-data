# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Schema definitions for extraction output configuration.

This module provides a user-friendly way to define extraction output schemas in YAML.
Each document produces exactly one extraction result object.

Supported field types: STRING, BOOLEAN, ENUM, INTEGER, FLOAT, ARRAY_OF_STRUCT.
ARRAY_OF_STRUCT fields contain nested sub-fields, each individually wrapped in the
standard {value, null_reason, confidence_score, citations} structure.

Example YAML configuration:
```yaml
output_schema:
  full_batch_description: >
    Employment information extracted from case notes.
  result_level_description: >
    Comprehensive extraction of employment-related information from a case note.

  inferred_fields:
    - name: primary_status
      type: ENUM
      description: The main employment status.
      values: [employed, unemployed, other]
      required: true

    - name: employers
      type: ARRAY_OF_STRUCT
      description: List of employers mentioned in this note.
      fields:
        - name: employer_name
          type: STRING
          description: Name of the employer/organization.
        - name: job_title
          type: STRING
          description: The person's job title.
```
"""
import json
from enum import Enum
from typing import Any

import attr

from recidiviz.utils.yaml_dict import YAMLDict

# Reserved field names that are auto-generated
RESERVED_FIELD_NAME_IS_RELEVANT = "is_relevant"


class ExtractionFieldType(Enum):
    """Supported field types for extraction output schemas."""

    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    ENUM = "ENUM"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    ARRAY_OF_STRUCT = "ARRAY_OF_STRUCT"


# TODO(#61717): Implement better classification for document-level errors

# Valid values for the null_reason field
# TODO(#61717): Implement better handling for errors due to explicit content (e.g.
# info about a sexual offense)
NULL_REASON_VALUES = ("not_applicable", "no_info_found", "explicitly_unknown")

# Reserved enum values that should not be used in field definitions.
# These are handled by the null_reason field instead.
RESERVED_ENUM_VALUES = frozenset({"unknown"} | set(NULL_REASON_VALUES))


@attr.define(frozen=True)
class ExtractionInferredField:
    """A field whose value is inferred by the extraction process.

    These are the actual outputs of the extraction - information extracted
    from the document content.
    """

    name: str
    field_type: ExtractionFieldType
    # The description as written in the YAML, before type-specific augmentation
    unaugmented_description: str

    # Whether or not the extraction must provide this info in the output
    required: bool
    # Only populated for ENUM type
    enum_values: tuple[str, ...] | None = None
    # Only populated for ARRAY_OF_STRUCT type — defines the fields within each struct
    struct_fields: tuple["ExtractionInferredField", ...] | None = None

    def __attrs_post_init__(self) -> None:
        if self.field_type == ExtractionFieldType.ENUM and not self.enum_values:
            raise ValueError(
                f"Field '{self.name}' has type ENUM but no enum_values specified"
            )
        if self.field_type != ExtractionFieldType.ENUM and self.enum_values:
            raise ValueError(
                f"Field '{self.name}' has enum_values but type is "
                f"{self.field_type.value}, not ENUM"
            )
        if (
            self.field_type == ExtractionFieldType.ARRAY_OF_STRUCT
            and not self.struct_fields
        ):
            raise ValueError(
                f"Field '{self.name}' has type ARRAY_OF_STRUCT but no struct_fields specified"
            )
        if (
            self.field_type != ExtractionFieldType.ARRAY_OF_STRUCT
            and self.struct_fields
        ):
            raise ValueError(
                f"Field '{self.name}' has struct_fields but type is "
                f"{self.field_type.value}, not ARRAY_OF_STRUCT"
            )
        # Validate that enum values don't use reserved values
        if self.enum_values:
            reserved_found = set(self.enum_values) & RESERVED_ENUM_VALUES
            if reserved_found:
                raise ValueError(
                    f"Field '{self.name}' uses reserved enum values: {sorted(reserved_found)}. "
                    f"These values are reserved for the null_reason field. "
                    f"Use null_reason='not_applicable' or null_reason='no_info_found' instead."
                )

    @property
    def description(self) -> str:
        """Returns the description with type-specific guidance for the LLM."""
        description = self.unaugmented_description
        if self.field_type == ExtractionFieldType.BOOLEAN:
            # Prepend "true/false, " and lowercase the first character
            description = f"true/false, {description[0].lower()}{description[1:]}"
        elif self.field_type == ExtractionFieldType.ENUM:
            # Prepend "Choose one from: [...]."
            # enum_values is guaranteed to be non-None for ENUM types (enforced in __attrs_post_init__)
            assert self.enum_values is not None
            values_str = str(list(self.enum_values))
            description = f"Choose one from: {values_str}. {description}"
        elif self.field_type == ExtractionFieldType.INTEGER:
            description = description.rstrip().rstrip(".") + "."
            description = f"{description} Provide as an integer string."
        elif self.field_type == ExtractionFieldType.FLOAT:
            description = description.rstrip().rstrip(".") + "."
            description = f"{description} Provide as a numeric string."
        return description

    @classmethod
    def from_yaml_dict(cls, yaml_dict: YAMLDict) -> "ExtractionInferredField":
        """Constructs an ExtractionInferredField from a parsed YAML dictionary."""
        field_type = ExtractionFieldType(yaml_dict.pop("type", str).upper())

        enum_values: tuple[str, ...] | None = None
        if field_type == ExtractionFieldType.ENUM:
            values_list = yaml_dict.pop("values", list)
            enum_values = tuple(str(v) for v in values_list)
        else:
            # Throw if values is present on a non-ENUM field
            if yaml_dict.peek_optional("values", list) is not None:
                field_name = yaml_dict.peek("name", str)
                raise ValueError(
                    f"Field '{field_name}' has 'values' specified but type is "
                    f"{field_type.value}, not ENUM. Remove 'values' or change type to ENUM."
                )

        struct_fields: tuple[ExtractionInferredField, ...] | None = None
        if field_type == ExtractionFieldType.ARRAY_OF_STRUCT:
            fields_list = yaml_dict.pop_dicts("fields")
            struct_fields = tuple(
                ExtractionInferredField.from_yaml_dict(f) for f in fields_list
            )
        else:
            if yaml_dict.peek_optional("fields", list) is not None:
                field_name = yaml_dict.peek("name", str)
                raise ValueError(
                    f"Field '{field_name}' has 'fields' specified but type is "
                    f"{field_type.value}, not ARRAY_OF_STRUCT. "
                    f"Remove 'fields' or change type to ARRAY_OF_STRUCT."
                )

        return cls(
            name=yaml_dict.pop("name", str),
            field_type=field_type,
            unaugmented_description=yaml_dict.pop("description", str),
            required=yaml_dict.pop_optional("required", bool) or False,
            enum_values=enum_values,
            struct_fields=struct_fields,
        )

    def value_wrapped_schema_property(self) -> dict[str, Any]:
        """Returns the standard {value, null_reason, confidence_score, citations}
        wrapper schema for a flat field."""
        # Build the value property
        value_property: dict[str, Any] = {
            "type": "STRING",
            "description": self.description,
            "nullable": True,
        }
        if self.enum_values:
            value_property["enum"] = list(self.enum_values)

        # Build the citation item schema
        citation_item_schema: dict[str, Any] = {
            "type": "OBJECT",
            "properties": {
                "text": {
                    "type": "STRING",
                    "description": (
                        "The exact text from the document. Where possible, include a "
                        "full human-readable phrase or sentence that provides context "
                        "for how the source text informed the extraction, rather than "
                        "just the minimal extracted value. For example, prefer "
                        "'Client has been staying at the Sunny Days Halfway House' "
                        "over just 'Sunny Days Halfway House'."
                    ),
                },
                "start": {
                    "type": "INTEGER",
                    "description": "Start character index of the snippet in the document provided in the `text` field.",
                },
                "end": {
                    "type": "INTEGER",
                    "description": "End character index of the snippet in the document provided in the `text` field.",
                },
            },
            "required": ["text", "start", "end"],
        }

        return {
            "type": "OBJECT",
            "description": f"Extraction result for '{self.name}'",
            "properties": {
                "value": value_property,
                "null_reason": {
                    "type": "STRING",
                    "description": (
                        "Reason why value is null. Use 'not_applicable' if the field doesn't apply "
                        "given other extracted values, 'no_info_found' if the information was looked "
                        "for but not found, 'explicitly_unknown' if the document explicitly states "
                        "the value is unknown/N/A. Set to null when value is present."
                    ),
                    "nullable": True,
                    "enum": list(NULL_REASON_VALUES),
                },
                "confidence_score": {
                    "type": "NUMBER",
                    "description": "Confidence score (0.0-1.0) for this extraction",
                },
                "citations": {
                    "type": "ARRAY",
                    "description": (
                        "Citations from the source document supporting this extraction. "
                        "Set to null when null_reason is 'not_applicable' or 'no_info_found'. "
                        "Provide citations when null_reason is 'explicitly_unknown' to cite "
                        "the text that explicitly states the value is unknown."
                    ),
                    "nullable": True,
                    "items": citation_item_schema,
                },
            },
            "required": ["value", "null_reason", "confidence_score", "citations"],
        }

    def to_llm_schema_property(self) -> dict[str, Any]:
        """Converts to the JSON schema property for this field.

        For flat fields, returns a nested object with
        {value, null_reason, confidence_score, citations}.

        For ARRAY_OF_STRUCT fields, returns an ARRAY where each item is an OBJECT
        whose inner fields are individually wrapped in the standard
        {value, null_reason, confidence_score, citations} structure. The
        ARRAY_OF_STRUCT field itself is NOT wrapped.
        """
        if self.field_type == ExtractionFieldType.ARRAY_OF_STRUCT:
            if self.struct_fields is None:
                raise ValueError(
                    f"ARRAY_OF_STRUCT field '{self.name}' has no struct_fields"
                )
            fields = list(self.struct_fields)
            struct_properties: dict[str, Any] = {}
            for sf in fields:
                struct_properties[sf.name] = sf.value_wrapped_schema_property()
            return {
                "type": "ARRAY",
                "description": self.description,
                "nullable": True,
                "items": {
                    "type": "OBJECT",
                    "properties": struct_properties,
                    "required": [sf.name for sf in fields if sf.required],
                },
            }
        return self.value_wrapped_schema_property()

    def example_results(self) -> list[dict[str, Any]]:
        """Returns example results showing common cases for inferred fields."""
        return [
            # Case 1: Value present
            {
                "value": "example_value",
                "null_reason": None,
                "confidence_score": 0.95,
                "citations": [{"text": "example citation", "start": 0, "end": 15}],
            },
            # Case 2: Value null - not applicable
            {
                "value": None,
                "null_reason": "not_applicable",
                "confidence_score": 0.9,
                "citations": None,
            },
            # Case 3: Value null - no info found
            {
                "value": None,
                "null_reason": "no_info_found",
                "confidence_score": 0.85,
                "citations": None,
            },
            # Case 4: Value null - explicitly unknown (with citation)
            {
                "value": None,
                "null_reason": "explicitly_unknown",
                "confidence_score": 0.95,
                "citations": [{"text": "Status: Unknown", "start": 100, "end": 115}],
            },
        ]


@attr.define(frozen=True)
class ExtractionOutputSchema:
    """Complete output schema for an extractor collection.

    Defines the structure of extraction results. Users define fields at the
    individual result level - batching into arrays is handled transparently.
    """

    # Description for the full batch (array) of results
    full_batch_description: str

    # Description for each individual result in the batch
    result_level_description: str

    # Map of inferred field name to field definition
    inferred_fields_by_name: dict[str, "ExtractionInferredField"]

    @classmethod
    def from_yaml_dict(
        cls,
        yaml_dict: YAMLDict,
        collection_description: str,
    ) -> "ExtractionOutputSchema":
        """Constructs an ExtractionOutputSchema from a parsed YAML dictionary."""
        inferred_fields_by_name: dict[str, ExtractionInferredField] = {}

        # Auto-generate is_relevant description from collection description
        is_relevant_description = (
            f"Whether this document contains any relevant information to this "
            f"extraction job, whose aim is to '{collection_description.strip()}'"
        )

        # Auto-inject is_relevant as the first inferred field
        is_relevant_field = ExtractionInferredField(
            name=RESERVED_FIELD_NAME_IS_RELEVANT,
            field_type=ExtractionFieldType.BOOLEAN,
            unaugmented_description=is_relevant_description,
            required=True,
        )
        inferred_fields_by_name[RESERVED_FIELD_NAME_IS_RELEVANT] = is_relevant_field

        for field_yaml in yaml_dict.pop_dicts("inferred_fields"):
            inferred_field = ExtractionInferredField.from_yaml_dict(field_yaml)
            if inferred_field.name == RESERVED_FIELD_NAME_IS_RELEVANT:
                raise ValueError(
                    f"Field name '{RESERVED_FIELD_NAME_IS_RELEVANT}' is reserved and auto-generated."
                )
            if inferred_field.name in inferred_fields_by_name:
                raise ValueError(f"Duplicate field name: '{inferred_field.name}'")
            inferred_fields_by_name[inferred_field.name] = inferred_field

        return cls(
            full_batch_description=yaml_dict.pop("full_batch_description", str),
            result_level_description=yaml_dict.pop("result_level_description", str),
            inferred_fields_by_name=inferred_fields_by_name,
        )

    @property
    def inferred_fields(self) -> list[ExtractionInferredField]:
        """Returns the inferred fields in order, with is_relevant first."""
        result: list[ExtractionInferredField] = []
        # Ensure is_relevant is first
        if RESERVED_FIELD_NAME_IS_RELEVANT in self.inferred_fields_by_name:
            result.append(self.inferred_fields_by_name[RESERVED_FIELD_NAME_IS_RELEVANT])
        # Then add remaining fields
        for name, field in self.inferred_fields_by_name.items():
            if name != RESERVED_FIELD_NAME_IS_RELEVANT:
                result.append(field)
        return result

    def required_field_names(self) -> list[str]:
        """Returns names of all required fields."""
        return [f.name for f in self.inferred_fields if f.required]

    def to_llm_json_schema(self) -> dict[str, Any]:
        """Converts to the full JSON schema format expected by the LLM.

        Returns a single OBJECT schema — each document produces exactly one
        extraction result.
        """
        # Build properties from all fields
        properties: dict[str, Any] = {}
        for field in self.inferred_fields_by_name.values():
            properties[field.name] = field.to_llm_schema_property()

        return {
            "type": "OBJECT",
            "description": self.result_level_description,
            "properties": properties,
            "required": self.required_field_names(),
        }

    def output_format_instructions(self) -> str:
        """Generates human-readable output format instructions for the LLM prompt.

        This can be inserted into a prompt template using {output_format_instructions}.
        Provides a high-level explanation of the output structure with examples.
        """
        lines = [self.result_level_description.strip()]
        lines.append("")
        lines.append(
            "Each document produces exactly one extraction result object. "
            "The output schema is defined with the submitted job and must be "
            "followed strictly."
        )
        lines.append("")

        # Inferred field examples
        lines.append(
            "Each flat field in the output is an object with "
            "{value, null_reason, confidence_score, citations}. Example cases:"
        )
        dummy_inferred_field = ExtractionInferredField(
            name="my_field",
            field_type=ExtractionFieldType.STRING,
            unaugmented_description="Field description",
            required=False,
        )
        for inferred_example_value in dummy_inferred_field.example_results():
            inferred_example = {"my_field": inferred_example_value}
            for json_line in json.dumps(inferred_example, indent=2).split("\n"):
                lines.append(f"  {json_line}")
            lines.append("")

        # ARRAY_OF_STRUCT example
        has_array_fields = any(
            f.field_type == ExtractionFieldType.ARRAY_OF_STRUCT
            for f in self.inferred_fields_by_name.values()
        )
        if has_array_fields:
            lines.append(
                "Array-of-struct fields are arrays where each element is an object "
                "whose fields each use the same {value, null_reason, confidence_score, "
                "citations} wrapper. Example:"
            )
            array_example = {
                "my_array_field": [
                    {
                        "sub_field_a": {
                            "value": "example",
                            "null_reason": None,
                            "confidence_score": 0.95,
                            "citations": [
                                {"text": "example citation", "start": 0, "end": 15}
                            ],
                        },
                        "sub_field_b": {
                            "value": None,
                            "null_reason": "no_info_found",
                            "confidence_score": 0.9,
                            "citations": None,
                        },
                    }
                ]
            }
            for json_line in json.dumps(array_example, indent=2).split("\n"):
                lines.append(f"  {json_line}")
            lines.append("")

        return "\n".join(lines)
