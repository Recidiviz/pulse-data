# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for llm_json_schema_generator.py.

Builds `LLMRequestOutputSchema`s inline (the same `_field`/`_build_schema`
pattern as llm_request_output_schema_test.py) and asserts the shape of the
generated JSON Schema dict — the outer relevance `anyOf`, the per-field
value/null `anyOf`, STRUCTURAL bare values, nested ARRAY_OF_STRUCT, and the
companion-metadata enums. Also asserts every real collection's schema is valid
JSON Schema and that the US_OZ employment schema matches a checked-in golden.
"""
import json
from typing import Any
from unittest import TestCase

from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    load_llm_extractor_collection_configs,
)
from recidiviz.documents.extraction.models.llm_json_schema_generator import (
    LLMJsonSchemaGenerator,
)
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ConfidenceLevel,
    NullReason,
)
from recidiviz.tests.ingest import fixtures
from recidiviz.utils.yaml_dict import YAMLDict

_DESCRIPTION = "A description that is long enough to be meaningful."
_COLLECTION_DESCRIPTION = "Extract employment information from case notes."


def _field(name: str, *, field_type: str = "STRING", **extra: Any) -> dict[str, Any]:
    return {"name": name, "type": field_type, "description": _DESCRIPTION, **extra}


def _build_schema(*user_fields: dict[str, Any]) -> LLMRequestOutputSchema:
    return LLMRequestOutputSchema.from_yaml_dict(
        yaml_dict=YAMLDict(
            {
                "full_batch_description": _DESCRIPTION,
                "result_level_description": _DESCRIPTION,
                "inferred_fields": list(user_fields),
            }
        ),
        collection_description=_COLLECTION_DESCRIPTION,
        default_minimum_confidence_level=ConfidenceLevel.INFERRED,
    )


def _generate(*user_fields: dict[str, Any]) -> dict[str, Any]:
    return LLMJsonSchemaGenerator.generate(_build_schema(*user_fields))


def _relevant_branch(schema: dict[str, Any]) -> dict[str, Any]:
    return schema["properties"]["result"]["anyOf"][1]


def _irrelevant_branch(schema: dict[str, Any]) -> dict[str, Any]:
    return schema["properties"]["result"]["anyOf"][0]


class TopLevelStructureTest(TestCase):
    """Tests the outer relevance `anyOf` placed on the `result` property."""

    def test_result_property_holds_irrelevant_then_relevant_anyof(self) -> None:
        schema = _generate(_field("a"))
        self.assertEqual(["result"], schema["required"])
        self.assertEqual(["anyOf"], list(schema["properties"]["result"]))
        branches = schema["properties"]["result"]["anyOf"]
        self.assertEqual(2, len(branches))
        self.assertEqual(
            "Use when the document is NOT relevant.", branches[0]["description"]
        )
        self.assertEqual(
            "Use when the document IS relevant.", branches[1]["description"]
        )
        # Root description is the result-level description from the schema.
        self.assertEqual(_DESCRIPTION, schema["description"])

    def test_irrelevant_branch_has_only_is_relevant(self) -> None:
        schema = _build_schema(_field("a"))
        branch = _irrelevant_branch(LLMJsonSchemaGenerator.generate(schema))
        self.assertEqual(["is_relevant"], branch["required"])
        self.assertEqual(
            {
                "is_relevant": {
                    "type": "boolean",
                    "description": schema.is_relevant_field.description,
                    "const": False,
                }
            },
            branch["properties"],
        )

    def test_relevant_branch_lists_is_relevant_then_user_fields_in_order(self) -> None:
        branch = _relevant_branch(_generate(_field("a"), _field("b"), _field("c")))
        self.assertEqual(["is_relevant", "a", "b", "c"], list(branch["properties"]))

    def test_relevant_branch_required_lists_only_required_fields(self) -> None:
        branch = _relevant_branch(
            _generate(
                _field("required_field", required=True),
                _field("optional_field"),
            )
        )
        self.assertEqual(["is_relevant", "required_field"], branch["required"])


class InferredFieldSchemaTest(TestCase):
    """Tests the per-field value/null `anyOf` emitted for INFERRED fields."""

    def test_inferred_scalar_field_is_value_null_anyof(self) -> None:
        field_schema = _relevant_branch(_generate(_field("note")))["properties"]["note"]
        value_branch, null_branch = field_schema["anyOf"]

        self.assertEqual(
            ["adversarial_interpretation", "value", "confidence_level", "citations"],
            list(value_branch["properties"]),
        )
        self.assertEqual(
            ["adversarial_interpretation", "value", "confidence_level", "citations"],
            value_branch["required"],
        )
        self.assertEqual(
            {"type": "string", "description": _DESCRIPTION},
            value_branch["properties"]["value"],
        )
        # Value branch requires at least one citation.
        self.assertEqual(1, value_branch["properties"]["citations"]["minItems"])

        self.assertEqual(
            [
                "adversarial_interpretation",
                "null_reason",
                "confidence_level",
                "citations",
            ],
            list(null_branch["properties"]),
        )
        # Null branch does not require citations.
        self.assertEqual(
            ["adversarial_interpretation", "null_reason", "confidence_level"],
            null_branch["required"],
        )
        self.assertNotIn("minItems", null_branch["properties"]["citations"])

    def test_value_branch_value_is_enum_for_enum_field(self) -> None:
        field_schema = _relevant_branch(
            _generate(_field("status", field_type="ENUM", values=["x", "y"]))
        )["properties"]["status"]
        value = field_schema["anyOf"][0]["properties"]["value"]
        self.assertEqual(
            {"type": "string", "description": _DESCRIPTION, "enum": ["x", "y"]}, value
        )

    def test_confidence_level_enum_lists_all_levels(self) -> None:
        field_schema = _relevant_branch(_generate(_field("note")))["properties"]["note"]
        expected = [level.value for level in ConfidenceLevel]
        for branch in field_schema["anyOf"]:
            self.assertEqual(expected, branch["properties"]["confidence_level"]["enum"])

    def test_null_reason_enum_lists_all_reasons(self) -> None:
        field_schema = _relevant_branch(_generate(_field("note")))["properties"]["note"]
        null_branch = field_schema["anyOf"][1]
        self.assertEqual(
            [reason.value for reason in NullReason],
            null_branch["properties"]["null_reason"]["enum"],
        )

    def test_adversarial_interpretation_is_nullable_and_first(self) -> None:
        field_schema = _relevant_branch(_generate(_field("note")))["properties"]["note"]
        for branch in field_schema["anyOf"]:
            self.assertEqual(
                "adversarial_interpretation", list(branch["properties"])[0]
            )
            self.assertEqual(
                ["string", "null"],
                branch["properties"]["adversarial_interpretation"]["type"],
            )


class StructuralFieldSchemaTest(TestCase):
    """Tests that STRUCTURAL fields are emitted as bare values."""

    def test_structural_scalar_is_bare_value(self) -> None:
        field_schema = _relevant_branch(
            _generate(_field("summary", field_mode="STRUCTURAL"))
        )["properties"]["summary"]
        self.assertEqual({"type": "string", "description": _DESCRIPTION}, field_schema)

    def test_structural_enum_is_bare_enum(self) -> None:
        field_schema = _relevant_branch(
            _generate(
                _field(
                    "kind",
                    field_type="ENUM",
                    values=["x", "y"],
                    field_mode="STRUCTURAL",
                )
            )
        )["properties"]["kind"]
        self.assertEqual(
            {"type": "string", "description": _DESCRIPTION, "enum": ["x", "y"]},
            field_schema,
        )

    def test_is_relevant_is_boolean_pinned_true_in_relevant_branch(self) -> None:
        schema = _build_schema(_field("a"))
        is_relevant = _relevant_branch(LLMJsonSchemaGenerator.generate(schema))[
            "properties"
        ]["is_relevant"]
        self.assertEqual(
            {
                "type": "boolean",
                "description": schema.is_relevant_field.description,
                "const": True,
            },
            is_relevant,
        )

    def test_scalar_type_mapping(self) -> None:
        for field_type, json_type in [
            ("STRING", "string"),
            ("BOOLEAN", "boolean"),
            ("INTEGER", "integer"),
            ("FLOAT", "number"),
        ]:
            with self.subTest(field_type=field_type):
                field_schema = _relevant_branch(
                    _generate(
                        _field("f", field_type=field_type, field_mode="STRUCTURAL")
                    )
                )["properties"]["f"]
                self.assertEqual(
                    {"type": json_type, "description": _DESCRIPTION}, field_schema
                )


class ArrayOfStructSchemaTest(TestCase):
    """Tests that ARRAY_OF_STRUCT fields are emitted as bare arrays of objects."""

    def _array_field(self, **extra: Any) -> dict[str, Any]:
        return _relevant_branch(
            _generate(
                _field(
                    "employers",
                    field_type="ARRAY_OF_STRUCT",
                    primary_keys=["employer_name"],
                    fields=[
                        _field("employer_name", required=True),
                        _field("job_title"),
                        _field(
                            "kind",
                            field_type="ENUM",
                            values=["a"],
                            field_mode="STRUCTURAL",
                        ),
                    ],
                    **extra,
                )
            )
        )["properties"]["employers"]

    def test_array_of_struct_is_bare_array_of_objects(self) -> None:
        array_schema = self._array_field()
        self.assertEqual("array", array_schema["type"])
        self.assertNotIn("anyOf", array_schema)
        items = array_schema["items"]
        self.assertEqual("object", items["type"])
        self.assertEqual(
            ["employer_name", "job_title", "kind"], list(items["properties"])
        )
        # Only the required sub-field is listed.
        self.assertEqual(["employer_name"], items["required"])

    def test_array_sub_fields_recurse_by_type_and_mode(self) -> None:
        items = self._array_field()["items"]["properties"]
        # INFERRED sub-field -> value/null anyOf.
        self.assertIn("anyOf", items["employer_name"])
        # STRUCTURAL enum sub-field -> bare enum.
        self.assertEqual(
            {"type": "string", "description": _DESCRIPTION, "enum": ["a"]},
            items["kind"],
        )


class GoldenSchemaTest(TestCase):
    """Pins the full generated schema for a real collection to a checked-in
    golden. Real-collection JSON Schema validity is guarded in
    llm_extractor_collection_config_test.py's ParseAllCollectionConfigsTest.
    """

    def test_playground_employment_schema_matches_golden(self) -> None:
        collection = load_llm_extractor_collection_configs()[
            "PLAYGROUND_EMPLOYMENT_INFO"
        ]
        with open(
            fixtures.as_filepath(
                "playground_employment_info_json_schema.json",
                subdir="fixtures/json_schema_generator",
            ),
            encoding="utf-8",
        ) as golden_file:
            expected = json.load(golden_file)
        self.assertEqual(expected, collection.generate_json_schema())
