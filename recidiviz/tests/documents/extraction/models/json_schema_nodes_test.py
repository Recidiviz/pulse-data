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
"""Tests for json_schema_nodes.py.

Covers the per-node `to_json_schema()` serialization (including emitted key
order), the `EnumJSONSchema.for_enum` factory, and the validators/invariants the
node classes enforce.
"""
import re
from enum import Enum
from unittest import TestCase

from recidiviz.documents.extraction.models.json_schema_nodes import (
    AnyOfJSONSchema,
    ArrayJSONSchema,
    ConstBooleanJSONSchema,
    EnumJSONSchema,
    JSONScalarType,
    ObjectJSONSchema,
    ScalarJSONSchema,
)

_DESCRIPTION = "A description that is long enough to be meaningful."


class _Color(Enum):
    """A throwaway enum for exercising `EnumJSONSchema.for_enum`."""

    RED = "red"
    GREEN = "green"
    BLUE = "blue"


class NodeSerializationTest(TestCase):
    """Tests `to_json_schema()` output and emitted key order."""

    def test_scalar_serialization(self) -> None:
        self.assertEqual(
            {"type": "integer", "description": _DESCRIPTION},
            ScalarJSONSchema(
                description=_DESCRIPTION, json_type=JSONScalarType.INTEGER
            ).to_json_schema(),
        )

    def test_nullable_scalar_serialization(self) -> None:
        self.assertEqual(
            {"type": ["string", "null"], "description": _DESCRIPTION},
            ScalarJSONSchema(
                description=_DESCRIPTION, json_type=JSONScalarType.STRING, nullable=True
            ).to_json_schema(),
        )

    def test_const_boolean_serialization(self) -> None:
        self.assertEqual(
            {"type": "boolean", "description": _DESCRIPTION, "const": False},
            ConstBooleanJSONSchema(
                description=_DESCRIPTION, value=False
            ).to_json_schema(),
        )
        self.assertEqual(
            {"type": "boolean", "description": _DESCRIPTION, "const": True},
            ConstBooleanJSONSchema(
                description=_DESCRIPTION, value=True
            ).to_json_schema(),
        )

    def test_enum_serialization(self) -> None:
        self.assertEqual(
            {"type": "string", "description": _DESCRIPTION, "enum": ["a", "b"]},
            EnumJSONSchema(
                description=_DESCRIPTION, values=["a", "b"]
            ).to_json_schema(),
        )

    def test_array_serialization_without_min_items(self) -> None:
        array_description = "Description of the array."
        item_description = "Description of an array item."
        self.assertEqual(
            {
                "type": "array",
                "description": array_description,
                "items": {"type": "string", "description": item_description},
            },
            ArrayJSONSchema(
                description=array_description,
                items=ScalarJSONSchema(
                    description=item_description, json_type=JSONScalarType.STRING
                ),
            ).to_json_schema(),
        )

    def test_array_serialization_with_min_items(self) -> None:
        schema = ArrayJSONSchema(
            description="Description of the array.",
            items=ScalarJSONSchema(
                description="Description of an array item.",
                json_type=JSONScalarType.STRING,
            ),
            min_items=1,
        ).to_json_schema()
        # minItems sits between description and items.
        self.assertEqual(["type", "description", "minItems", "items"], list(schema))
        self.assertEqual(1, schema["minItems"])

    def test_object_serialization(self) -> None:
        object_description = "Description of the object."
        a_description = "Description of property a."
        b_description = "Description of property b."
        schema = ObjectJSONSchema(
            description=object_description,
            properties={
                "a": ScalarJSONSchema(
                    description=a_description, json_type=JSONScalarType.STRING
                ),
                "b": ScalarJSONSchema(
                    description=b_description, json_type=JSONScalarType.INTEGER
                ),
            },
            required=["a"],
        ).to_json_schema()
        self.assertEqual(
            {
                "type": "object",
                "description": object_description,
                "properties": {
                    "a": {"type": "string", "description": a_description},
                    "b": {"type": "integer", "description": b_description},
                },
                "required": ["a"],
            },
            schema,
        )
        # Property order follows insertion order.
        self.assertEqual(["a", "b"], list(schema["properties"]))

    def test_anyof_serialization(self) -> None:
        string_branch_description = "Description of the string branch."
        integer_branch_description = "Description of the integer branch."
        schema = AnyOfJSONSchema(
            branches=[
                ScalarJSONSchema(
                    description=string_branch_description,
                    json_type=JSONScalarType.STRING,
                ),
                ScalarJSONSchema(
                    description=integer_branch_description,
                    json_type=JSONScalarType.INTEGER,
                ),
            ]
        ).to_json_schema()
        # `anyOf` is the only key — no description or other siblings.
        self.assertEqual(["anyOf"], list(schema))
        self.assertEqual(
            [
                {"type": "string", "description": string_branch_description},
                {"type": "integer", "description": integer_branch_description},
            ],
            schema["anyOf"],
        )

    def test_nested_serialization(self) -> None:
        object_description = "Description of the outer object."
        array_description = "Description of the array."
        item_description = "Description of an array item."
        name_description = "Description of the name property."
        self.assertEqual(
            {
                "type": "object",
                "description": object_description,
                "properties": {
                    "items": {
                        "type": "array",
                        "description": array_description,
                        "items": {
                            "type": "object",
                            "description": item_description,
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "description": name_description,
                                }
                            },
                            "required": ["name"],
                        },
                    }
                },
                "required": ["items"],
            },
            ObjectJSONSchema(
                description=object_description,
                properties={
                    "items": ArrayJSONSchema(
                        description=array_description,
                        items=ObjectJSONSchema(
                            description=item_description,
                            properties={
                                "name": ScalarJSONSchema(
                                    description=name_description,
                                    json_type=JSONScalarType.STRING,
                                )
                            },
                            required=["name"],
                        ),
                    )
                },
                required=["items"],
            ).to_json_schema(),
        )


class ForEnumTest(TestCase):
    """Tests the `EnumJSONSchema.for_enum` factory."""

    def test_for_enum_uses_member_values_in_definition_order(self) -> None:
        node = EnumJSONSchema.for_enum(enum_cls=_Color, description=_DESCRIPTION)
        self.assertEqual(["red", "green", "blue"], node.values)

    def test_for_enum_sets_description(self) -> None:
        node = EnumJSONSchema.for_enum(enum_cls=_Color, description=_DESCRIPTION)
        self.assertEqual(_DESCRIPTION, node.description)


class NodeValidationTest(TestCase):
    """Tests the validators and cross-field invariants the nodes enforce."""

    def test_object_required_not_in_properties_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Object schema marks ['ghost'] required, but they are not "
                "declared properties: ['a']."
            ),
        ):
            ObjectJSONSchema(
                description="Description of the object.",
                properties={
                    "a": ScalarJSONSchema(
                        description="Description of property a.",
                        json_type=JSONScalarType.STRING,
                    )
                },
                required=["ghost"],
            )

    def test_empty_description_raises(self) -> None:
        with self.assertRaises(ValueError):
            ScalarJSONSchema(description="", json_type=JSONScalarType.STRING)

    def test_enum_empty_values_raises(self) -> None:
        with self.assertRaises(ValueError):
            EnumJSONSchema(description=_DESCRIPTION, values=[])

    def test_anyof_empty_branches_raises(self) -> None:
        with self.assertRaises(ValueError):
            AnyOfJSONSchema(branches=[])

    def test_array_non_positive_min_items_raises(self) -> None:
        with self.assertRaises(ValueError):
            ArrayJSONSchema(
                description=_DESCRIPTION,
                items=ScalarJSONSchema(
                    description=_DESCRIPTION, json_type=JSONScalarType.STRING
                ),
                min_items=0,
            )

    def test_array_items_not_a_node_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "Expected a JSONSchemaNode"):
            ArrayJSONSchema(description=_DESCRIPTION, items="not a node")  # type: ignore[arg-type]

    def test_object_property_value_not_a_node_raises(self) -> None:
        with self.assertRaises(ValueError):
            ObjectJSONSchema(
                description=_DESCRIPTION,
                properties={"a": "not a node"},  # type: ignore[dict-item]
                required=[],
            )
