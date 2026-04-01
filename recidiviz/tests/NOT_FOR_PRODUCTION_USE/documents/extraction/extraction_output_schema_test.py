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
"""Tests for ExtractionInferredField and ExtractionOutputSchema semantic
consistency constraints (allowed_if_nonnull, allowed_if_value,
allowed_value_combinations)."""
import io
import unittest

import yaml

from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.extraction_output_schema import (
    ExtractionFieldType,
    ExtractionInferredField,
    ExtractionOutputSchema,
)
from recidiviz.utils.yaml_dict import YAMLDict


class TestExtractionInferredFieldSemanticConstraints(unittest.TestCase):
    """Tests for semantic constraint attrs on ExtractionInferredField."""

    def test_allowed_if_value_appends_description(self) -> None:
        field = ExtractionInferredField(
            name="employer_name",
            field_type=ExtractionFieldType.STRING,
            unaugmented_description="Name of the employer.",
            required=False,
            allowed_if_value={"primary_status": ("employed",)},
        )
        self.assertIn(
            "Only valid when 'primary_status' is one of ['employed'].",
            field.description,
        )

    def test_allowed_if_nonnull_appends_description(self) -> None:
        field = ExtractionInferredField(
            name="change_date",
            field_type=ExtractionFieldType.STRING,
            unaugmented_description="Date of the change.",
            required=False,
            allowed_if_nonnull="change_type",
        )
        self.assertIn(
            "Only valid when 'change_type' has a value.",
            field.description,
        )

    def test_allowed_value_combinations_appends_description(self) -> None:
        field = ExtractionInferredField(
            name="change_type",
            field_type=ExtractionFieldType.ENUM,
            unaugmented_description="Type of change.",
            required=False,
            enum_values=("hired", "fired"),
            allowed_value_combinations={
                "status": {
                    "employed": ("hired", "fired"),
                    "unemployed": ("fired",),
                }
            },
        )
        self.assertIn("When 'status' is 'employed'", field.description)
        self.assertIn("When 'status' is 'unemployed'", field.description)

    def test_allowed_value_combinations_rejects_non_enum(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            ExtractionInferredField(
                name="my_field",
                field_type=ExtractionFieldType.STRING,
                unaugmented_description="A field.",
                required=False,
                allowed_value_combinations={"other": {"x": ("a",)}},
            )
        self.assertIn("not ENUM", str(ctx.exception))

    def test_allowed_value_combinations_rejects_value_not_in_enum(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            ExtractionInferredField(
                name="status",
                field_type=ExtractionFieldType.ENUM,
                unaugmented_description="Status.",
                required=False,
                enum_values=("a", "b"),
                allowed_value_combinations={"other": {"x": ("c",)}},
            )
        self.assertIn("'c'", str(ctx.exception))
        self.assertIn("not in enum values", str(ctx.exception))

    def test_allowed_if_nonnull_rejects_self_reference(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            ExtractionInferredField(
                name="my_field",
                field_type=ExtractionFieldType.STRING,
                unaugmented_description="A field.",
                required=False,
                allowed_if_nonnull="my_field",
            )
        self.assertIn("referencing itself", str(ctx.exception))

    def test_allowed_if_value_rejects_self_reference(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            ExtractionInferredField(
                name="my_field",
                field_type=ExtractionFieldType.STRING,
                unaugmented_description="A field.",
                required=False,
                allowed_if_value={"my_field": ("x",)},
            )
        self.assertIn("referencing itself", str(ctx.exception))

    def test_allowed_value_combinations_rejects_self_reference(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            ExtractionInferredField(
                name="status",
                field_type=ExtractionFieldType.ENUM,
                unaugmented_description="Status.",
                required=False,
                enum_values=("a", "b"),
                allowed_value_combinations={"status": {"a": ("b",)}},
            )
        self.assertIn("referencing itself", str(ctx.exception))

    def test_from_yaml_dict_allowed_if_value_scalar(self) -> None:
        yaml_dict = YAMLDict(
            {
                "name": "employer_name",
                "type": "STRING",
                "description": "Name of the employer.",
                "allowed_if_value": {"primary_status": "employed"},
            }
        )
        field = ExtractionInferredField.from_yaml_dict(yaml_dict)
        assert field.allowed_if_value is not None
        self.assertEqual(field.allowed_if_value["primary_status"], ("employed",))

    def test_from_yaml_dict_allowed_if_value_list(self) -> None:
        yaml_dict = YAMLDict(
            {
                "name": "added_info",
                "type": "STRING",
                "description": "Additional info.",
                "allowed_if_value": {"primary_status": ["housed", "unhoused"]},
            }
        )
        field = ExtractionInferredField.from_yaml_dict(yaml_dict)
        assert field.allowed_if_value is not None
        self.assertEqual(
            field.allowed_if_value["primary_status"], ("housed", "unhoused")
        )

    def test_from_yaml_dict_allowed_if_nonnull(self) -> None:
        yaml_dict = YAMLDict(
            {
                "name": "change_date",
                "type": "STRING",
                "description": "Date of the change.",
                "allowed_if_nonnull": "change_type",
            }
        )
        field = ExtractionInferredField.from_yaml_dict(yaml_dict)
        self.assertEqual(field.allowed_if_nonnull, "change_type")

    def test_from_yaml_dict_allowed_value_combinations(self) -> None:
        yaml_dict = YAMLDict(
            {
                "name": "change_type",
                "type": "ENUM",
                "description": "Type of change.",
                "values": ["hired", "fired", "quit"],
                "allowed_value_combinations": {
                    "employment_status": {
                        "employed": ["hired", "fired", "quit"],
                        "unemployed": ["fired", "quit"],
                    }
                },
            }
        )
        field = ExtractionInferredField.from_yaml_dict(yaml_dict)
        assert field.allowed_value_combinations is not None
        self.assertIn("employment_status", field.allowed_value_combinations)
        self.assertEqual(
            field.allowed_value_combinations["employment_status"]["employed"],
            ("hired", "fired", "quit"),
        )
        self.assertEqual(
            field.allowed_value_combinations["employment_status"]["unemployed"],
            ("fired", "quit"),
        )


class TestExtractionOutputSchemaFieldSiblingValidation(unittest.TestCase):
    """Tests that ExtractionOutputSchema validates semantic constraint references."""

    def _make_schema_yaml(
        self,
        inferred_fields: list[dict],
    ) -> YAMLDict:
        raw = {
            "full_batch_description": "Test batch.",
            "result_level_description": "Test result.",
            "inferred_fields": inferred_fields,
        }
        yaml_str = yaml.dump(raw, default_flow_style=False)
        return YAMLDict.from_io(io.StringIO(yaml_str))

    def test_valid_allowed_if_value_references(self) -> None:
        schema = ExtractionOutputSchema.from_yaml_dict(
            self._make_schema_yaml(
                [
                    {
                        "name": "primary_status",
                        "type": "ENUM",
                        "description": "Status.",
                        "values": ["employed", "unemployed"],
                        "required": True,
                    },
                    {
                        "name": "employer_name",
                        "type": "STRING",
                        "description": "Employer.",
                        "allowed_if_value": {"primary_status": "employed"},
                    },
                ]
            ),
            collection_description="Test collection",
        )
        self.assertIn("employer_name", schema.inferred_fields_by_name)

    def test_invalid_allowed_if_value_reference_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            ExtractionOutputSchema.from_yaml_dict(
                self._make_schema_yaml(
                    [
                        {
                            "name": "employer_name",
                            "type": "STRING",
                            "description": "Employer.",
                            "allowed_if_value": {
                                "nonexistent_field": "x",
                            },
                        },
                    ]
                ),
                collection_description="Test collection",
            )
        self.assertIn("nonexistent_field", str(ctx.exception))
        self.assertIn("does not exist as a sibling", str(ctx.exception))

    def test_invalid_allowed_if_nonnull_reference_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            ExtractionOutputSchema.from_yaml_dict(
                self._make_schema_yaml(
                    [
                        {
                            "name": "change_date",
                            "type": "STRING",
                            "description": "Date.",
                            "allowed_if_nonnull": "nonexistent_field",
                        },
                    ]
                ),
                collection_description="Test collection",
            )
        self.assertIn("nonexistent_field", str(ctx.exception))
        self.assertIn("does not exist as a sibling", str(ctx.exception))

    def test_invalid_sub_field_reference_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            ExtractionOutputSchema.from_yaml_dict(
                self._make_schema_yaml(
                    [
                        {
                            "name": "items",
                            "type": "ARRAY_OF_STRUCT",
                            "description": "Items.",
                            "fields": [
                                {
                                    "name": "status",
                                    "type": "ENUM",
                                    "description": "Status.",
                                    "values": ["a", "b"],
                                },
                                {
                                    "name": "detail",
                                    "type": "STRING",
                                    "description": "Detail.",
                                    "allowed_if_value": {
                                        "bad_ref": "x",
                                    },
                                },
                            ],
                        },
                    ]
                ),
                collection_description="Test collection",
            )
        self.assertIn("bad_ref", str(ctx.exception))
        self.assertIn("ARRAY_OF_STRUCT 'items'", str(ctx.exception))

    def test_allowed_if_value_on_array_of_struct(self) -> None:
        """An ARRAY_OF_STRUCT field itself can have allowed_if_value referencing
        top-level siblings."""
        schema = ExtractionOutputSchema.from_yaml_dict(
            self._make_schema_yaml(
                [
                    {
                        "name": "primary_status",
                        "type": "ENUM",
                        "description": "Status.",
                        "values": ["employed", "unemployed"],
                        "required": True,
                    },
                    {
                        "name": "employers",
                        "type": "ARRAY_OF_STRUCT",
                        "description": "Employers.",
                        "allowed_if_value": {"primary_status": "employed"},
                        "fields": [
                            {
                                "name": "employer_name",
                                "type": "STRING",
                                "description": "Name.",
                            },
                        ],
                    },
                ]
            ),
            collection_description="Test collection",
        )
        employers_field = schema.inferred_fields_by_name["employers"]
        assert employers_field.allowed_if_value is not None
        self.assertIn("primary_status", employers_field.allowed_if_value)
