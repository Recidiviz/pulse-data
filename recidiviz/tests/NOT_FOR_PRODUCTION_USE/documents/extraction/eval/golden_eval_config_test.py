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
"""Tests for GoldenEvalConfig and ArrayOfStructEvalConfig parsing."""
import os
import tempfile
import unittest

from google.cloud import bigquery

from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction import (
    document_extractor_configs,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.eval.golden_eval_config import (
    ArrayOfStructEvalConfig,
    GoldenEvalConfig,
)

_TEST_SOURCE_URI = "https://example.com/test-eval-sheet"


def _write_yaml(content: str) -> str:
    """Writes |content| to a temp yaml file and returns its path."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write(content)
        return tmp.name


class TestArrayOfStructEvalConfig(unittest.TestCase):
    """Tests for ArrayOfStructEvalConfig validation."""

    def test_requires_primary_key_cols(self) -> None:
        with self.assertRaises(ValueError):
            ArrayOfStructEvalConfig(field_name="employers", primary_key_cols=[])

    def test_single_key(self) -> None:
        config = ArrayOfStructEvalConfig(
            field_name="employers", primary_key_cols=["employer_name"]
        )
        self.assertEqual(config.primary_key_cols, ["employer_name"])

    def test_composite_key(self) -> None:
        config = ArrayOfStructEvalConfig(
            field_name="employment_changes",
            primary_key_cols=["employment_change_type", "employment_change_date"],
        )
        self.assertEqual(
            config.primary_key_cols,
            ["employment_change_type", "employment_change_date"],
        )


class TestGoldenEvalConfig(unittest.TestCase):
    """Tests for GoldenEvalConfig parsing."""

    def test_from_yaml_minimal(self) -> None:
        yaml_content = f"""\
source_uri: "{_TEST_SOURCE_URI}"
accuracy_threshold: 0.80
"""
        path = _write_yaml(yaml_content)
        schema = GoldenEvalConfig.from_yaml(path)
        os.unlink(path)
        self.assertEqual(schema.source_uri, _TEST_SOURCE_URI)
        self.assertAlmostEqual(schema.accuracy_threshold, 0.80)
        self.assertEqual(schema.array_of_struct_configs, {})
        # No collection.yaml sibling → only fixed metadata columns
        column_names = [c.name for c in schema.columns]
        self.assertIn("test_type", column_names)
        self.assertIn("document_id", column_names)
        self.assertIn("is_validated", column_names)
        self.assertNotIn("is_relevant__expected", column_names)

    def test_from_yaml_with_array_configs(self) -> None:
        yaml_content = f"""\
source_uri: "{_TEST_SOURCE_URI}"
accuracy_threshold: 0.85

array_of_struct_field_configs:
  employers:
    primary_key_cols: [employer_name]
  employment_changes:
    primary_key_cols: [employment_change_type, employment_change_date]
"""
        path = _write_yaml(yaml_content)
        schema = GoldenEvalConfig.from_yaml(path)
        os.unlink(path)

        self.assertAlmostEqual(schema.accuracy_threshold, 0.85)
        self.assertEqual(
            schema.array_of_struct_configs["employers"].primary_key_cols,
            ["employer_name"],
        )
        self.assertEqual(
            schema.array_of_struct_configs["employment_changes"].primary_key_cols,
            ["employment_change_type", "employment_change_date"],
        )

    def test_from_yaml_collection_eval_schemas(self) -> None:
        """Smoke test: all three collection golden_eval_config.yaml files parse correctly."""
        collections_dir = os.path.join(
            os.path.dirname(document_extractor_configs.__file__),
            "config",
            "collections",
            "llm_prompt",
        )
        for collection_name in [
            "case_note_employment_info",
            "case_note_housing_info",
            "case_note_payment_info",
        ]:
            with self.subTest(collection=collection_name):
                eval_schema_path = os.path.join(
                    collections_dir, collection_name, "golden_eval_config.yaml"
                )
                self.assertTrue(
                    os.path.exists(eval_schema_path),
                    msg=f"golden_eval_config.yaml not found at {eval_schema_path}",
                )
                schema = GoldenEvalConfig.from_yaml(eval_schema_path)
                self.assertGreater(schema.accuracy_threshold, 0.0)
                self.assertTrue(schema.source_uri.startswith("https://"))

                column_names = [c.name for c in schema.columns]
                self.assertEqual(column_names[0], "test_type")
                self.assertEqual(column_names[1], "document_id")
                self.assertIn("is_relevant__expected", column_names)
                self.assertIn("is_validated", column_names)
                self.assertEqual(column_names[-1], "test_case_description")

    def test_derived_columns_employment(self) -> None:
        """Derived columns include all expected fields in the right order."""
        collections_dir = os.path.join(
            os.path.dirname(document_extractor_configs.__file__),
            "config",
            "collections",
            "llm_prompt",
        )
        schema = GoldenEvalConfig.from_yaml(
            os.path.join(
                collections_dir, "case_note_employment_info", "golden_eval_config.yaml"
            )
        )
        column_names = [c.name for c in schema.columns]
        # Fixed prefix
        self.assertEqual(
            column_names[:4],
            ["test_type", "document_id", "state_code", "document_text"],
        )
        # Expected columns from collection schema (is_relevant first, then inferred fields)
        self.assertIn("is_relevant__expected", column_names)
        self.assertIn("primary_status__expected", column_names)
        self.assertIn("employers__expected", column_names)
        self.assertIn("employment_changes__expected", column_names)
        # Fixed suffix
        self.assertEqual(
            column_names[-4:],
            ["difficulty", "is_validated", "test_case", "test_case_description"],
        )
        # Column types
        col_by_name = {c.name: c for c in schema.columns}
        self.assertEqual(
            col_by_name["is_relevant__expected"].field_type,
            bigquery.SqlTypeNames.BOOLEAN,
        )
        self.assertEqual(
            col_by_name["primary_status__expected"].field_type,
            bigquery.SqlTypeNames.STRING,
        )
        # ARRAY_OF_STRUCT fields are stored as STRING (JSON-encoded) in BQ.
        self.assertEqual(
            col_by_name["employers__expected"].field_type, bigquery.SqlTypeNames.STRING
        )

    def test_array_of_struct_configs_employment(self) -> None:
        """array_of_struct_configs are parsed from the yaml."""
        collections_dir = os.path.join(
            os.path.dirname(document_extractor_configs.__file__),
            "config",
            "collections",
            "llm_prompt",
        )
        schema = GoldenEvalConfig.from_yaml(
            os.path.join(
                collections_dir, "case_note_employment_info", "golden_eval_config.yaml"
            )
        )
        self.assertEqual(
            schema.array_of_struct_configs["employers"].primary_key_cols,
            ["employer_name"],
        )
        self.assertEqual(
            schema.array_of_struct_configs["employment_changes"].primary_key_cols,
            ["employment_change_type", "employment_change_date"],
        )


if __name__ == "__main__":
    unittest.main()
