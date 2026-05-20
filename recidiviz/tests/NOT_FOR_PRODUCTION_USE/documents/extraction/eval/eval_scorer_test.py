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
"""Unit tests for eval_scorer using synthetic data (no LLM calls or BQ access)."""
import json
import unittest

from recidiviz.big_query.big_query_view_column import String
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.eval.eval_scorer import (
    MatchStrategy,
    score_array_of_struct,
    score_document,
    score_flat_field,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.eval.golden_eval_config import (
    ArrayOfStructEvalConfig,
    GoldenEvalConfig,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.extraction_output_schema import (
    RESERVED_FIELD_NAME_IS_RELEVANT,
    ExtractionFieldType,
    ExtractionInferredField,
    ExtractionOutputSchema,
)

# ---------------------------------------------------------------------------
# Synthetic schema helpers
# ---------------------------------------------------------------------------

_EMPLOYER_STRUCT_FIELDS = (
    ExtractionInferredField(
        name="employer_name",
        field_type=ExtractionFieldType.STRING,
        unaugmented_description="Employer name.",
        required=True,
    ),
    ExtractionInferredField(
        name="employment_type",
        field_type=ExtractionFieldType.ENUM,
        unaugmented_description="Employment type.",
        required=False,
        enum_values=("full_time", "part_time"),
    ),
)

_IS_RELEVANT_FIELD = ExtractionInferredField(
    name=RESERVED_FIELD_NAME_IS_RELEVANT,
    field_type=ExtractionFieldType.BOOLEAN,
    unaugmented_description="Whether the document is relevant.",
    required=True,
)

_STATUS_FIELD = ExtractionInferredField(
    name="status",
    field_type=ExtractionFieldType.ENUM,
    unaugmented_description="Employment status.",
    required=False,
    enum_values=("employed", "unemployed"),
)

_NOTES_FIELD = ExtractionInferredField(
    name="notes",
    field_type=ExtractionFieldType.STRING,
    unaugmented_description="Free-text notes.",
    required=False,
)

_EMPLOYERS_FIELD = ExtractionInferredField(
    name="employers",
    field_type=ExtractionFieldType.ARRAY_OF_STRUCT,
    unaugmented_description="List of employers.",
    required=False,
    struct_fields=_EMPLOYER_STRUCT_FIELDS,
)


def _make_output_schema(
    *extra_fields: ExtractionInferredField,
) -> ExtractionOutputSchema:
    fields_by_name = {RESERVED_FIELD_NAME_IS_RELEVANT: _IS_RELEVANT_FIELD}
    for f in extra_fields:
        fields_by_name[f.name] = f
    return ExtractionOutputSchema(
        full_batch_description="Test batch.",
        result_level_description="Test result.",
        inferred_fields_by_name=fields_by_name,
    )


def _make_eval_config(
    output_schema: ExtractionOutputSchema | None = None,
    array_configs: dict[str, ArrayOfStructEvalConfig] | None = None,
) -> GoldenEvalConfig:
    return GoldenEvalConfig(
        source_uri="https://example.com/sheet",
        accuracy_threshold=0.80,
        columns=[String(name="document_id", description="ID", mode="REQUIRED")],
        array_of_struct_configs=array_configs or {},
        output_schema=output_schema,
    )


_EMPLOYERS_ARRAY_CONFIG = ArrayOfStructEvalConfig(
    field_name="employers",
    primary_key_cols=["employer_name"],
)


# ---------------------------------------------------------------------------
# score_flat_field
# ---------------------------------------------------------------------------


class TestScoreFlatField(unittest.TestCase):
    """Tests for score_flat_field, which applies the appropriate matching strategy for non-ARRAY_OF_STRUCT fields based on the field type defined in the output schema."""

    def test_exact_match(self) -> None:
        self.assertTrue(score_flat_field("employed", "employed", MatchStrategy.EXACT))

    def test_exact_mismatch(self) -> None:
        self.assertFalse(
            score_flat_field("employed", "unemployed", MatchStrategy.EXACT)
        )

    def test_exact_case_insensitive(self) -> None:
        self.assertTrue(score_flat_field("Employed", "employed", MatchStrategy.EXACT))

    def test_both_none(self) -> None:
        self.assertTrue(score_flat_field(None, None, MatchStrategy.EXACT))
        self.assertTrue(score_flat_field(None, None, MatchStrategy.FUZZY))

    def test_one_sided_none(self) -> None:
        self.assertFalse(score_flat_field("employed", None, MatchStrategy.EXACT))
        self.assertFalse(score_flat_field(None, "employed", MatchStrategy.FUZZY))

    def test_fuzzy_exact_string(self) -> None:
        self.assertTrue(
            score_flat_field(
                "Black Pine Cabinetry", "Black Pine Cabinetry", MatchStrategy.FUZZY
            )
        )

    def test_fuzzy_minor_typo(self) -> None:
        # One character different — should still exceed the threshold.
        self.assertTrue(
            score_flat_field(
                "Black Pine Cabinetry", "Black Pine Cabintery", MatchStrategy.FUZZY
            )
        )

    def test_fuzzy_bigger_typo(self) -> None:
        # Multiple characters different — should still exceed the threshold.
        self.assertTrue(
            score_flat_field(
                "Black Pine Cabinetry", "Black Pine Cabinets", MatchStrategy.FUZZY
            )
        )

    def test_fuzzy_completely_different(self) -> None:
        self.assertFalse(
            score_flat_field("Black Pine Cabinetry", "Acme Corp", MatchStrategy.FUZZY)
        )


# ---------------------------------------------------------------------------
# score_array_of_struct
# ---------------------------------------------------------------------------


class TestScoreArrayOfStruct(unittest.TestCase):
    """
    Tests for score_array_of_struct, which compares expected and actual lists of dicts
    based on primary key pairing and per-field match strategies.
    """

    def _config(self) -> ArrayOfStructEvalConfig:
        return _EMPLOYERS_ARRAY_CONFIG

    def test_perfect_match(self) -> None:
        expected = [{"employer_name": "Acme", "employment_type": "full_time"}]
        actual = [{"employer_name": "Acme", "employment_type": "full_time"}]
        self.assertTrue(
            score_array_of_struct(
                expected, actual, self._config(), _EMPLOYER_STRUCT_FIELDS
            )
        )

    def test_empty_arrays_match(self) -> None:
        self.assertTrue(
            score_array_of_struct([], [], self._config(), _EMPLOYER_STRUCT_FIELDS)
        )

    def test_extra_actual_element(self) -> None:
        expected = [{"employer_name": "Acme", "employment_type": "full_time"}]
        actual = [
            {"employer_name": "Acme", "employment_type": "full_time"},
            {"employer_name": "Extra Co", "employment_type": "part_time"},
        ]
        self.assertFalse(
            score_array_of_struct(
                expected, actual, self._config(), _EMPLOYER_STRUCT_FIELDS
            )
        )

    def test_missing_expected_element(self) -> None:
        expected = [
            {"employer_name": "Acme", "employment_type": "full_time"},
            {"employer_name": "Other Inc", "employment_type": "part_time"},
        ]
        actual = [{"employer_name": "Acme", "employment_type": "full_time"}]
        self.assertFalse(
            score_array_of_struct(
                expected, actual, self._config(), _EMPLOYER_STRUCT_FIELDS
            )
        )

    def test_key_mismatch(self) -> None:
        expected = [{"employer_name": "Acme", "employment_type": "full_time"}]
        actual = [{"employer_name": "Wrong Corp", "employment_type": "full_time"}]
        self.assertFalse(
            score_array_of_struct(
                expected, actual, self._config(), _EMPLOYER_STRUCT_FIELDS
            )
        )

    def test_non_key_subfield_mismatch(self) -> None:
        expected = [{"employer_name": "Acme", "employment_type": "full_time"}]
        actual = [{"employer_name": "Acme", "employment_type": "part_time"}]
        self.assertFalse(
            score_array_of_struct(
                expected, actual, self._config(), _EMPLOYER_STRUCT_FIELDS
            )
        )

    def test_composite_key_match(self) -> None:
        config = ArrayOfStructEvalConfig(
            field_name="changes",
            primary_key_cols=["change_type", "change_date"],
        )
        struct_fields = (
            ExtractionInferredField(
                name="change_type",
                field_type=ExtractionFieldType.ENUM,
                unaugmented_description="",
                required=True,
                enum_values=("hired", "fired"),
            ),
            ExtractionInferredField(
                name="change_date",
                field_type=ExtractionFieldType.STRING,
                unaugmented_description="",
                required=False,
            ),
            ExtractionInferredField(
                name="notes",
                field_type=ExtractionFieldType.STRING,
                unaugmented_description="",
                required=False,
            ),
        )
        expected = [
            {"change_type": "hired", "change_date": "2024-01-01", "notes": "start"}
        ]
        actual = [
            {"change_type": "hired", "change_date": "2024-01-01", "notes": "start"}
        ]
        self.assertTrue(score_array_of_struct(expected, actual, config, struct_fields))

    def test_fuzzy_match_on_string_key(self) -> None:
        # STRING key fields use fuzzy matching, so a minor typo in employer_name still pairs.
        expected = [
            {"employer_name": "Black Pine Cabinetry", "employment_type": "full_time"}
        ]
        actual = [
            {"employer_name": "Black Pine Cabintery", "employment_type": "full_time"}
        ]
        self.assertTrue(
            score_array_of_struct(
                expected, actual, self._config(), _EMPLOYER_STRUCT_FIELDS
            )
        )

    def test_fuzzy_match_on_non_key_string_subfield(self) -> None:
        # Fuzzy also applies to non-key STRING sub-fields.
        config = ArrayOfStructEvalConfig(
            field_name="employers",
            primary_key_cols=["employer_id"],
        )
        struct_fields = (
            ExtractionInferredField(
                name="employer_id",
                field_type=ExtractionFieldType.STRING,
                unaugmented_description="",
                required=True,
            ),
            ExtractionInferredField(
                name="employer_name",
                field_type=ExtractionFieldType.STRING,
                unaugmented_description="",
                required=False,
            ),
        )
        expected = [{"employer_id": "e1", "employer_name": "Black Pine Cabinetry"}]
        actual = [{"employer_id": "e1", "employer_name": "Black Pine Cabintery"}]
        self.assertTrue(score_array_of_struct(expected, actual, config, struct_fields))


# ---------------------------------------------------------------------------
# score_document
# ---------------------------------------------------------------------------


class TestScoreDocument(unittest.TestCase):
    """
    Tests for score_document, which compares expected and actual document-level fields.
    """

    def _config(self) -> GoldenEvalConfig:
        return _make_eval_config(_make_output_schema(_STATUS_FIELD, _NOTES_FIELD))

    def test_relevant_all_fields_correct(self) -> None:
        expected = {
            "document_id": "doc1",
            "is_relevant__expected": True,
            "status__expected": "employed",
            "notes__expected": "All good.",
        }
        actual = {
            "document_id": "doc1",
            "is_relevant": True,
            "status": "employed",
            "notes": "All good.",
        }
        result = score_document(expected, actual, self._config())
        self.assertTrue(result.is_relevant_expected)
        self.assertTrue(result.is_relevant_actual)
        self.assertTrue(result.field_scores["status"].is_match)
        self.assertTrue(result.field_scores["notes"].is_match)

    def test_relevant_one_field_wrong(self) -> None:
        expected = {
            "document_id": "doc1",
            "is_relevant__expected": True,
            "status__expected": "employed",
            "notes__expected": "All good.",
        }
        actual = {
            "document_id": "doc1",
            "is_relevant": True,
            "status": "unemployed",
            "notes": "All good.",
        }
        result = score_document(expected, actual, self._config())
        self.assertFalse(result.field_scores["status"].is_match)
        self.assertTrue(result.field_scores["notes"].is_match)

    def test_not_relevant_no_field_scores(self) -> None:
        expected = {
            "document_id": "doc1",
            "is_relevant__expected": False,
            "status__expected": None,
            "notes__expected": None,
        }
        actual = {
            "document_id": "doc1",
            "is_relevant": False,
            "status": None,
            "notes": None,
        }
        result = score_document(expected, actual, self._config())
        self.assertFalse(result.is_relevant_expected)
        self.assertEqual(result.field_scores, {})

    def test_no_actual_result(self) -> None:
        expected = {
            "document_id": "doc1",
            "is_relevant__expected": True,
            "status__expected": "employed",
        }
        result = score_document(expected, None, self._config())
        self.assertIsNone(result.is_relevant_actual)
        self.assertEqual(result.field_scores, {})

    def test_array_of_struct_field(self) -> None:
        config = _make_eval_config(
            _make_output_schema(_EMPLOYERS_FIELD),
            {"employers": _EMPLOYERS_ARRAY_CONFIG},
        )
        expected = {
            "document_id": "doc1",
            "is_relevant__expected": True,
            "employers__expected": json.dumps(
                [{"employer_name": "Acme", "employment_type": "full_time"}]
            ),
        }
        actual = {
            "document_id": "doc1",
            "is_relevant": True,
            "employers": [{"employer_name": "Acme", "employment_type": "full_time"}],
        }
        result = score_document(expected, actual, config)
        self.assertTrue(result.field_scores["employers"].is_match)

    def test_array_of_struct_mismatch(self) -> None:
        config = _make_eval_config(
            _make_output_schema(_EMPLOYERS_FIELD),
            {"employers": _EMPLOYERS_ARRAY_CONFIG},
        )
        expected = {
            "document_id": "doc1",
            "is_relevant__expected": True,
            "employers__expected": json.dumps(
                [{"employer_name": "Acme", "employment_type": "full_time"}]
            ),
        }
        actual = {
            "document_id": "doc1",
            "is_relevant": True,
            "employers": [
                {"employer_name": "Wrong Corp", "employment_type": "full_time"}
            ],
        }
        result = score_document(expected, actual, config)
        self.assertFalse(result.field_scores["employers"].is_match)

    def test_empty_expected_array(self) -> None:
        config = _make_eval_config(
            _make_output_schema(_EMPLOYERS_FIELD),
            {"employers": _EMPLOYERS_ARRAY_CONFIG},
        )
        expected = {
            "document_id": "doc1",
            "is_relevant__expected": True,
            "employers__expected": "[]",
        }
        actual = {"document_id": "doc1", "is_relevant": True, "employers": []}
        result = score_document(expected, actual, config)
        self.assertTrue(result.field_scores["employers"].is_match)

    def test_is_relevant_string_true(self) -> None:
        expected = {
            "document_id": "doc1",
            "is_relevant__expected": "true",
            "status__expected": "employed",
            "notes__expected": None,
        }
        actual = {
            "document_id": "doc1",
            "is_relevant": True,
            "status": "employed",
            "notes": None,
        }
        result = score_document(expected, actual, self._config())
        self.assertTrue(result.is_relevant_expected)


if __name__ == "__main__":
    unittest.main()
