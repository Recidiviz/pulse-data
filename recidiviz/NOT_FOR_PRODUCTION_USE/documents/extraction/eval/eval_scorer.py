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
"""Eval scorer for document extraction collections.

Compares expected values (from the golden eval sheet) against actual extracted
values and produces per-field and per-document scores.  No LLM calls — the
scorer operates on already-extracted data.

Input formats
-------------
expected_row: dict[str, Any]
    One dict per golden eval row.  Keys use the ``{field}__expected`` convention
    (matching the eval BQ source table).  ARRAY_OF_STRUCT fields are JSON strings
    (as stored in the sheet/BQ).  Scalar fields are native Python types.

actual_row: dict[str, Any]
    One dict per extraction result row.  Keys use bare field names (no suffix).
    ARRAY_OF_STRUCT fields are lists of dicts (already deserialized).
    Must include the ``is_relevant`` key (read from the unvalidated view).

Scoring rules
-------------
- STRING fields:  fuzzy match via difflib.SequenceMatcher (ratio >= FUZZY_MATCH_THRESHOLD).
- All other flat fields (ENUM, BOOLEAN, INTEGER, FLOAT): case-insensitive exact match.
- ARRAY_OF_STRUCT fields: elements are matched by composite primary_key_cols; a field
  scores True only when expected and actual arrays have the same cardinality and every
  matched element agrees on all non-key sub-fields (using per-sub-field strategies above).
- Both expected and actual null → match. One-sided null → no match.
"""
import difflib
import enum
import json
from dataclasses import dataclass, field
from typing import Any

from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.eval.golden_eval_config import (
    ArrayOfStructEvalConfig,
    GoldenEvalConfig,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.extraction_output_schema import (
    RESERVED_FIELD_NAME_IS_RELEVANT,
    ExtractionFieldType,
    ExtractionInferredField,
)

FUZZY_MATCH_THRESHOLD = 0.85


class MatchStrategy(enum.Enum):
    """Comparison strategy for a single field value."""

    EXACT = "exact"
    FUZZY = "fuzzy"


FIELD_TYPE_TO_STRATEGY: dict[ExtractionFieldType, MatchStrategy] = {
    ExtractionFieldType.STRING: MatchStrategy.FUZZY,
    ExtractionFieldType.ENUM: MatchStrategy.EXACT,
    ExtractionFieldType.BOOLEAN: MatchStrategy.EXACT,
    ExtractionFieldType.INTEGER: MatchStrategy.EXACT,
    ExtractionFieldType.FLOAT: MatchStrategy.EXACT,
}


@dataclass(frozen=True)
class FieldScore:
    field_name: str
    is_match: bool
    expected: Any
    actual: Any


@dataclass(frozen=True)
class DocumentScore:
    document_id: str
    is_relevant_expected: bool
    # None when the document has no corresponding actual result row.
    is_relevant_actual: bool | None
    # Only populated when is_relevant_expected=True and an actual result exists.
    field_scores: dict[str, FieldScore] = field(default_factory=dict)


def _coerce_numeric(val: Any) -> Any:
    """Normalizes integers, floats, and numeric strings to float.

    Allows "8", 8, and 8.0 to compare equal regardless of how the LLM
    serialized the value vs. how it was entered in the eval sheet.
    Booleans are excluded to avoid 0/1 conflation with False/True.
    """
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        try:
            return float(val)
        except ValueError:
            return val
    return val


def score_flat_field(expected: Any, actual: Any, strategy: MatchStrategy) -> bool:
    """Returns True if expected and actual match according to strategy."""
    if expected is None and actual is None:
        return True
    if expected is None or actual is None:
        return False
    expected = _coerce_numeric(expected)
    actual = _coerce_numeric(actual)
    exp_str = str(expected).lower().strip()
    act_str = str(actual).lower().strip()
    if strategy == MatchStrategy.EXACT:
        return exp_str == act_str
    return (
        difflib.SequenceMatcher(None, exp_str, act_str).ratio() >= FUZZY_MATCH_THRESHOLD
    )


def _keys_match(
    expected_elem: dict[str, Any],
    actual_elem: dict[str, Any],
    primary_key_cols: list[str],
    struct_field_by_name: dict[str, ExtractionInferredField],
) -> bool:
    """Returns True if all primary key fields match between two elements.

    Each key field is compared using its own strategy (STRING → fuzzy,
    others → exact), so that e.g. a free-text employer name key tolerates
    minor typos while an ENUM change_type key requires an exact match.
    """
    for col in primary_key_cols:
        sf = struct_field_by_name.get(col)
        strategy = FIELD_TYPE_TO_STRATEGY[sf.field_type] if sf else MatchStrategy.EXACT
        if not score_flat_field(expected_elem.get(col), actual_elem.get(col), strategy):
            return False
    return True


def score_array_of_struct(
    expected_list: list[dict[str, Any]],
    actual_list: list[dict[str, Any]],
    array_config: ArrayOfStructEvalConfig,
    struct_fields: tuple[ExtractionInferredField, ...],
) -> bool:
    """Returns True when expected and actual arrays match exactly.

    Elements are paired by primary_key_cols using per-key-field match strategies
    (STRING keys use fuzzy matching; ENUM/BOOLEAN/numeric keys use exact matching).
    After pairing, all non-key sub-fields are compared using their own strategies.
    Any cardinality difference or unpaired element is an immediate False.
    """
    if len(expected_list) != len(actual_list):
        return False

    struct_field_by_name = {sf.name: sf for sf in struct_fields}
    unmatched_actual = list(actual_list)

    for expected_elem in expected_list:
        matched_idx: int | None = None
        for i, actual_elem in enumerate(unmatched_actual):
            if _keys_match(
                expected_elem,
                actual_elem,
                array_config.primary_key_cols,
                struct_field_by_name,
            ):
                matched_idx = i
                break
        if matched_idx is None:
            return False
        actual_elem = unmatched_actual.pop(matched_idx)
        for sf_name, sf in struct_field_by_name.items():
            if sf_name in array_config.primary_key_cols:
                continue
            if sf_name not in expected_elem:
                # Field absent from expected → annotator didn't specify it; don't check.
                continue
            strategy = FIELD_TYPE_TO_STRATEGY[sf.field_type]
            if not score_flat_field(
                expected_elem.get(sf_name), actual_elem.get(sf_name), strategy
            ):
                return False

    return True


def _parse_expected_array(raw: Any) -> list[dict[str, Any]]:
    """Parses an expected ARRAY_OF_STRUCT value from its JSON string form."""
    if raw is None or raw == "":
        return []
    if isinstance(raw, list):
        return raw
    return json.loads(raw)


def _parse_is_relevant(raw: Any) -> bool:
    if isinstance(raw, bool):
        return raw
    return str(raw).lower().strip() == "true"


def score_document(
    expected_row: dict[str, Any],
    actual_row: dict[str, Any] | None,
    eval_config: GoldenEvalConfig,
) -> DocumentScore:
    """Scores one document by comparing expected values against actual extracted values."""
    document_id = str(expected_row["document_id"])
    is_relevant_expected = _parse_is_relevant(
        expected_row.get(f"{RESERVED_FIELD_NAME_IS_RELEVANT}__expected")
    )

    is_relevant_actual: bool | None = None
    if actual_row is not None:
        is_relevant_actual = _parse_is_relevant(
            actual_row[RESERVED_FIELD_NAME_IS_RELEVANT]
        )

    if (
        not is_relevant_expected
        or actual_row is None
        or eval_config.output_schema is None
    ):
        return DocumentScore(
            document_id=document_id,
            is_relevant_expected=is_relevant_expected,
            is_relevant_actual=is_relevant_actual,
        )

    field_scores: dict[str, FieldScore] = {}
    for f in eval_config.output_schema.inferred_fields:
        if f.name == RESERVED_FIELD_NAME_IS_RELEVANT:
            continue

        expected_val = expected_row.get(f"{f.name}__expected")
        actual_val = actual_row.get(f.name)

        if f.field_type == ExtractionFieldType.ARRAY_OF_STRUCT:
            assert f.struct_fields is not None
            expected_list = _parse_expected_array(expected_val)
            actual_list = actual_val if isinstance(actual_val, list) else []
            if f.name in eval_config.array_of_struct_configs:
                is_match = score_array_of_struct(
                    expected_list,
                    actual_list,
                    eval_config.array_of_struct_configs[f.name],
                    f.struct_fields,
                )
            else:
                is_match = expected_list == actual_list
            field_scores[f.name] = FieldScore(
                field_name=f.name,
                is_match=is_match,
                expected=expected_list,
                actual=actual_list,
            )
        else:
            strategy = FIELD_TYPE_TO_STRATEGY[f.field_type]
            is_match = score_flat_field(expected_val, actual_val, strategy)
            field_scores[f.name] = FieldScore(
                field_name=f.name,
                is_match=is_match,
                expected=expected_val,
                actual=actual_val,
            )

    return DocumentScore(
        document_id=document_id,
        is_relevant_expected=is_relevant_expected,
        is_relevant_actual=is_relevant_actual,
        field_scores=field_scores,
    )
