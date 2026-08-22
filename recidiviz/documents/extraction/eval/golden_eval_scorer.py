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
"""The pure golden eval scorer: compares an extractor's actual output against a
GoldenEvalDocument's expected values, field by field.
"""
from typing import Any, Sequence

from recidiviz.documents.extraction.eval.golden_eval_document import GoldenEvalDocument
from recidiviz.documents.extraction.eval.golden_eval_result import (
    GoldenEvalFieldScore,
    array_sub_field_score_name,
    render_array_element_count,
)
from recidiviz.documents.extraction.exceptions import LLMOutputParsingError
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfStructLLMRequestOutputSchemaField,
    LLMOutputFieldType,
    LLMRequestOutputSchemaField,
    ScalarValuedLLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)

# The comparison key of a value that is not present on one side of a comparison.
# Distinct from any key a present value produces, so a null never matches a
# non-null.
_ABSENT_COMPARISON_KEY = None

# The pairing key of one ARRAY_OF_STRUCT element: its primary-key sub-field
# values, each reduced to its comparison key.
_ElementPairingKey = tuple[Any, ...]


def field_comparison_key(*, field: LLMRequestOutputSchemaField, value: Any) -> Any:
    """Returns the key two values of |field| are compared by. String-valued fields
    (STRING and ENUM) compare fuzzily — case-insensitive and whitespace-normalized;
    every other type compares exactly. Also read by the golden eval sheet parser,
    so it detects expected array elements that this scorer could not tell apart.
    """
    if value is None:
        return _ABSENT_COMPARISON_KEY
    if field.field_type.primitive_scalar_value_type() is LLMOutputFieldType.STRING:
        return " ".join(str(value).split()).casefold()
    return value


class LLMDocumentExtractionGoldenEvalScorer:
    """Scores an extractor's actual output against a GoldenEvalDocument's expected
    output.

    Flat fields score by fuzzy match (string-valued fields: case-insensitive,
    whitespace-normalized) or exact comparison (every other type).
    ARRAY_OF_STRUCT fields pair elements on the field's `primary_keys`, score each
    paired element's sub-fields, score unmatched expected elements as misses and
    unmatched actual elements as false positives, and emit an array-level score
    recording whether the counts matched and every element paired.

    Returns one GoldenEvalFieldScore per (document, field[, element]) comparison.
    """

    def score(
        self,
        *,
        # The output schema every document's fields are scored against: the
        # schema of the extractor this eval run exercised.
        output_schema: LLMRequestOutputSchema,
        documents: Sequence[GoldenEvalDocument],
        # The processed actual output per document, or None for a document that
        # produced nothing usable (a request error or a validation downgrade),
        # which scores as a miss on every expected field.
        actual_output_values_by_document_id: dict[str, LLMRequestOutputValues | None],
    ) -> list[GoldenEvalFieldScore]:
        """Returns one score per (document, field[, element]) comparison, over
        every field |output_schema| declares — `is_relevant` included — for every
        document in |documents|.
        """
        scores = []
        for document in documents:
            if document.golden_document_id not in actual_output_values_by_document_id:
                raise ValueError(
                    f"No actual extraction output was provided for golden eval "
                    f"document [{document.golden_document_id}]."
                )
            actual_output = actual_output_values_by_document_id[
                document.golden_document_id
            ]
            if (
                actual_output is not None
                and actual_output.output_schema != output_schema
            ):
                raise ValueError(
                    f"Actual output for golden eval document "
                    f"[{document.golden_document_id}] uses a different output "
                    f"schema than the one being scored against."
                )
            for field in output_schema.all_fields:
                try:
                    scores.extend(
                        self._score_field(
                            document=document,
                            field=field,
                            actual_output=self._actual_output_for_field(
                                field=field, actual_output=actual_output
                            ),
                        )
                    )
                except LLMOutputParsingError:
                    # A field whose shape contradicts the schema is unreadable,
                    # so it scores as though the extractor produced nothing for
                    # it — the fields that read cleanly keep their own scores.
                    scores.extend(
                        self._score_field(
                            document=document, field=field, actual_output=None
                        )
                    )
        return scores

    @staticmethod
    def _actual_output_for_field(
        *,
        field: LLMRequestOutputSchemaField,
        actual_output: LLMRequestOutputValues | None,
    ) -> LLMRequestOutputValues | None:
        """Returns the output |field| is scored against: |actual_output| itself,
        or None when the extractor called the document irrelevant and |field| is
        not the relevance field.

        An irrelevant result is exactly `{"is_relevant": false}`, so every other
        field scores as though the extractor produced nothing for it. Resolving
        that here lets everything below assume the output carries the fields the
        schema declares.
        """
        if actual_output is None or actual_output.is_relevant:
            return actual_output
        if field.name == IS_RELEVANT_FIELD_NAME:
            return actual_output
        return None

    @classmethod
    def _score_field(
        cls,
        *,
        document: GoldenEvalDocument,
        field: LLMRequestOutputSchemaField,
        actual_output: LLMRequestOutputValues | None,
    ) -> list[GoldenEvalFieldScore]:
        """Returns every score for one output schema field: a single score for a
        scalar-valued field, or the array-level score plus one per sub-field of
        each element for an ARRAY_OF_STRUCT field. An |actual_output| of None —
        nothing usable extracted — scores every expected value as a miss.
        """
        if isinstance(field, ArrayOfStructLLMRequestOutputSchemaField):
            return cls._score_array_field(
                document=document, field=field, actual_output=actual_output
            )
        if isinstance(field, ScalarValuedLLMRequestOutputSchemaField):
            expected_value = document.expected_scalar_value(field.name)
            actual_value = (
                actual_output.value_for_field(field=field)
                if actual_output is not None
                else None
            )
            return [
                cls._build_score(
                    document=document,
                    field_name=field.name,
                    element_index=None,
                    expected_value=expected_value,
                    actual_value=actual_value,
                    is_correct=cls._values_match(
                        field=field,
                        expected_value=expected_value,
                        actual_value=actual_value,
                    ),
                )
            ]
        raise ValueError(
            f"Cannot golden eval score field [{field.name}] of type "
            f"[{field.field_type.value}]."
        )

    @classmethod
    def _score_array_field(
        cls,
        *,
        document: GoldenEvalDocument,
        field: ArrayOfStructLLMRequestOutputSchemaField,
        actual_output: LLMRequestOutputValues | None,
    ) -> list[GoldenEvalFieldScore]:
        """Returns the scores for one ARRAY_OF_STRUCT field: an array-level score
        recording whether the counts matched and every element paired, then one
        score per sub-field of each paired element, each unmatched expected
        element (a miss on every sub-field), and each unmatched actual element (a
        false positive on every sub-field).
        """
        actual_elements = (
            actual_output.array_elements(field=field)
            if actual_output is not None
            else None
        )
        element_pairs = cls._element_pairs(
            field=field, document=document, actual_elements=actual_elements or []
        )

        scores = [
            cls._build_array_level_score(
                document=document,
                field=field,
                actual_elements=actual_elements,
                expected_element_count=len(
                    document.expected_array_elements(field.name)
                ),
                paired_element_count=sum(1 for e in element_pairs if None not in e),
            )
        ]

        for element_index, (expected_element, actual_element) in enumerate(
            element_pairs
        ):
            element_paired = expected_element is not None and actual_element is not None
            for sub_field in field.fields:
                # A sub-field absent from an expected element is not expected to
                # be present — sub-field coverage is not column-enforced in the
                # eval sheet.
                expected_value = (
                    expected_element.get(sub_field.name)
                    if expected_element is not None
                    else None
                )
                actual_value = (
                    actual_element[sub_field.name]
                    if actual_element is not None
                    else None
                )
                scores.append(
                    cls._build_score(
                        document=document,
                        field_name=array_sub_field_score_name(
                            array_field_name=field.name, sub_field_name=sub_field.name
                        ),
                        element_index=element_index,
                        expected_value=expected_value,
                        actual_value=actual_value,
                        # An element that paired with nothing is wrong on every
                        # sub-field — even a sub-field null on both sides. Only a
                        # paired element's sub-fields are worth comparing: a null
                        # expected sub-field agrees with a null actual one purely
                        # because the expected element itself is missing.
                        is_correct=(
                            element_paired
                            and cls._values_match(
                                field=sub_field,
                                expected_value=expected_value,
                                actual_value=actual_value,
                            )
                        ),
                    )
                )

        return scores

    @classmethod
    def _build_array_level_score(
        cls,
        *,
        document: GoldenEvalDocument,
        field: ArrayOfStructLLMRequestOutputSchemaField,
        actual_elements: list[dict[str, Any]] | None,
        expected_element_count: int,
        paired_element_count: int,
    ) -> GoldenEvalFieldScore:
        """Returns the array-level score for one ARRAY_OF_STRUCT field: how many
        elements each side held, and whether every one of them paired. Only a
        field that neither side carried at all renders no count — an expected
        blank cell and an omitted output field alike mean no elements, which is
        equivalent to an empty array.
        """
        return GoldenEvalFieldScore(
            golden_document_id=document.golden_document_id,
            test_type=document.test_type,
            test_case=document.test_case,
            field_name=field.name,
            element_index=None,
            expected_value=(
                render_array_element_count(expected_element_count)
                if document.expects_field(field.name)
                else None
            ),
            actual_value=(
                render_array_element_count(len(actual_elements))
                if actual_elements is not None
                else None
            ),
            # Both counts matching is implied by every element on both sides
            # pairing, so this single check covers the count comparison too.
            is_correct=(
                paired_element_count
                == expected_element_count
                == len(actual_elements or [])
            ),
        )

    @classmethod
    def _build_score(
        cls,
        *,
        document: GoldenEvalDocument,
        field_name: str,
        element_index: int | None,
        expected_value: Any,
        actual_value: Any,
        is_correct: bool,
    ) -> GoldenEvalFieldScore:
        """Returns the score row for one scalar expected value and its actual
        counterpart, rendering both for the results row.
        """
        return GoldenEvalFieldScore(
            golden_document_id=document.golden_document_id,
            test_type=document.test_type,
            test_case=document.test_case,
            field_name=field_name,
            element_index=element_index,
            # Preserves the original casing/whitespace of a string for the
            # results row — only the correctness comparison is normalized.
            expected_value=str(expected_value) if expected_value is not None else None,
            actual_value=str(actual_value) if actual_value is not None else None,
            is_correct=is_correct,
        )

    @classmethod
    def _values_match(
        cls,
        *,
        field: LLMRequestOutputSchemaField,
        expected_value: Any,
        actual_value: Any,
    ) -> bool:
        """Returns whether |expected_value| and |actual_value| compare equal
        under |field|'s comparison key.
        """
        return field_comparison_key(
            field=field, value=expected_value
        ) == field_comparison_key(field=field, value=actual_value)

    @classmethod
    def _element_pairs(
        cls,
        *,
        field: ArrayOfStructLLMRequestOutputSchemaField,
        document: GoldenEvalDocument,
        actual_elements: list[dict[str, Any]],
    ) -> list[tuple[dict[str, Any] | None, dict[str, Any] | None]]:
        """Returns every array element of |field| to score as an (expected,
        actual) pair, in score order. An (<element>, <element>) tuple indicates an
        actual value that matched an expected value via their pairing keys.
        (<element>, None) indicates a missing expected value. (None, <element>)
        indicates an extra actual value.
        """
        unpaired_actuals = [
            (cls._element_pairing_key(field=field, element=element), element)
            for element in actual_elements
        ]
        pairs: list[tuple[dict[str, Any] | None, dict[str, Any] | None]] = []
        for expected_element in document.expected_array_elements(field.name):
            expected_key = cls._element_pairing_key(
                field=field, element=expected_element
            )
            paired_actual = None  # No key match: an (expected, None) miss.
            for index, (actual_key, _) in enumerate(unpaired_actuals):
                if actual_key == expected_key:
                    paired_actual = unpaired_actuals.pop(index)[1]
                    break
            pairs.append((expected_element, paired_actual))
        # Add unmatched actual elements in order as false positives
        pairs.extend((None, element) for _, element in unpaired_actuals)
        return pairs

    @classmethod
    def _element_pairing_key(
        cls,
        *,
        field: ArrayOfStructLLMRequestOutputSchemaField,
        element: dict[str, Any],
    ) -> _ElementPairingKey:
        """Returns the pairing key of one array element — expected or actual,
        both holding one plain scalar value per present sub-field name.
        """
        return tuple(
            field_comparison_key(
                field=field.get_field(primary_key), value=element.get(primary_key)
            )
            for primary_key in field.primary_keys
        )
