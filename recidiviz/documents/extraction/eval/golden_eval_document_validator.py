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
"""Flags invalid expected values of a golden eval document.

Most of these checks — declared types, enum membership, required fields,
`min_items`, unknown keys — are already done by LLMExtractionResultValidator on
the JSON Schema generated from the same output schema, and should eventually be
delegated to it — TODO(OBT-45424). Its checks read an LLMRequestOutputValues
built from a real result, which carries the confidence and citation metadata a
golden eval sheet does not, so each one becomes reusable only once it can read
bare values. Every check here is therefore written to be retired on its own.

One check will outlive the rest: the scorer pairs expected and actual array
elements on a field's `primary_keys`, which is a golden eval concern the pipeline
has no equivalent of. Field coverage — one `__expected` column per field the
schema declares — is enforced up front by GoldenEvalDocumentParser.column_issues,
so every document read here carries a value (possibly None) for every field.
"""
import json
import math
from typing import Any

import attr

from recidiviz.common import attr_validators
from recidiviz.documents.extraction.eval.golden_eval_document import GoldenEvalDocument
from recidiviz.documents.extraction.eval.golden_eval_scorer import field_comparison_key
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfStructLLMRequestOutputSchemaField,
    EnumLLMRequestOutputSchemaField,
    LLMOutputFieldType,
    LLMRequestOutputSchemaField,
    ScalarValuedLLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
)
from recidiviz.utils.types import assert_type


@attr.define(frozen=True, kw_only=True)
class GoldenEvalDocumentIssue:
    """One expected value of a golden eval document that no extractor output could
    legally hold.
    """

    field_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Name of the output schema field the issue is about."""

    element_index: int | None = attr.ib(validator=attr_validators.is_opt_int)
    """Position within an ARRAY_OF_STRUCT field's expected elements, or None when
    the issue is about the field as a whole.
    """

    sub_field_name: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """Name of the sub-field within that element, or None when the issue is about
    the element as a whole.
    """

    detail: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """What is wrong with the value, phrased to follow the position naming it."""

    @property
    def field_path(self) -> str:
        """Returns the dotted path of the value this issue is about — e.g.
        `assignments[0].rate_amount` — matching how a ValidationIssue names the
        field it was raised on.
        """
        path = self.field_name
        if self.element_index is not None:
            path = f"{path}[{self.element_index}]"
        if self.sub_field_name is not None:
            path = f"{path}.{self.sub_field_name}"
        return path


@attr.define(frozen=True, kw_only=True)
class GoldenEvalDocumentValidator:
    """Flags the expected values of a golden eval document that no extractor output
    could legally hold, reading values already coerced to their declared Python
    types by GoldenEvalDocumentParser.
    """

    output_schema: LLMRequestOutputSchema = attr.ib(
        validator=attr.validators.instance_of(LLMRequestOutputSchema)
    )
    """The output schema of the extractor being evaluated, which fixes what its
    output may legally hold.
    """

    def issues(self, *, document: GoldenEvalDocument) -> list[GoldenEvalDocumentIssue]:
        """Returns at most one issue per output schema field whose expected value in
        |document| is invalid. An empty list means the document is valid. The
        document must carry an expected value for every declared field, which
        GoldenEvalDocumentParser guarantees.
        """
        expected_values = document.expected_values
        if (relevance_issue := self._relevance_issue(expected_values)) is not None:
            return [relevance_issue]

        expects_relevant = self._expects_relevant_document(expected_values)
        return [
            issue
            for field in self.output_schema.all_fields
            if (
                issue := self._field_issue(
                    field=field,
                    value=expected_values[field.name],
                    expects_relevant=expects_relevant,
                )
            )
            is not None
        ]

    def _relevance_issue(
        self, expected_values: dict[str, Any]
    ) -> GoldenEvalDocumentIssue | None:
        """Returns the issue for a document that does not say whether the extractor
        should find it relevant. Every other check reads that answer, so it is the
        only one worth reporting while it is missing.
        """
        if self.output_schema.is_relevant_field is None:
            return None
        if expected_values[IS_RELEVANT_FIELD_NAME] is not None:
            return None
        return GoldenEvalDocumentIssue(
            field_name=IS_RELEVANT_FIELD_NAME,
            element_index=None,
            sub_field_name=None,
            detail="is a required boolean field for golden evals.",
        )

    def _expects_relevant_document(self, expected_values: dict[str, Any]) -> bool:
        """Returns whether |expected_values| describes a document the extractor
        should find relevant. A schema declaring no relevance criteria has no
        irrelevant branch, so every one of its documents is relevant.
        """
        if self.output_schema.is_relevant_field is None:
            return True
        return expected_values[IS_RELEVANT_FIELD_NAME] is True

    @classmethod
    def _field_issue(
        cls,
        *,
        field: LLMRequestOutputSchemaField,
        value: Any,
        expects_relevant: bool,
    ) -> GoldenEvalDocumentIssue | None:
        """Returns the first issue with the expected |value| of |field|, or None
        when it is a legal output value.
        """
        if value is None:
            if field.required and expects_relevant:
                return GoldenEvalDocumentIssue(
                    field_name=field.name,
                    element_index=None,
                    sub_field_name=None,
                    detail=(
                        f"is blank, but field [{field.name}] is required of every "
                        f"relevant document"
                    ),
                )
            return None
        if isinstance(field, ArrayOfStructLLMRequestOutputSchemaField):
            return cls._array_issue(
                field=field, value=value, expects_relevant=expects_relevant
            )
        if isinstance(field, ScalarValuedLLMRequestOutputSchemaField):
            return cls._scalar_issue(field=field, value=value)
        return None

    @classmethod
    def _scalar_issue(
        cls, *, field: ScalarValuedLLMRequestOutputSchemaField, value: Any
    ) -> GoldenEvalDocumentIssue | None:
        """Returns the issue with the expected |value| of scalar-valued |field|, or
        None when the field's type allows it.
        """
        field_type = field.field_type
        if field_type is LLMOutputFieldType.ENUM:
            enum_field = assert_type(field, EnumLLMRequestOutputSchemaField)
            if value in enum_field.value_names:
                return None
            return GoldenEvalDocumentIssue(
                field_name=field.name,
                element_index=None,
                sub_field_name=None,
                detail=(
                    f"is not one of the values field [{field.name}] declares: "
                    f"{enum_field.value_names}"
                ),
            )
        if field_type is LLMOutputFieldType.FLOAT and not math.isfinite(value):
            return GoldenEvalDocumentIssue(
                field_name=field.name,
                element_index=None,
                sub_field_name=None,
                detail="must be a finite number.",
            )
        return None

    @classmethod
    def _array_issue(
        cls,
        *,
        field: ArrayOfStructLLMRequestOutputSchemaField,
        value: Any,
        expects_relevant: bool,
    ) -> GoldenEvalDocumentIssue | None:
        """Returns the first issue with the expected elements of ARRAY_OF_STRUCT
        |field|, or None when every element is a legal output element.
        """
        if not isinstance(value, list):
            return GoldenEvalDocumentIssue(
                field_name=field.name,
                element_index=None,
                sub_field_name=None,
                detail=(
                    f"is a JSON [{type(value).__name__}], but ARRAY_OF_STRUCT field "
                    f"[{field.name}] expects a JSON array of objects"
                ),
            )

        for element_index, element in enumerate(value):
            if (
                issue := cls._element_issue(
                    field=field, element_index=element_index, element=element
                )
            ) is not None:
                return issue

        if (
            pairing_key_issue := cls._pairing_key_issue(field=field, elements=value)
        ) is not None:
            return pairing_key_issue

        if field.min_items is not None and len(value) < field.min_items:
            return GoldenEvalDocumentIssue(
                field_name=field.name,
                element_index=None,
                sub_field_name=None,
                detail=(
                    f"holds [{len(value)}] element(s), but ARRAY_OF_STRUCT field "
                    f"[{field.name}] requires at least [{field.min_items}]"
                ),
            )

        if not expects_relevant:
            return None
        return cls._required_sub_field_issue(field=field, elements=value)

    @classmethod
    def _element_issue(
        cls,
        *,
        field: ArrayOfStructLLMRequestOutputSchemaField,
        element_index: int,
        element: Any,
    ) -> GoldenEvalDocumentIssue | None:
        """Returns the first issue with one expected element of ARRAY_OF_STRUCT
        |field|, or None when it is a legal output element.
        """
        if not isinstance(element, dict):
            return GoldenEvalDocumentIssue(
                field_name=field.name,
                element_index=element_index,
                sub_field_name=None,
                detail=(
                    f"is a JSON [{type(element).__name__}], but every element of "
                    f"ARRAY_OF_STRUCT field [{field.name}] must be a JSON object "
                    f"keyed by sub-field name"
                ),
            )
        # Every key must name a sub-field the field declares, so a typo does not
        # silently drop an expectation.
        if unknown_keys := set(element) - set(field.fields_by_name):
            return GoldenEvalDocumentIssue(
                field_name=field.name,
                element_index=element_index,
                sub_field_name=None,
                detail=(
                    f"holds key(s) {sorted(unknown_keys)} that are not sub-fields "
                    f"of field [{field.name}]: {sorted(field.fields_by_name)}"
                ),
            )

        for sub_field_name, value in element.items():
            if (
                detail := cls._sub_field_detail(
                    field=field.get_field(sub_field_name), value=value
                )
            ) is not None:
                return GoldenEvalDocumentIssue(
                    field_name=field.name,
                    element_index=element_index,
                    sub_field_name=sub_field_name,
                    detail=detail,
                )
        return None

    @classmethod
    def _required_sub_field_issue(
        cls,
        *,
        field: ArrayOfStructLLMRequestOutputSchemaField,
        elements: list[Any],
    ) -> GoldenEvalDocumentIssue | None:
        """Returns the issue for the first element of ARRAY_OF_STRUCT |field| that
        leaves a required sub-field null, or None when every element fills them all.
        """
        for element_index, element in enumerate(elements):
            if null_sub_field_names := sorted(
                sub_field.name
                for sub_field in field.fields
                if sub_field.required and element.get(sub_field.name) is None
            ):
                return GoldenEvalDocumentIssue(
                    field_name=field.name,
                    element_index=element_index,
                    sub_field_name=None,
                    detail=(
                        f"has no value for required sub-field(s) "
                        f"{null_sub_field_names} of field [{field.name}]"
                    ),
                )
        return None

    @classmethod
    def _sub_field_detail(
        cls, *, field: LLMRequestOutputSchemaField, value: Any
    ) -> str | None:
        """Returns what is wrong with array sub-field |field|'s expected |value|, or
        None when the sub-field's type allows it.
        """
        if value is None:
            return None
        if not isinstance(field, ScalarValuedLLMRequestOutputSchemaField):
            raise ValueError(
                f"Cannot golden eval score array sub-field [{field.name}] of type "
                f"[{field.field_type.value}]."
            )

        field_type = field.field_type
        if field_type is LLMOutputFieldType.STRING:
            if not isinstance(value, str):
                return cls._wrong_json_type_detail(
                    field=field, value=value, expected_json_type="a string"
                )
            return None
        if field_type is LLMOutputFieldType.ENUM:
            if not isinstance(value, str):
                return cls._wrong_json_type_detail(
                    field=field, value=value, expected_json_type="a string"
                )
            enum_field = assert_type(field, EnumLLMRequestOutputSchemaField)
            if value in enum_field.value_names:
                return None
            return (
                f"is not one of the values field [{field.name}] declares: "
                f"{enum_field.value_names}"
            )
        if field_type is LLMOutputFieldType.BOOLEAN:
            if not isinstance(value, bool):
                return cls._wrong_json_type_detail(
                    field=field,
                    value=value,
                    expected_json_type="an unquoted true or false",
                )
            return None
        if field_type is LLMOutputFieldType.INTEGER:
            # Mirrors JSON Schema `integer`, which the pipeline validates actual
            # output against: any number with a zero fractional part, so 12.0 is
            # as legal an expectation as 12. `isinstance(True, int)` is True, so
            # booleans must be excluded explicitly or `true` would read as 1.
            if isinstance(value, bool) or not (
                isinstance(value, int)
                or (isinstance(value, float) and value.is_integer())
            ):
                return cls._wrong_json_type_detail(
                    field=field, value=value, expected_json_type="an unquoted integer"
                )
            return None
        if field_type is LLMOutputFieldType.FLOAT:
            # Mirrors JSON Schema `number`, which the pipeline validates actual
            # output against: an integer is as legal an expectation as a float.
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                return cls._wrong_json_type_detail(
                    field=field, value=value, expected_json_type="an unquoted number"
                )
            if not math.isfinite(value):
                return (
                    f"sub-field [{field.name}] holds a non-finite number, which "
                    f"could never compare equal to an extracted value"
                )
            return None
        raise ValueError(
            f"Cannot golden eval score array sub-field [{field.name}] of type "
            f"[{field_type.value}]."
        )

    @classmethod
    def _pairing_key_issue(
        cls,
        *,
        field: ArrayOfStructLLMRequestOutputSchemaField,
        elements: list[Any],
    ) -> GoldenEvalDocumentIssue | None:
        """Returns the first issue that would stop the scorer pairing the expected
        elements of ARRAY_OF_STRUCT |field| with the extractor's actual ones, or
        None when every element pairs cleanly. The scorer pairs on the field's
        `primary_keys`, so a null key silently mispairs, and duplicate keys make
        its greedy pairing — and therefore the score — depend on element order.
        """
        element_index_by_pairing_key: dict[tuple[Any, ...], int] = {}
        for element_index, element in enumerate(elements):
            if null_primary_keys := [
                primary_key
                for primary_key in field.primary_keys
                if element.get(primary_key) is None
            ]:
                return GoldenEvalDocumentIssue(
                    field_name=field.name,
                    element_index=element_index,
                    sub_field_name=None,
                    detail=(
                        f"has no value for primary key sub-field(s) "
                        f"{sorted(null_primary_keys)} of field [{field.name}], which "
                        f"expected and actual elements pair on"
                    ),
                )
            pairing_key = tuple(
                field_comparison_key(
                    field=field.get_field(primary_key), value=element[primary_key]
                )
                for primary_key in field.primary_keys
            )
            if pairing_key in element_index_by_pairing_key:
                return GoldenEvalDocumentIssue(
                    field_name=field.name,
                    element_index=element_index,
                    sub_field_name=None,
                    detail=(
                        f"has the same primary key values as element "
                        f"[{element_index_by_pairing_key[pairing_key]}], so the two "
                        f"cannot be told apart when pairing on "
                        f"{field.primary_keys} of field [{field.name}]"
                    ),
                )
            element_index_by_pairing_key[pairing_key] = element_index
        return None

    @staticmethod
    def _wrong_json_type_detail(
        *,
        field: LLMRequestOutputSchemaField,
        value: Any,
        expected_json_type: str,
    ) -> str:
        """Returns what is wrong with an array element sub-field value of the wrong
        JSON type, naming what the sub-field expects and what the element holds.
        """
        return (
            f"sub-field [{field.name}] of type [{field.field_type.value}] expects "
            f"{expected_json_type}, but the element holds [{json.dumps(value)}]"
        )
