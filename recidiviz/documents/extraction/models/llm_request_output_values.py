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
"""Wraps the raw JSON output returned by an extractor, paired with the output schema
needed to interpret it.
"""
import copy
from typing import Any

import attr

from recidiviz.common import attr_validators
from recidiviz.documents.extraction.exceptions import LLMOutputParsingError
from recidiviz.documents.extraction.models.llm_inferred_field_output import (
    InferredFieldOutput,
)
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfIntegerLLMRequestOutputSchemaField,
    ArrayOfStructLLMRequestOutputSchemaField,
    LLMRequestOutputSchemaField,
    ScalarValuedLLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    RESULT_KEY,
)


def _scalar_field_value(
    *,
    field: ScalarValuedLLMRequestOutputSchemaField,
    container: dict[str, Any] | None,
) -> Any:
    """Returns the scalar value |container| holds for |field|, unwrapping
    INFERRED fields. Returns `None` when the container is absent, when it
    omits the field, or when an INFERRED field took its null branch.
    """
    if container is None or field.name not in container:
        return None
    field_json = container[field.name]
    if not field.is_inferred_field:
        return field_json
    if not isinstance(field_json, dict):
        raise LLMOutputParsingError(
            f"Expected INFERRED field [{field.name}] to hold a dict, found "
            f"[{type(field_json)}]."
        )
    return InferredFieldOutput(
        field=field, display_name=field.name, field_json=field_json
    ).value


def _integer_array_field_value(
    *,
    field: ArrayOfIntegerLLMRequestOutputSchemaField,
    container: dict[str, Any] | None,
) -> list[int] | None:
    """Returns the integer array |container| holds for |field| — carried bare in
    the JSON, with no INFERRED wrapper to unwrap. Returns `None` when the
    container is absent or omits the field.
    """
    if container is None or field.name not in container:
        return None
    field_json = container[field.name]
    if not isinstance(field_json, list):
        raise LLMOutputParsingError(
            f"Expected ARRAY_OF_INTEGER field [{field.name}] to hold a list, "
            f"found [{type(field_json)}]."
        )
    return field_json


def _sub_field_value(
    *,
    field: LLMRequestOutputSchemaField,
    container: dict[str, Any] | None,
) -> Any:
    """Returns the value |container| holds for non-ARRAY_OF_STRUCT |field| — an
    ARRAY_OF_STRUCT element's sub-field, or a top-level field: the unwrapped
    scalar for a scalar-valued field, or the bare integer list for an
    ARRAY_OF_INTEGER field (an ER entity's `entry_nums`).
    """
    if isinstance(field, ScalarValuedLLMRequestOutputSchemaField):
        return _scalar_field_value(field=field, container=container)
    if isinstance(field, ArrayOfIntegerLLMRequestOutputSchemaField):
        return _integer_array_field_value(field=field, container=container)
    raise ValueError(
        f"Cannot read a value for field [{field.name}] of type "
        f"[{field.field_type.value}]."
    )


@attr.define(frozen=True, kw_only=True)
class LLMRequestOutputValues:
    """Wraps the raw JSON output returned by an extractor, paired with the output
    schema needed to interpret it.
    """

    output_schema: LLMRequestOutputSchema = attr.ib(
        validator=attr.validators.instance_of(LLMRequestOutputSchema)
    )
    """The output schema the output JSON conforms to."""

    output_json: dict[str, Any] = attr.ib(validator=attr_validators.is_dict)
    """The raw output JSON as the extractor returned it, which may or may not
    have been through validation. An extractor that produced no usable output at
    all (a request error, or a validation downgrade) has no
    `LLMRequestOutputValues` to wrap it, rather than one holding nothing.
    """

    @property
    def _extracted_fields_json(self) -> dict[str, Any]:
        """Returns the object holding the extracted fields within the output
        JSON — the content of the `result` envelope for a schema that has one,
        the JSON itself otherwise.
        """
        if not self.output_schema.has_result_envelope:
            return self.output_json
        if RESULT_KEY not in self.output_json:
            raise LLMOutputParsingError(
                f"Output JSON for a schema with a result envelope has no "
                f"[{RESULT_KEY}] key. Found keys: {sorted(self.output_json)}."
            )
        envelope_json = self.output_json[RESULT_KEY]
        if not isinstance(envelope_json, dict):
            raise LLMOutputParsingError(
                f"Expected the [{RESULT_KEY}] envelope to hold a dict, found "
                f"[{type(envelope_json)}]."
            )
        return envelope_json

    def deep_copy(self) -> "LLMRequestOutputValues":
        """Returns a copy of these values over a deep copy of the output JSON, so
        the copy can be edited without touching the output these values wrap.
        """
        return LLMRequestOutputValues(
            output_schema=self.output_schema,
            output_json=copy.deepcopy(self.output_json),
        )

    def value_for_field(self, *, field: ScalarValuedLLMRequestOutputSchemaField) -> Any:
        """Returns the scalar value the extractor produced for top-level
        |field|, unwrapping INFERRED fields.
        """
        return _scalar_field_value(field=field, container=self._extracted_fields_json)

    def has_field(self, field_name: str) -> bool:
        """Returns whether the output carries top-level |field_name| at all."""
        return field_name in self._extracted_fields_json

    def is_value_present(self, *, field: LLMRequestOutputSchemaField) -> bool:
        """Returns whether the output carries a value for top-level |field|: any
        non-null value. Emptiness is not nullness — an empty string or an empty
        array is a value the model returned; an omitted field or an INFERRED
        null branch is not.
        """
        if isinstance(field, ScalarValuedLLMRequestOutputSchemaField):
            return self.value_for_field(field=field) is not None

        if isinstance(
            field,
            (
                ArrayOfStructLLMRequestOutputSchemaField,
                ArrayOfIntegerLLMRequestOutputSchemaField,
            ),
        ):
            extracted_fields_json = self._extracted_fields_json
            if field.name not in extracted_fields_json:
                return False
            field_json = extracted_fields_json[field.name]
            if not isinstance(field_json, list):
                raise LLMOutputParsingError(
                    f"Expected array field [{field.name}] to hold a list, found "
                    f"[{type(field_json)}]."
                )
            # Any list — even an empty one — is a value: the extractor's affirmative
            # statement about the field, unlike an omitted key, which says nothing.
            return True

        raise ValueError(f"Unexpected type for field [{field.name}]: {type(field)}")

    def inferred_field_outputs(self) -> list[InferredFieldOutput]:
        """Returns one `InferredFieldOutput` per INFERRED field the output
        actually carries a companion-metadata wrapper for — the top-level
        INFERRED fields, plus the INFERRED sub-fields of every element of every
        ARRAY_OF_STRUCT field.

        An ARRAY_OF_STRUCT field is skipped in its own right even when it is
        INFERRED, because the generated JSON Schema emits it as a bare array with
        no wrapper of its own; only its sub-fields carry companion metadata. A
        field the output omits entirely (as an older extractor version's output
        would) has no metadata to read and is skipped.

        Only safe to call on a structurally conformant result: the accessors on
        the returned views read the companion keys the generated schema requires,
        and the confidence level is parsed straight into `ConfidenceLevel`.
        """
        extracted_fields_json = self._extracted_fields_json
        outputs: list[InferredFieldOutput] = []
        for field in self.output_schema.all_fields:
            if isinstance(field, ArrayOfStructLLMRequestOutputSchemaField):
                outputs.extend(self._array_element_inferred_field_outputs(field=field))
                continue
            if not field.is_inferred_field or field.name not in extracted_fields_json:
                continue
            outputs.append(
                self._inferred_field_output(
                    field=field,
                    display_name=field.name,
                    container=extracted_fields_json,
                )
            )
        return outputs

    def _array_element_inferred_field_outputs(
        self, *, field: ArrayOfStructLLMRequestOutputSchemaField
    ) -> list[InferredFieldOutput]:
        """Returns one `InferredFieldOutput` per INFERRED sub-field of each
        element of ARRAY_OF_STRUCT |field|, named `<field>[<index>].<sub_field>`.
        """
        extracted_fields_json = self._extracted_fields_json
        if field.name not in extracted_fields_json:
            return []
        elements_json = extracted_fields_json[field.name]
        if not isinstance(elements_json, list):
            raise LLMOutputParsingError(
                f"Expected ARRAY_OF_STRUCT field [{field.name}] to hold a list, "
                f"found [{type(elements_json)}]."
            )
        outputs: list[InferredFieldOutput] = []
        for index, element_json in enumerate(elements_json):
            if not isinstance(element_json, dict):
                raise LLMOutputParsingError(
                    f"Expected element [{index}] of ARRAY_OF_STRUCT field "
                    f"[{field.name}] to hold a dict, found [{type(element_json)}]."
                )
            for sub_field in field.fields:
                if (
                    not sub_field.is_inferred_field
                    or sub_field.name not in element_json
                ):
                    continue
                outputs.append(
                    self._inferred_field_output(
                        field=sub_field,
                        display_name=f"{field.name}[{index}].{sub_field.name}",
                        container=element_json,
                    )
                )
        return outputs

    @staticmethod
    def _inferred_field_output(
        *,
        field: LLMRequestOutputSchemaField,
        display_name: str,
        container: dict[str, Any],
    ) -> InferredFieldOutput:
        """Returns the view over the companion-metadata wrapper |container| holds
        for INFERRED |field|.
        """
        field_json = container[field.name]
        if not isinstance(field_json, dict):
            raise LLMOutputParsingError(
                f"Expected INFERRED field [{display_name}] to hold a dict, found "
                f"[{type(field_json)}]."
            )
        return InferredFieldOutput(
            field=field, display_name=display_name, field_json=field_json
        )

    @property
    def is_relevant(self) -> bool:
        """Returns the model's relevance determination: the schema's is_relevant
        field's value when the schema declares one, or True unconditionally when
        it doesn't (every document is relevant by construction — e.g. entity
        resolution schemas).

        Raises an LLMOutputParsingError when the schema declares an is_relevant
        field but the output omits it or holds a non-bool for it — a schema that
        declares the field always requires it.
        """
        if (is_relevant_field := self.output_schema.is_relevant_field) is None:
            return True
        extracted_fields_json = self._extracted_fields_json
        if is_relevant_field.name not in extracted_fields_json:
            raise LLMOutputParsingError(
                f"Output JSON for a schema that declares "
                f"[{is_relevant_field.name}] does not carry it. Found keys: "
                f"{sorted(extracted_fields_json)}."
            )
        is_relevant = self.value_for_field(field=is_relevant_field)
        if not isinstance(is_relevant, bool):
            raise LLMOutputParsingError(
                f"Expected [{is_relevant_field.name}] to hold a bool, found "
                f"[{type(is_relevant)}]."
            )
        return is_relevant

    @property
    def values_dict(self) -> dict[str, Any]:
        """Returns the value the extractor produced for every field the schema
        declares — the framework-injected `is_relevant` included — keyed by
        field name. A scalar-valued field maps to its unwrapped scalar value, an
        ARRAY_OF_INTEGER field to its bare integer list, and an ARRAY_OF_STRUCT
        field to its list of per-element dicts (see `array_elements`). A `None`
        value means the output omits the field (or, for a scalar, that an
        INFERRED field took its null branch).

        This is the actual-output counterpart to a golden eval document's
        `expected_values`, and has the same shape.
        """
        return {
            field.name: self._field_value(field=field)
            for field in self.output_schema.all_fields
        }

    def _field_value(self, *, field: LLMRequestOutputSchemaField) -> Any:
        """Returns the value the extractor produced for top-level |field|,
        whatever its type.
        """
        if isinstance(field, ArrayOfStructLLMRequestOutputSchemaField):
            return self.array_elements(field=field)
        return _sub_field_value(field=field, container=self._extracted_fields_json)

    def array_elements(
        self, *, field: ArrayOfStructLLMRequestOutputSchemaField
    ) -> list[dict[str, Any]] | None:
        """Returns the elements the extractor produced for ARRAY_OF_STRUCT
        |field|, each mapping every sub-field |field| declares to its unwrapped
        scalar value — or its bare integer list, for an ARRAY_OF_INTEGER
        sub-field (`None` for an omitted sub-field or an INFERRED null
        branch, and for every sub-field of a literal-null element). Returns
        `None` when the output omits the field entirely.
        """
        extracted_fields_json = self._extracted_fields_json
        if field.name not in extracted_fields_json:
            return None
        elements_json = extracted_fields_json[field.name]
        if not isinstance(elements_json, list):
            raise LLMOutputParsingError(
                f"Expected ARRAY_OF_STRUCT field [{field.name}] to hold a list, "
                f"found [{type(elements_json)}]."
            )
        return [
            self._array_element(field=field, element_json=element_json, index=index)
            for index, element_json in enumerate(elements_json)
        ]

    @staticmethod
    def _array_element(
        *,
        field: ArrayOfStructLLMRequestOutputSchemaField,
        element_json: Any,
        index: int,
    ) -> dict[str, Any]:
        """Returns unwrapped element |index| of ARRAY_OF_STRUCT |field|"""
        if element_json is not None and not isinstance(element_json, dict):
            raise LLMOutputParsingError(
                f"Expected element [{index}] of ARRAY_OF_STRUCT field "
                f"[{field.name}] to hold a dict, found [{type(element_json)}]."
            )
        return {
            sub_field.name: _sub_field_value(field=sub_field, container=element_json)
            for sub_field in field.fields
        }
