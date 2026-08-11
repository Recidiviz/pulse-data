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
from typing import Any

import attr

from recidiviz.common import attr_validators
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfStructLLMRequestOutputSchemaField,
    ScalarValuedLLMRequestOutputSchemaField,
    assert_scalar_valued_field,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    NULL_REASON_FIELD_NAME,
    RESULT_KEY,
    VALUE_FIELD_NAME,
)


class LLMOutputParsingError(ValueError):
    """Raised when an output JSON's shape contradicts the output schema it is read
    against."""


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
    if VALUE_FIELD_NAME in field_json:
        return field_json[VALUE_FIELD_NAME]
    # An INFERRED field's null branch carries a null_reason in place of the
    # value key.
    if NULL_REASON_FIELD_NAME in field_json:
        return None
    raise LLMOutputParsingError(
        f"INFERRED field [{field.name}] holds neither a [{VALUE_FIELD_NAME}] nor a "
        f"[{NULL_REASON_FIELD_NAME}]. Found keys: {sorted(field_json)}."
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

    def value_for_field(self, *, field: ScalarValuedLLMRequestOutputSchemaField) -> Any:
        """Returns the scalar value the extractor produced for top-level
        |field|, unwrapping INFERRED fields.
        """
        return _scalar_field_value(field=field, container=self._extracted_fields_json)

    def has_field(self, field_name: str) -> bool:
        """Returns whether the output carries top-level |field_name| at all."""
        return field_name in self._extracted_fields_json

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

    def array_elements(
        self, *, field: ArrayOfStructLLMRequestOutputSchemaField
    ) -> list[dict[str, Any]] | None:
        """Returns the elements the extractor produced for ARRAY_OF_STRUCT
        |field|, each mapping every sub-field |field| declares to its unwrapped
        scalar value (`None` for an omitted sub-field or an INFERRED null
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
            sub_field.name: _scalar_field_value(
                field=assert_scalar_valued_field(sub_field),
                container=element_json,
            )
            for sub_field in field.fields
        }
