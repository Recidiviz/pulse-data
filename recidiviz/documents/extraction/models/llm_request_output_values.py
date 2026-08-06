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


class LLMResultParsingError(ValueError):
    """Raised when a result JSON's shape contradicts the output schema it is read
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
        raise LLMResultParsingError(
            f"Expected INFERRED field [{field.name}] to hold a dict, found "
            f"[{type(field_json)}]."
        )
    if VALUE_FIELD_NAME in field_json:
        return field_json[VALUE_FIELD_NAME]
    # An INFERRED field's null branch carries a null_reason in place of the
    # value key.
    if NULL_REASON_FIELD_NAME in field_json:
        return None
    raise LLMResultParsingError(
        f"INFERRED field [{field.name}] holds neither a [{VALUE_FIELD_NAME}] nor a "
        f"[{NULL_REASON_FIELD_NAME}]. Found keys: {sorted(field_json)}."
    )


@attr.define(frozen=True, kw_only=True)
class LLMRequestOutputValues:
    """Wraps the raw JSON result returned by an extractor, paired with the output
    schema needed to interpret it.
    """

    output_schema: LLMRequestOutputSchema = attr.ib(
        validator=attr.validators.instance_of(LLMRequestOutputSchema)
    )
    """The output schema the result JSON conforms to."""

    result_json: dict[str, Any] | None = attr.ib(validator=attr_validators.is_opt_dict)
    """The raw output JSON — the validated content of a SUCCESS. `None`
    means there is no usable output (a request error or a validation downgrade),
    which every accessor reads as "nothing produced".
    """

    @property
    def _extracted_fields_json(self) -> dict[str, Any] | None:
        """Returns the object holding the extracted fields within the result
        JSON — the content of the `result` envelope for a schema that has one,
        the JSON itself otherwise — or `None` when there is no usable output.
        """
        if self.result_json is None:
            return None
        if not self.output_schema.has_result_envelope:
            return self.result_json
        if RESULT_KEY not in self.result_json:
            raise LLMResultParsingError(
                f"Result JSON for a schema with a result envelope has no "
                f"[{RESULT_KEY}] key. Found keys: {sorted(self.result_json)}."
            )
        envelope_json = self.result_json[RESULT_KEY]
        if not isinstance(envelope_json, dict):
            raise LLMResultParsingError(
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
        extracted_fields_json = self._extracted_fields_json
        return extracted_fields_json is not None and field_name in extracted_fields_json

    def array_elements(
        self, *, field: ArrayOfStructLLMRequestOutputSchemaField
    ) -> list[dict[str, Any]] | None:
        """Returns the elements the extractor produced for ARRAY_OF_STRUCT
        |field|, each mapping every sub-field |field| declares to its unwrapped
        scalar value (`None` for an omitted sub-field or an INFERRED null
        branch, and for every sub-field of a literal-null element). Returns
        `None` when there is no usable output or the output omits the field
        entirely.
        """
        extracted_fields_json = self._extracted_fields_json
        if extracted_fields_json is None or field.name not in extracted_fields_json:
            return None
        elements_json = extracted_fields_json[field.name]
        if not isinstance(elements_json, list):
            raise LLMResultParsingError(
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
            raise LLMResultParsingError(
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
