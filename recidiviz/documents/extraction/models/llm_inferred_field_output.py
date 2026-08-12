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
"""Typed view over the JSON wrapper an extractor emits for a single INFERRED
field: its extracted value (or null branch) plus the companion metadata that
the validation checks read.

The view is *live* over a node of a result's output JSON: the accessors read
through to that JSON.
"""
from typing import Any

import attr

from recidiviz.common import attr_validators
from recidiviz.documents.extraction.exceptions import LLMOutputParsingError
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ConfidenceLevel,
    LLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    CONFIDENCE_LEVEL_FIELD_NAME,
    NULL_REASON_FIELD_NAME,
    VALUE_FIELD_NAME,
)


@attr.define(frozen=True, kw_only=True)
class InferredFieldOutput:
    """What one INFERRED field's JSON wrapper carries in one extraction result:
    which branch it took (a value, or a null reason), and the companion metadata
    the model emitted alongside.
    """

    field: LLMRequestOutputSchemaField = attr.ib()
    """The INFERRED schema field this output was produced for."""

    @field.validator
    def _check_field(self, _attribute: "attr.Attribute", value: object) -> None:
        # `attr.validators.instance_of` can't be used here because
        # LLMRequestOutputSchemaField is abstract (mypy `type-abstract`).
        if not isinstance(value, LLMRequestOutputSchemaField):
            raise ValueError(
                f"field must be an output schema field, received [{type(value)}]."
            )
        if not value.is_inferred_field:
            raise ValueError(
                f"field [{value.name}] is STRUCTURAL, but this view reads the "
                f"companion metadata only an INFERRED field carries."
            )

    display_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """How the field is named in an audit finding: its own name at the top level,
    or `<array_field>[<index>].<name>` within an ARRAY_OF_STRUCT element."""

    field_json: dict[str, Any] = attr.ib(validator=attr_validators.is_dict)
    """The field's companion-metadata wrapper exactly as it sits in the output
    JSON, which may or may not have been through validation.
    """

    @property
    def has_value(self) -> bool:
        """Returns whether the field took its value branch — i.e. the model
        extracted a value rather than reporting a null reason.

        Raises an LLMOutputParsingError when the wrapper neither extracted a value nor
        reported a null reason.
        """
        if VALUE_FIELD_NAME in self.field_json:
            return True
        # An INFERRED field's null branch carries a null_reason in place of the
        # value key.
        if NULL_REASON_FIELD_NAME in self.field_json:
            return False
        raise LLMOutputParsingError(
            f"INFERRED field [{self.display_name}] holds neither a "
            f"[{VALUE_FIELD_NAME}] nor a [{NULL_REASON_FIELD_NAME}]. Found keys: "
            f"{sorted(self.field_json)}."
        )

    @property
    def value(self) -> Any:
        """Returns the extracted value, or None when the field took its null
        branch."""
        if not self.has_value:
            return None
        return self.field_json[VALUE_FIELD_NAME]

    @property
    def confidence_level(self) -> ConfidenceLevel:
        """Returns the evidence quality level the model reported for this field.

        Raises an LLMOutputParsingError when the wrapper has a malformed
        confidence_level (omits the key or holds a value that is not a
        `ConfidenceLevel`).
        """
        if CONFIDENCE_LEVEL_FIELD_NAME not in self.field_json:
            raise LLMOutputParsingError(
                f"INFERRED field [{self.display_name}] holds no "
                f"[{CONFIDENCE_LEVEL_FIELD_NAME}]. Found keys: "
                f"{sorted(self.field_json)}."
            )
        confidence_level = self.field_json[CONFIDENCE_LEVEL_FIELD_NAME]
        try:
            return ConfidenceLevel(confidence_level)
        except ValueError as e:
            raise LLMOutputParsingError(
                f"INFERRED field [{self.display_name}] holds an unrecognized "
                f"[{CONFIDENCE_LEVEL_FIELD_NAME}] of [{confidence_level}]."
            ) from e
