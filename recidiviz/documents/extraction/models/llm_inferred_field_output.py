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
"""Typed views over the JSON wrapper an extractor emits for a single INFERRED
field: its extracted value (or null branch) plus the companion metadata —
confidence level, adversarial interpretation, and citations — that the
validation checks read.

Each class here is a *live view* over a node of a result's output JSON: the
accessors read through to that JSON, and `null_out_value` writes through to it.
Validation relies on that: the quality filters walk a deep copy of the raw output
and edit it in place to produce the validated copy.
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
    ADVERSARIAL_INTERPRETATION_FIELD_NAME,
    CITATION_END_FIELD_NAME,
    CITATION_START_FIELD_NAME,
    CITATION_TEXT_FIELD_NAME,
    CITATIONS_FIELD_NAME,
    CONFIDENCE_LEVEL_FIELD_NAME,
    NULL_REASON_FIELD_NAME,
    VALUE_FIELD_NAME,
)


@attr.define(frozen=True, kw_only=True)
class FieldCitation:
    """One element of an INFERRED field's `citations` array: an exact quote from
    the source document plus the character offsets the model reported it at.

    This is a live view over the citation object as it lives in the result's
    output JSON — `correct_offsets` writes through to that JSON.
    """

    citation_json: dict[str, Any] = attr.ib(validator=attr_validators.is_dict)
    """The citation object exactly as it sits in the output JSON."""

    field_display_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Display name of the field this citation supports — e.g. `location` or
    `assignments[0].rate_amount` — so an issue raised about the citation names
    the field it came from."""

    index: int = attr.ib(validator=attr_validators.is_non_negative_int)
    """This citation's 0-indexed position within its field's `citations` array."""

    @property
    def text(self) -> str:
        """Returns the quoted text the model attributes to the source document.

        Raises an LLMOutputParsingError when the citation holds no quoted text.
        """
        return self._field(CITATION_TEXT_FIELD_NAME)

    @property
    def start(self) -> int:
        """Returns the character offset in the source document the model reported
        the quote begins at.

        Raises an LLMOutputParsingError when the citation holds no start offset.
        """
        return self._field(CITATION_START_FIELD_NAME)

    @property
    def end(self) -> int:
        """Returns the character offset in the source document the model reported
        the quote ends at.

        Raises an LLMOutputParsingError when the citation holds no end offset.
        """
        return self._field(CITATION_END_FIELD_NAME)

    def _field(self, field_name: str) -> Any:
        """Returns |field_name| from this citation's JSON, raising an
        LLMOutputParsingError when it is missing rather than a bare KeyError."""
        if field_name not in self.citation_json:
            raise LLMOutputParsingError(
                f"Citation [{self.index}] on field [{self.field_display_name}] "
                f"holds no [{field_name}]. Found keys: "
                f"{sorted(self.citation_json)}."
            )
        return self.citation_json[field_name]

    def correct_offsets(self, *, start: int, end: int) -> None:
        """Rewrites this citation's reported offsets to |start| / |end| in the
        output JSON this view reads from."""
        self.citation_json[CITATION_START_FIELD_NAME] = start
        self.citation_json[CITATION_END_FIELD_NAME] = end


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

    @property
    def adversarial_interpretation(self) -> str | None:
        """Returns the strongest alternative reading of the source text the model
        recorded, or None when it found no reasonable alternative."""
        if ADVERSARIAL_INTERPRETATION_FIELD_NAME not in self.field_json:
            raise LLMOutputParsingError(
                f"INFERRED field [{self.display_name}] holds no "
                f"[{ADVERSARIAL_INTERPRETATION_FIELD_NAME}]. Found keys: "
                f"{sorted(self.field_json)}."
            )
        return self.field_json[ADVERSARIAL_INTERPRETATION_FIELD_NAME]

    @property
    def citations(self) -> list[FieldCitation]:
        """Returns the quotes the model offered in support of this field's value
        (or of the absence of one), in the order it emitted them.

        Raises an LLMOutputParsingError when the wrapper holds no citations key.
        """
        if CITATIONS_FIELD_NAME not in self.field_json:
            raise LLMOutputParsingError(
                f"INFERRED field [{self.display_name}] holds no "
                f"[{CITATIONS_FIELD_NAME}]. Found keys: {sorted(self.field_json)}."
            )
        return [
            FieldCitation(
                citation_json=citation_json,
                field_display_name=self.display_name,
                index=index,
            )
            for index, citation_json in enumerate(self.field_json[CITATIONS_FIELD_NAME])
        ]
