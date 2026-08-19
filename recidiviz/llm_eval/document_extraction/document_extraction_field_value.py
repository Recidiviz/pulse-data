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
"""Holds one value an extractor put in one field of one document, which is the thing a
human annotates.

Identifying a value takes more than a field name. A document has exactly one primary_status,
but one naming two employers has two job_titles, so a value from an array element also
carries which array it came from and the element's position. Both are None for a value that
sits at the document's top level, including the value saying an array came back empty.

Reading the employment result of a note that names two employers, the second of which gave
no job title, produces these values.

    field_name       array_field_name  source_array_index  extracted_value
    primary_status   None              None                employed
    employer_name    employers         0                   Walmart
    job_title        employers         0                   cashier
    employer_name    employers         1                   Ace Hardware
    job_title        employers         1                   None

LLMRequestOutputValues does the reading, unwrapping the result envelope and each INFERRED
field's wrapper and reporting the companion metadata alongside. Nothing here parses JSON.
"""
import json
from typing import Any

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.models.llm_inferred_field_output import (
    LLMInferredFieldOutput,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ConfidenceLevel,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)


@attr.define(frozen=True, kw_only=True)
class DocumentExtractionFieldValue:
    """One value an extractor put in one field of one document, with everything an annotator
    needs to judge whether it is right. A value from an array element also carries which array
    it came from and the element's position, since a document naming two employers has two
    job_titles.
    """

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))
    """State the document belongs to."""

    document_contents_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Identifier of the document text the value was extracted from."""

    document_text: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The full text of that document, which an annotator reads to judge the value."""

    extractor_version_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Version of the extractor config that produced the value."""

    field_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Name of the output-schema field the value was extracted into."""

    array_field_name: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """Name of the ARRAY_OF_STRUCT field whose element this value came from, or None when
    the field sits at the document's top level.
    """

    source_array_index: int | None = attr.ib(
        validator=attr_validators.is_opt_non_negative_int
    )
    """0-indexed position of the element within its array, or None when the value is not in
    an array.
    """

    extracted_value: Any = attr.ib()
    """The value the model extracted, as the extractor returned it, so a bool stays a bool
    and a number stays a number. None when the model extracted nothing, which the export
    still annotates, because a null asserts that the document says nothing about the field
    and that assertion can be wrong.

    Carries no validator because what a field may hold is the output schema's business, not
    this class's. Read extracted_value_str to render it for a human.
    """

    confidence_level: ConfidenceLevel | None = attr.ib(
        validator=attr_validators.is_opt(ConfidenceLevel)
    )
    """How confident the model reported being in the value. None only for the value saying
    an array field came back empty, because an ARRAY_OF_STRUCT field carries no
    companion-metadata wrapper of its own and so reports no confidence.
    """

    array_element_json: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """The whole array element this value came from, serialized as JSON. It holds every
    sub-field the schema declares, this value's own included, and keeps the null ones, since
    an annotator learns something from seeing that the model found no location for an
    employer. None for a top-level field, which sits in no element.

    An annotator judging a job_title of "cashier" needs the employer_name beside it to know
    which job the question is about.
    """

    @classmethod
    def from_extraction_result(
        cls,
        *,
        output_values: LLMRequestOutputValues,
        state_code: StateCode,
        document_contents_id: str,
        document_text: str,
        extractor_version_id: str,
    ) -> list["DocumentExtractionFieldValue"]:
        """Returns every annotatable value one document's extraction result holds, which is
        each INFERRED top-level field, each INFERRED sub-field of each array element, and one
        value per array field the model returned empty.

        An array field the result omits entirely, as an older extractor version's output
        would, yields nothing. Omitting a field says nothing, whereas returning it empty
        asserts that the document names none.

        Args:
            output_values: The result to read, paired with the schema that interprets it.
            state_code: State the document belongs to.
            document_contents_id: Identifier of the document text the result was extracted
                from.
            document_text: That document's full text, which an annotator reads.
            extractor_version_id: Version of the extractor config that produced the result.
        """
        element_json_by_coordinates = cls._element_json_by_coordinates(output_values)
        field_values = [
            cls(
                state_code=state_code,
                document_contents_id=document_contents_id,
                document_text=document_text,
                extractor_version_id=extractor_version_id,
                field_name=output.field.name,
                array_field_name=output.array_field_name,
                source_array_index=output.source_array_index,
                extracted_value=output.value,
                confidence_level=output.confidence_level,
                array_element_json=cls._array_element_json_for(
                    output, element_json_by_coordinates
                ),
            )
            for output in output_values.inferred_field_outputs()
        ]
        return field_values + [
            cls(
                state_code=state_code,
                document_contents_id=document_contents_id,
                document_text=document_text,
                extractor_version_id=extractor_version_id,
                field_name=array_field.name,
                array_field_name=None,
                source_array_index=None,
                extracted_value=None,
                # Only an array field's sub-fields carry a companion-metadata wrapper, so
                # the array field itself reports no confidence.
                confidence_level=None,
                array_element_json=None,
            )
            for array_field in output_values.output_schema.array_of_struct_user_fields
            if array_field.is_inferred_field
            and output_values.array_elements(field=array_field) == []
        ]

    @staticmethod
    def _array_element_json_for(
        output: LLMInferredFieldOutput,
        element_json_by_coordinates: dict[tuple[str, int], str],
    ) -> str | None:
        """Returns the JSON of the array element the given output came from, or None when it
        is a top-level field and sits in no element.

        Indexes rather than defaulting, because both walks cover the same elements. A missing
        key means they have drifted apart, which should fail rather than pass silently.
        """
        if output.array_field_name is None or output.source_array_index is None:
            return None
        return element_json_by_coordinates[
            (output.array_field_name, output.source_array_index)
        ]

    @staticmethod
    def _element_json_by_coordinates(
        output_values: LLMRequestOutputValues,
    ) -> dict[tuple[str, int], str]:
        """Returns each array element's full set of extracted values as JSON, keyed by the
        array field name and the element's position.
        """
        array_element_json: dict[tuple[str, int], str] = {}
        for array_field in output_values.output_schema.array_of_struct_user_fields:
            elements = output_values.array_elements(field=array_field)
            if not elements:
                continue
            for index, element in enumerate(elements):
                array_element_json[(array_field.name, index)] = json.dumps(element)
        return array_element_json

    def __attrs_post_init__(self) -> None:
        if (self.array_field_name is None) != (self.source_array_index is None):
            raise ValueError(
                f"Field value [{self.field_name}] of document "
                f"[{self.document_contents_id}] sets exactly one of array_field_name "
                f"[{self.array_field_name}] and source_array_index "
                f"[{self.source_array_index}]. An array element value carries both; a "
                f"top-level field value carries neither."
            )

    @property
    def is_null(self) -> bool:
        """Returns whether the model extracted no value for this field."""
        return self.extracted_value is None

    @property
    def has_inferred_confidence(self) -> bool:
        """Returns whether the model reported only INFERRED confidence in this value,
        meaning it followed from an inferential step rather than the document stating it
        outright. That makes it likelier to be wrong, so the export over-samples these.
        """
        return self.confidence_level is ConfidenceLevel.INFERRED

    @property
    def array_element_key(self) -> tuple[str, str, int] | None:
        """Returns the document, array field, and position identifying the array element
        this value came from, or None when it is a top-level field.

        Two values share a key exactly when they are sub-fields of the same element, which is
        how the export groups them. Exporting a job_title without the employer_name beside it
        would be exporting an unanswerable question.
        """
        if self.array_field_name is None or self.source_array_index is None:
            return None
        return (
            self.document_contents_id,
            self.array_field_name,
            self.source_array_index,
        )

    @property
    def extracted_value_str(self) -> str | None:
        """Returns the extracted value rendered for a human to read, or None when the model
        extracted none. A bool renders as "True" or "False" and a number by its Python repr,
        which suits the only consumer, an annotator reading one field at a time.
        """
        if self.extracted_value is None:
            return None
        return str(self.extracted_value)

    def display_value(self, *, null_sentinel: str) -> str:
        """Returns the extracted value as an annotator should see it, substituting
        |null_sentinel| when the model extracted nothing. "The document says nothing about
        this field" is a claim the annotator has to be able to read and disagree with, so
        it can never be shown as a blank.
        """
        if (extracted_value_str := self.extracted_value_str) is None:
            return null_sentinel
        return extracted_value_str

    @property
    def display_group(self) -> str:
        """Returns where this value came from in a form a human reads, such as
        "employers[0]", or empty string for a top-level field.
        """
        if self.array_field_name is None:
            return ""
        return f"{self.array_field_name}[{self.source_array_index}]"

    @property
    def file_safe_field_id(self) -> str:
        """Returns an identifier for this value that is safe to put in a filename:
        "job_title" for a top-level field, "employers_0__job_title" for a value inside
        an array element.
        """
        if self.array_field_name is None:
            return self.field_name
        return f"{self.array_field_name}_{self.source_array_index}__{self.field_name}"
