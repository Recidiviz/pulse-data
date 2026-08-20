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
"""Answers which of an extractor's fields a human can be asked to check, and what each one
means.

Only INFERRED fields qualify. A STRUCTURAL field holds a value the model composed rather
than a judgment about the document, so there is nothing to be right or wrong about.
LLMRequestOutputValues.inferred_field_outputs applies that same rule to an actual result;
these functions answer the schema question of what a task could be about, without a result
in hand.
"""
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfStructLLMRequestOutputSchemaField,
)


def scalar_top_level_annotatable_field_names(
    output_schema: LLMRequestOutputSchema,
) -> set[str]:
    """Returns every INFERRED scalar field the schema declares at the document's top level,
    which excludes the array fields that also stand there. A document holds exactly one
    annotated value under each of these names.

    Validate against this rather than annotatable_field_names when every document must hold
    the named value.

    For a collection declaring a primary_status ENUM, a status_note STRUCTURAL summary, and an
    employers array of employer_name and job_title, this returns

        {"primary_status"}

    status_note is absent because it is STRUCTURAL. employer_name and job_title are absent
    because they sit inside array elements rather than at the top level. employers does stand
    at the top level, but it is absent too: a document whose array came back empty holds one
    value under that name, and a document that populated the array holds none.
    """
    return {
        field.name
        for field in output_schema.scalar_valued_user_fields
        if field.is_inferred_field
    }


def annotatable_field_names(output_schema: LLMRequestOutputSchema) -> set[str]:
    """Returns every field name an annotated value can be about, which is the INFERRED scalar
    fields, the INFERRED sub-fields of every array field, and each array field itself.
    Validate a caller-supplied field name against this.

    For a collection declaring a primary_status ENUM, a status_note STRUCTURAL summary, and an
    employers array of employer_name and job_title, this returns

        {"primary_status", "employers", "employer_name", "job_title"}

    employers is present even though a populated array is annotated one sub-field at a time,
    because a document whose array came back empty gets one value annotated on the array field
    itself, asserting that the note names no employers. Those array field names belong here
    only for that reason. Stop emitting those values and drop these names too.

    A name in here but not in scalar_top_level_annotatable_field_names came from an array
    field, which a document holds either many values under (a sub-field, one per element) or
    one (the array field itself, only when the array came back empty).
    """
    field_names = scalar_top_level_annotatable_field_names(output_schema)
    for array_field in output_schema.array_of_struct_user_fields:
        # An ARRAY_OF_STRUCT field is INFERRED exactly when it has an
        # INFERRED sub-field, so this skips arrays with nothing to annotate.
        if not array_field.is_inferred_field:
            continue
        field_names.add(array_field.name)
        field_names.update(
            sub_field.name
            for sub_field in array_field.fields
            if sub_field.is_inferred_field
        )
    return field_names


def annotatable_field_description(
    output_schema: LLMRequestOutputSchema,
    *,
    field_name: str,
    array_field_name: str | None,
) -> str:
    """Returns the output schema's description of one annotated field, which is what tells
    an annotator what the field is supposed to capture. An ENUM field's description spells
    out every allowed value and its meaning.

    Args:
        output_schema: The schema declaring the field.
        field_name: Name of the field to describe.
        array_field_name: Name of the ARRAY_OF_STRUCT field whose sub-fields to search, or
            None to look field_name up among the top-level fields.
    """
    if array_field_name is None:
        return output_schema.get_field(field_name).description
    array_field = output_schema.get_field(array_field_name)
    if not isinstance(array_field, ArrayOfStructLLMRequestOutputSchemaField):
        raise ValueError(
            f"Field [{array_field_name}] is not an ARRAY_OF_STRUCT field, so it has no "
            f"sub-field [{field_name}] to describe."
        )
    return array_field.get_field(field_name).description
