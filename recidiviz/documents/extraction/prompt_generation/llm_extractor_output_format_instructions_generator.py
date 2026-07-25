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
"""Builds the `{output_instructions}` block of an extractor prompt, purely from a
collection's output schema.

This block is aimed to give the LLM guidance on the expectations for output format. This
block both gives color to the format we expect and prompt-specific field definitions.
"""
import attr

from recidiviz.common import attr_validators
from recidiviz.documents.extraction.models.json_schema_nodes import (
    ArrayJSONSchema,
    EnumJSONSchema,
    JSONScalarType,
    JSONSchemaNode,
    ObjectJSONSchema,
    ScalarJSONSchema,
)
from recidiviz.documents.extraction.models.llm_json_schema_generator import (
    LLMJsonSchemaGenerator,
)
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ApplicableWhenNonnullConstraint,
    ApplicableWhenValueConstraint,
    ArrayOfStructLLMRequestOutputSchemaField,
    ConfidenceLevel,
    DescribedEnum,
    LLMOutputFieldMode,
    LLMOutputFieldType,
    LLMOutputSemanticConsistencyConstraint,
    LLMRequestOutputSchemaField,
    NotApplicableWhenValueConstraint,
    NullReason,
    PrimitiveScalarLLMRequestOutputSchemaField,
    RequiredWhenNonnullConstraint,
    RequiredWhenValueConstraint,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    ADVERSARIAL_INTERPRETATION_FIELD_NAME,
    CONFIDENCE_LEVEL_FIELD_NAME,
    IS_RELEVANT_FIELD_NAME,
    NULL_REASON_FIELD_NAME,
    VALUE_FIELD_NAME,
)
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.string_formatting import (
    collapse_blank_lines,
    collapse_whitespace,
    fix_indent,
    render_list,
)
from recidiviz.utils.types import assert_type

# Human-readable type labels shown for each field in the "Output fields:" listing.
_FIELD_TYPE_LABELS = {
    LLMOutputFieldType.STRING: "string",
    LLMOutputFieldType.BOOLEAN: "boolean",
    LLMOutputFieldType.INTEGER: "integer",
    LLMOutputFieldType.FLOAT: "number",
    LLMOutputFieldType.ENUM: "enum",
    LLMOutputFieldType.ARRAY_OF_STRUCT: "list",
    LLMOutputFieldType.ARRAY_OF_INTEGER: "list of integers",
}


OUTPUT_FORMAT_INSTRUCTIONS_TEMPLATE = f"""
CRITICAL INSTRUCTION: First determine whether this document is relevant. A document is
considered relevant according to the following criteria:
{{relevance_criteria}}
If it is not relevant, set {IS_RELEVANT_FIELD_NAME} to false and use {NULL_REASON_FIELD_NAME}='{NullReason.NO_INFO_FOUND.value}'
for all other fields.

Output fields:
{{output_fields_description}}

{{semantic_consistency_constraints}}

Each document produces exactly one extraction result object, conforming
to the output schema supplied separately with this request. Each field
listed above is reported with a metadata wrapper, in one of two shapes
(the schema enforces exactly one). Emit only the keys shown for the
shape you use — the key that distinguishes the other shape
(`{VALUE_FIELD_NAME}` or `{NULL_REASON_FIELD_NAME}`) is absent, not
null. Within a shape, include every key shown. Exceptions:
{{metadata_wrapper_exceptions}}

When you extract a value:
{{nonnull_inferred_field_result_branch_shape}}

When you cannot extract a value:
{{null_inferred_field_result_branch_shape}}

- When `{ADVERSARIAL_INTERPRETATION_FIELD_NAME}` is non-null, set
  `{CONFIDENCE_LEVEL_FIELD_NAME}` to "{ConfidenceLevel.SPECULATIVE.value}".

`{CONFIDENCE_LEVEL_FIELD_NAME}` values, ordered from least to most
confident:
{{confidence_level_value_descriptions_list}}

`{NULL_REASON_FIELD_NAME}` values, used only on the null shape above:
{{null_reason_value_descriptions_list}}
"""

STRUCTURAL_FIELD_LABEL = "bare value"

# The value branch's `value` node is field-specific in the real schema; the prompt
# renders it generically, so this stand-in carries the generic placeholder text as
# its description (surfaced by the ScalarJSONSchema case in _placeholder_for).
_GENERIC_VALUE_NODE = ScalarJSONSchema(
    description="the extracted value", json_type=JSONScalarType.STRING
)


@attr.define(frozen=True, kw_only=True)
class JSONFieldDescriptor:
    """One key/value line of a pseudo-JSON object shape shown to the model: the
    key, a placeholder for its value, and an optional trailing annotation.
    """

    key: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The JSON key name."""

    value_placeholder: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The pseudo-JSON shown for the value (e.g. a `<...>` placeholder or a type
    union)."""

    annotation: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """An optional note rendered as a trailing `// ` comment (e.g. "at least one
    quote"), or None for no annotation."""

    def render(self) -> str:
        """Returns the line `"<key>": <value>,[  // <annotation>]`."""
        annotation = f"  // {self.annotation}" if self.annotation else ""
        return f'"{self.key}": {self.value_placeholder},{annotation}'


class LLMExtractorOutputFormatInstructionsGenerator:
    """Builds the `{output_instructions}` block of an extractor prompt."""

    @classmethod
    def generate(cls, output_schema: LLMRequestOutputSchema) -> str:
        """Returns the `{output_instructions}` block for |output_schema|. This includes
        relevance criteria, output field descriptions, semantic consistency constraint
        descriptions, and explanations of the INFERRED field wrapper shapes.
        """
        formatted = StrictStringFormatter().format(
            OUTPUT_FORMAT_INSTRUCTIONS_TEMPLATE,
            # TODO(OBT-36778): Adapt this generator to render the relevance-free,
            # all-STRUCTURAL entity-resolution schema (no `is_relevant`) when the
            # real ER clustering prompt lands. Until then prompt generation
            # supports only relevance-bearing (first-order) schemas, whose
            # `is_relevant_field` is always present.
            relevance_criteria=collapse_whitespace(
                assert_type(
                    output_schema.is_relevant_field,
                    PrimitiveScalarLLMRequestOutputSchemaField,
                ).description
            ),
            output_fields_description=cls.render_output_fields_description(
                output_schema
            ),
            semantic_consistency_constraints=cls.render_semantic_consistency_constraints(
                output_schema
            ),
            metadata_wrapper_exceptions=cls.render_metadata_wrapper_exceptions(
                output_schema
            ),
            nonnull_inferred_field_result_branch_shape=cls.render_nonnull_inferred_field_result_branch_shape(),
            null_inferred_field_result_branch_shape=cls.render_null_inferred_field_result_branch_shape(),
            confidence_level_value_descriptions_list=cls.render_described_enum_values(
                ConfidenceLevel
            ),
            null_reason_value_descriptions_list=cls.render_described_enum_values(
                NullReason
            ),
        )
        return collapse_blank_lines(formatted)

    @classmethod
    def render_output_fields_description(
        cls, output_schema: LLMRequestOutputSchema
    ) -> str:
        """Returns a bulleted listing of each output field, including its name, type,
        and description. An ARRAY_OF_STRUCT field's sub-fields nest one level beneath
        it.
        """
        listings: list[str] = []
        # all_fields (not user_defined_fields) so the framework-injected
        # is_relevant field — and any future framework field — is always listed.
        for field in output_schema.all_fields:
            listings.append(
                cls._render_field_description_list_item(field, indent_level=0)
            )
            if isinstance(field, ArrayOfStructLLMRequestOutputSchemaField):
                for sub_field in field.fields:
                    listings.append(
                        cls._render_field_description_list_item(
                            sub_field, indent_level=2
                        )
                    )
        return "\n".join(listings)

    @staticmethod
    def _render_field_description_list_item(
        field: LLMRequestOutputSchemaField, *, indent_level: int
    ) -> str:
        """Returns the list item for one field: `- <name> (<type>): <description>`,
        preserving any multi-line structure in the description (e.g. an ENUM
        field's baked-in "Allowed values:" bullet list) by hanging-indenting its
        continuation lines beneath the field's bullet.
        """
        type_label = _FIELD_TYPE_LABELS[field.field_type]
        if field.field_mode is LLMOutputFieldMode.STRUCTURAL:
            type_label = f"{type_label}, {STRUCTURAL_FIELD_LABEL}"
        field_block = f"{field.name} ({type_label}): {field.description.strip()}"
        return render_list([field_block], indent_level=indent_level)

    @classmethod
    def render_semantic_consistency_constraints(
        cls, output_schema: LLMRequestOutputSchema
    ) -> str:
        """Returns the semantic-consistency prose rendered from every field's (and
        sub-field's) constraints, or empty when there are none.
        """
        constraint_descriptions = []
        for field in output_schema.user_defined_fields:
            constraint_descriptions.extend(cls._render_field_constraints(field, []))

        if not constraint_descriptions:
            return ""
        return "Fields must be logically consistent:\n" + render_list(
            constraint_descriptions
        )

    @classmethod
    def _render_field_constraints(
        cls,
        field: LLMRequestOutputSchemaField,
        ancestor_fields: list[ArrayOfStructLLMRequestOutputSchemaField],
    ) -> list[str]:
        """Returns the constraint prose for |field| and, recursively, its
        ARRAY_OF_STRUCT sub-fields — one string per constraint, with no leading
        list dash (the caller adds it). |ancestor_fields| are the ARRAY_OF_STRUCT
        fields |field| is nested under, threaded so a sub-field's name can be
        qualified (e.g. `employers[].pay_rate_time_period`).
        """
        constraint_descriptions: list[str] = []
        for constraint in field.semantic_consistency_constraints:
            constraint_descriptions.append(
                cls._render_field_constraint(field, ancestor_fields, constraint)
            )
        if isinstance(field, ArrayOfStructLLMRequestOutputSchemaField):
            for sub_field in field.fields:
                constraint_descriptions.extend(
                    cls._render_field_constraints(sub_field, ancestor_fields + [field])
                )
        return constraint_descriptions

    @classmethod
    def _render_field_constraint(
        cls,
        field: LLMRequestOutputSchemaField,
        ancestor_fields: list[ArrayOfStructLLMRequestOutputSchemaField],
        constraint: LLMOutputSemanticConsistencyConstraint,
    ) -> str:
        """Returns the prose for a single |constraint| on |field|. |ancestor_fields| are
        the ARRAY_OF_STRUCT fields |field| is nested under, used to qualify its name
        (e.g. `employers[].pay_rate_time_period` for a sub-field of `employers`).
        """
        name = (
            "".join(f"{ancestor.name}[]." for ancestor in ancestor_fields) + field.name
        )
        is_list_field = isinstance(field, ArrayOfStructLLMRequestOutputSchemaField)
        otherwise_clause = (
            "leave it empty"
            if is_list_field
            else f"set it to null with null_reason='{NullReason.NOT_APPLICABLE.value}'"
        )
        if isinstance(constraint, ApplicableWhenValueConstraint):
            values = ", ".join(f"'{value}'" for value in constraint.values)
            return (
                f"`{name}` applies only when "
                f"`{constraint.condition_field.name}` is one of [{values}]; "
                f"otherwise {otherwise_clause}."
            )
        if isinstance(constraint, NotApplicableWhenValueConstraint):
            values = ", ".join(f"'{value}'" for value in constraint.values)
            requirement = (
                "must be empty"
                if is_list_field
                else f"must be null with null_reason='{NullReason.NOT_APPLICABLE.value}'"
            )
            return (
                f"`{name}` {requirement} when "
                f"`{constraint.condition_field.name}` is one of [{values}]."
            )
        if isinstance(constraint, ApplicableWhenNonnullConstraint):
            return (
                f"`{name}` applies only when "
                f"`{constraint.condition_field.name}` is set; otherwise "
                f"{otherwise_clause}."
            )
        if isinstance(constraint, RequiredWhenNonnullConstraint):
            requirement = (
                "must have at least one entry" if is_list_field else "must be set"
            )
            return (
                f"`{name}` {requirement} when "
                f"`{constraint.condition_field.name}` is set."
            )
        if isinstance(constraint, RequiredWhenValueConstraint):
            values = ", ".join(f"'{value}'" for value in constraint.values)
            requirement = (
                "must have at least one entry" if is_list_field else "must be set"
            )
            return (
                f"`{name}` {requirement} when "
                f"`{constraint.condition_field.name}` is one of [{values}]."
            )
        raise ValueError(f"Unexpected constraint type: [{type(constraint)}].")

    @classmethod
    def render_metadata_wrapper_exceptions(
        cls, output_schema: LLMRequestOutputSchema
    ) -> str:
        """Returns a bulleted list describing the fields whose values are not wrapped
        in the metadata wrapper structure used for INFERRED fields.
        """
        exceptions = [
            f'fields tagged "{STRUCTURAL_FIELD_LABEL}", which you output directly'
        ]
        if any(
            isinstance(field, ArrayOfStructLLMRequestOutputSchemaField)
            for field in output_schema.user_defined_fields
        ):
            exceptions.append(
                '"list" fields, which are arrays whose elements are objects (each '
                "sub-field follows these same rules)"
            )
        return render_list(exceptions)

    @classmethod
    def render_nonnull_inferred_field_result_branch_shape(cls) -> str:
        """Returns the pseudo-JSON for the result shape of an INFERRED field
        when the inferred value is non-null.
        """
        branch = LLMJsonSchemaGenerator.nonnull_inferred_field_branch_schema(
            value_node=_GENERIC_VALUE_NODE
        )
        return fix_indent(cls._render_object_json_schema(branch), indent_level=2)

    @classmethod
    def render_null_inferred_field_result_branch_shape(cls) -> str:
        """Returns the pseudo-JSON for the result shape of an INFERRED field
        when the inferred value is null.
        """
        branch = LLMJsonSchemaGenerator.null_inferred_field_branch_schema()
        return fix_indent(cls._render_object_json_schema(branch), indent_level=2)

    @classmethod
    def _render_object_json_schema(cls, node: ObjectJSONSchema) -> str:
        """Returns the multi-line pseudo-JSON object shape for |node| — one property
        per line, in schema order — with each property's placeholder and annotation
        derived from its schema node, and nested objects rendered multi-line too.
        """
        field_lines: list[str] = []
        for key, child in node.properties.items():
            field_lines.append(
                JSONFieldDescriptor(
                    key=key,
                    value_placeholder=cls._placeholder_for(child),
                    annotation=cls._annotation_for(child),
                ).render()
            )
        return "{\n" + fix_indent("\n".join(field_lines), indent_level=2) + "\n}"

    @classmethod
    def _placeholder_for(cls, node: JSONSchemaNode) -> str:
        """Returns the pseudo-JSON value placeholder for one property |node|,
        derived from its schema so the shape mirrors the response schema.
        """
        if isinstance(node, EnumJSONSchema):
            return " | ".join(f'"{value}"' for value in node.values)
        if isinstance(node, ScalarJSONSchema):
            return f"<{collapse_whitespace(node.description)}>"
        if isinstance(node, ArrayJSONSchema):
            return f"[\n{fix_indent(cls._placeholder_for(node.items), indent_level=2)},\n  ...\n]"
        if isinstance(node, ObjectJSONSchema):
            return cls._render_object_json_schema(node)
        raise ValueError(f"Cannot render a prompt placeholder for node [{type(node)}].")

    @staticmethod
    def _annotation_for(node: JSONSchemaNode) -> str | None:
        """Returns the trailing `// ` annotation for one property |node|, or None.
        An array carries its own guidance (cardinality, when to include an entry)
        as a comment, since its placeholder shows only the element shape.
        """
        if isinstance(node, ArrayJSONSchema):
            return collapse_whitespace(node.description)
        return None

    @staticmethod
    def render_described_enum_values(enum_cls: type[DescribedEnum]) -> str:
        """Returns a bulleted `- "<value>": <meaning>` list (one per member of
        |enum_cls|, in declaration order), sourced from the enum's own
        `get_value_descriptions()` so the prompt's value meanings cannot drift from
        the generated schema's.
        """
        return render_list(
            f'"{member.value}": {collapse_whitespace(description)}'
            for member, description in enum_cls.get_value_descriptions().items()
        )
