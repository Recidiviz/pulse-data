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
"""Generates the JSON Schema we hand the LLM API (with `strict: true`) from an
extractor collection's parsed `LLMRequestOutputSchema`.

This is a pure lowering pass over the typed output schema — no config is read
here. It composes the typed nodes in `json_schema_nodes` rather than nesting
dict literals. The shape it builds encodes several provider-specific decisions,
kept together here rather than spread across the model classes:

  - The relevance `anyOf` is placed on a `result` property rather than the root,
    because Vertex AI rejects `anyOf` alongside `type` on the same schema node.
    Its first branch is the short irrelevant output (`is_relevant` only), its
    second the full relevant output.
  - Each INFERRED scalar/enum field is itself an `anyOf` of a nonnull-value
    branch and a null branch, structurally enforcing that exactly one of `value`
    / `null_reason` is present.
  - Within each INFERRED field the companion keys are emitted in a fixed,
    semantically meaningful order: `adversarial_interpretation` first (so the
    model weighs alternative readings before committing), then the
    `value` / `null_reason`, then `confidence_level`, then `citations`. This
    order must be preserved when serializing.
  - On the value branch, `citations` carries `minItems: 1` — a value extraction
    must cite its source; on the null branch citations are optional.

STRUCTURAL fields are emitted as bare values (no companion metadata), and
ARRAY_OF_STRUCT fields as bare arrays whose item sub-fields recurse through the
same per-field lowering.
"""
from recidiviz.documents.extraction.models.json_schema_nodes import (
    AnyOfJSONSchema,
    ArrayJSONSchema,
    ConstBooleanJSONSchema,
    EnumJSONSchema,
    JSONScalarType,
    JSONSchemaDict,
    JSONSchemaNode,
    ObjectJSONSchema,
    ScalarJSONSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfStructLLMRequestOutputSchemaField,
    ConfidenceLevel,
    DescribedEnum,
    EnumLLMRequestOutputSchemaField,
    LLMOutputFieldMode,
    LLMOutputFieldType,
    LLMRequestOutputSchemaField,
    NullReason,
    ScalarLLMRequestOutputSchemaField,
    description_with_enum_value_guidance,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    ADVERSARIAL_INTERPRETATION_FIELD_NAME,
    CITATION_END_FIELD_NAME,
    CITATION_START_FIELD_NAME,
    CITATION_TEXT_FIELD_NAME,
    CITATIONS_FIELD_NAME,
    CONFIDENCE_LEVEL_FIELD_NAME,
    IS_RELEVANT_FIELD_NAME,
    NULL_REASON_FIELD_NAME,
    RESULT_KEY,
    VALUE_FIELD_NAME,
)


class LLMJsonSchemaGenerator:
    """Generates the JSON Schema dict sent to the LLM API from a parsed
    `LLMRequestOutputSchema`.
    """

    @classmethod
    def generate(cls, output_schema: LLMRequestOutputSchema) -> JSONSchemaDict:
        """Returns the JSON Schema dict for |output_schema| — an object with a
        single `result` property holding the irrelevant/relevant `anyOf`.
        """
        return ObjectJSONSchema(
            description=output_schema.result_level_description,
            properties={
                RESULT_KEY: AnyOfJSONSchema(
                    branches=[
                        cls._irrelevant_document_schema(output_schema),
                        cls._relevant_branch_schema(output_schema),
                    ]
                )
            },
            required=[RESULT_KEY],
        ).to_json_schema()

    @classmethod
    def _irrelevant_document_schema(
        cls, output_schema: LLMRequestOutputSchema
    ) -> ObjectJSONSchema:
        """Returns the schema the model should emit for a document where
        is_relevant=False.
        """
        return ObjectJSONSchema(
            description="Use when the document is NOT relevant.",
            properties={
                IS_RELEVANT_FIELD_NAME: cls._is_relevant_schema(
                    output_schema, is_relevant_value=False
                )
            },
            required=[IS_RELEVANT_FIELD_NAME],
        )

    @classmethod
    def _relevant_branch_schema(
        cls, output_schema: LLMRequestOutputSchema
    ) -> ObjectJSONSchema:
        """Returns the schema the model should emit for a document with
        is_relevant=True.
        """
        properties: dict[str, JSONSchemaNode] = {
            IS_RELEVANT_FIELD_NAME: cls._is_relevant_schema(
                output_schema, is_relevant_value=True
            )
        }
        for field in output_schema.user_defined_fields:
            properties[field.name] = cls._field_schema(field)
        return ObjectJSONSchema(
            description="Use when the document IS relevant.",
            properties=properties,
            required=[
                IS_RELEVANT_FIELD_NAME,
                *(
                    field.name
                    for field in output_schema.user_defined_fields
                    if field.required
                ),
            ],
        )

    @classmethod
    def _is_relevant_schema(
        cls, output_schema: LLMRequestOutputSchema, *, is_relevant_value: bool
    ) -> ConstBooleanJSONSchema:
        """Returns the `is_relevant` node, pinned to |is_relevant_value| via
        `const` so the relevance value is fixed per branch.
        """
        return ConstBooleanJSONSchema(
            description=output_schema.is_relevant_field.description,
            value=is_relevant_value,
        )

    @classmethod
    def _field_schema(cls, field: LLMRequestOutputSchemaField) -> JSONSchemaNode:
        """Returns the schema for a single field, dispatching on its type and mode."""
        if isinstance(field, ArrayOfStructLLMRequestOutputSchemaField):
            return cls._array_field_schema(field)

        if field.field_mode is LLMOutputFieldMode.INFERRED:
            return AnyOfJSONSchema(
                branches=[
                    cls._nonnull_value_inferred_field_schema(field),
                    cls._null_value_inferred_field_schema(),
                ]
            )
        if field.field_mode is LLMOutputFieldMode.STRUCTURAL:
            return cls._bare_field_value_schema(field)

        raise ValueError(
            f"Unexpected field type [{type(field)}] and mode [{field.field_mode}]."
        )

    @classmethod
    def _array_field_schema(
        cls, field: ArrayOfStructLLMRequestOutputSchemaField
    ) -> ArrayJSONSchema:
        """Returns the schema for an ARRAY_OF_STRUCT field, with properties that contain
        the schemas of each sub-field.
        """
        return ArrayJSONSchema(
            description=field.description,
            items=ObjectJSONSchema(
                description="A single element of the array.",
                properties={
                    sub_field.name: cls._field_schema(sub_field)
                    for sub_field in field.fields
                },
                required=[
                    sub_field.name for sub_field in field.fields if sub_field.required
                ],
            ),
        )

    @classmethod
    def _nonnull_value_inferred_field_schema(
        cls, field: LLMRequestOutputSchemaField
    ) -> ObjectJSONSchema:
        """Returns the schema used for an INFERRED field when the inferred value is
        nonnull.
        """
        return ObjectJSONSchema(
            description="A value was extracted.",
            properties={
                ADVERSARIAL_INTERPRETATION_FIELD_NAME: cls._adversarial_interpretation_schema(),
                VALUE_FIELD_NAME: cls._bare_field_value_schema(field),
                CONFIDENCE_LEVEL_FIELD_NAME: cls._confidence_level_schema(),
                CITATIONS_FIELD_NAME: ArrayJSONSchema(
                    description=(
                        "Exact quotes from the document supporting the extracted "
                        "value; at least one is required."
                    ),
                    items=cls._citation_item_schema(),
                    min_items=1,
                ),
            },
            required=[
                ADVERSARIAL_INTERPRETATION_FIELD_NAME,
                VALUE_FIELD_NAME,
                CONFIDENCE_LEVEL_FIELD_NAME,
                CITATIONS_FIELD_NAME,
            ],
        )

    @classmethod
    def _null_value_inferred_field_schema(cls) -> ObjectJSONSchema:
        """Returns the schema used for an INFERRED field when the inferred value is
        null.
        """
        return ObjectJSONSchema(
            description="No value could be extracted.",
            properties={
                ADVERSARIAL_INTERPRETATION_FIELD_NAME: cls._adversarial_interpretation_schema(),
                NULL_REASON_FIELD_NAME: cls._described_enum_schema(
                    enum_cls=NullReason,
                    description="Why no value could be extracted for this field.",
                ),
                CONFIDENCE_LEVEL_FIELD_NAME: cls._confidence_level_schema(),
                CITATIONS_FIELD_NAME: ArrayJSONSchema(
                    description=(
                        "Exact quotes from the document supporting why no value "
                        "was extracted (optional)."
                    ),
                    items=cls._citation_item_schema(),
                ),
            },
            required=[
                ADVERSARIAL_INTERPRETATION_FIELD_NAME,
                NULL_REASON_FIELD_NAME,
                CONFIDENCE_LEVEL_FIELD_NAME,
            ],
        )

    @classmethod
    def _bare_field_value_schema(
        cls, field: LLMRequestOutputSchemaField
    ) -> JSONSchemaNode:
        """Returns the schema for a field's bare value. Used as the property schema
        itself for a STRUCTURAL field, or the `value` property within an INFERRED
        field's nonnull value branch.
        """
        if isinstance(field, EnumLLMRequestOutputSchemaField):
            return EnumJSONSchema(
                description=field.description,
                values=field.value_names,
            )
        if isinstance(field, ScalarLLMRequestOutputSchemaField):
            return ScalarJSONSchema(
                description=field.description,
                json_type=cls._json_scalar_type(field.scalar_type),
            )
        raise ValueError(
            f"Cannot build a bare value schema for field [{field.name}] of type "
            f"[{field.field_type}]."
        )

    @staticmethod
    def _described_enum_schema(
        *, enum_cls: type[DescribedEnum], description: str
    ) -> EnumJSONSchema:
        """Returns an enum node whose allowed values come from |enum_cls|, with
        each value's meaning baked into the description.
        """
        return EnumJSONSchema.for_enum(
            enum_cls=enum_cls,
            description=description_with_enum_value_guidance(
                description=description, enum_cls=enum_cls
            ),
        )

    @staticmethod
    def _json_scalar_type(scalar_type: LLMOutputFieldType) -> JSONScalarType:
        """Returns the JSON Schema scalar type for a scalar output field type."""
        if scalar_type is LLMOutputFieldType.STRING:
            return JSONScalarType.STRING
        if scalar_type is LLMOutputFieldType.BOOLEAN:
            return JSONScalarType.BOOLEAN
        if scalar_type is LLMOutputFieldType.INTEGER:
            return JSONScalarType.INTEGER
        if scalar_type is LLMOutputFieldType.FLOAT:
            return JSONScalarType.NUMBER
        raise ValueError(f"Unexpected scalar field type: [{scalar_type}].")

    @classmethod
    def _confidence_level_schema(cls) -> EnumJSONSchema:
        """Returns the schema for the `confidence_level` property."""
        return cls._described_enum_schema(
            enum_cls=ConfidenceLevel,
            description="The evidence quality behind this field's value.",
        )

    @staticmethod
    def _adversarial_interpretation_schema() -> ScalarJSONSchema:
        """Returns the schema for the nullable `adversarial_interpretation`
        property.
        """
        return ScalarJSONSchema(
            description=(
                "The strongest alternative reading of the source text — how one "
                "could reasonably arrive at a different value, or why a value "
                "might be present when none was extracted. Null when no "
                "reasonable alternative exists."
            ),
            json_type=JSONScalarType.STRING,
            nullable=True,
        )

    @staticmethod
    def _citation_item_schema() -> ObjectJSONSchema:
        """Returns the schema for a single citation object in a |citations| array."""
        return ObjectJSONSchema(
            description="A single quote locating supporting text within the document.",
            properties={
                CITATION_TEXT_FIELD_NAME: ScalarJSONSchema(
                    description="The exact quoted text from the document.",
                    json_type=JSONScalarType.STRING,
                ),
                CITATION_START_FIELD_NAME: ScalarJSONSchema(
                    description="Character offset in the document where the quoted text begins.",
                    json_type=JSONScalarType.INTEGER,
                ),
                CITATION_END_FIELD_NAME: ScalarJSONSchema(
                    description="Character offset in the document where the quoted text ends.",
                    json_type=JSONScalarType.INTEGER,
                ),
            },
            required=[
                CITATION_TEXT_FIELD_NAME,
                CITATION_START_FIELD_NAME,
                CITATION_END_FIELD_NAME,
            ],
        )
