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
"""Synthesizes the entity-resolution clustering output schema for one entity
group.

The ER extractor runs a per-document LLM pass over a composite document (one per
root entity, holding all of that entity's first-order mentions numbered as
`entry_nums`) and clusters those entries into canonical entities. This module
builds the output schema that pass is constrained to, expressed in the *same*
typed `LLMRequestOutputSchema` model first-order schemas use — so both existing
generators (the JSON schema sent to the model and the `{output_instructions}`
prose) derive from it through their unchanged `generate(output_schema)` entry
points.

The ER schema differs from a first-order schema only in shape, not in machinery:

  - it declares no `is_relevant` (every composite document is relevant by
    construction), so `LLMRequestOutputSchema.relevance_criteria` is `None`;
  - every field is STRUCTURAL — a value the model assigns, with no confidence /
    citation companion metadata — so no field carries an `InferredFieldConfig`;
  - it is a single `entities` ARRAY_OF_STRUCT whose sub-fields are a STRUCTURAL
    `entity_id` (sequential integer), the parent schema's `entity_fields` copied
    verbatim but flipped to STRUCTURAL, and a STRUCTURAL `entry_nums`
    integer-array. Nesting `entry_nums` under each entity — rather than a flat
    entry->entity assignment list — is what lets the ER validator check an exact
    partition of the input entry set E = {1..N}.

Every entity field key is always present on each entity, but its value is
nullable unless the field was required in the first-order collection: a
first-order-required field is guaranteed a value on every mention, so the
resolved entity has one too, while an optional field may have been left unfilled
by every mention an entity clusters (e.g. an assignment whose only mention left
`assignment_type` blank), in which case the model returns an explicit `null`
rather than inventing a value. `entity_id` and `entry_nums` are always present
and non-null.

Because the copied entity-field definitions are literally part of the synthesized
schema, the ER version-ID cascade falls out of the normal schema-hash machinery:
a change to a parent entity field's type or description reshapes this schema and
therefore its hash, with no special hashing rule.
"""
import attr

from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    EntityGroupConfig,
)
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfIntegerLLMRequestOutputSchemaField,
    ArrayOfStructLLMRequestOutputSchemaField,
    LLMOutputFieldType,
    PrimitiveScalarLLMRequestOutputSchemaField,
    ScalarValuedLLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    ENTITIES_FIELD_NAME,
    ENTITY_ID_FIELD_NAME,
    ENTRY_NUMS_FIELD_NAME,
)

# Framework-fixed descriptions of the synthesized STRUCTURAL fields. These are
# baked into both the generated JSON schema and the prompt's output instructions,
# so the model knows what each field means.
ENTITY_ID_DESCRIPTION = "Index of this entity in the entities array (using 1-indexing)."
ENTRY_NUMS_DESCRIPTION = (
    "The entry numbers ([Entry N]) that refer to this entity. Across all "
    "entities, assign every entry in the document to exactly one entity, and "
    "never use a number with no matching entry in the document."
)


def _entities_field_description(entity_group: EntityGroupConfig) -> str:
    descriptor = entity_group.entity_descriptor
    description = (
        f'Each element is a distinct real-world {descriptor.singular} (an "entity"). If '
        f"two would have identical entity-field values, they are the same "
        f"{descriptor.singular} — emit one element, not two."
    )
    # Group-level clustering heuristics that no single entity_field description can
    # express (e.g. "cluster all self-employment into one entity") ride along here,
    # so they reach the model through the generated `{output_instructions}`.
    if entity_group.grouping_instructions:
        description += f" {entity_group.grouping_instructions.strip()}"
    return description


def _to_structural_entity_field(
    field: ScalarValuedLLMRequestOutputSchemaField,
) -> ScalarValuedLLMRequestOutputSchemaField:
    """Returns |field| copied verbatim (type and description) but flipped to a
    STRUCTURAL, always-present entity field: dropping the INFERRED
    companion-metadata config (and with it any semantic-consistency constraints,
    which only an INFERRED field carries), so the model emits a single canonical
    value per entity rather than the first-order extracted-value wrapper. The key
    is always required (present), but its value is nullable iff the field was
    optional in the first-order collection — so an entity whose mentions never
    filled it returns an explicit `null` instead of a forced value.
    """
    return attr.evolve(
        field, inferred_field_config=None, required=True, nullable=not field.required
    )


def entity_field_schema_fields(
    entity_group: EntityGroupConfig,
) -> list[ScalarValuedLLMRequestOutputSchemaField]:
    """Returns the schema fields for |entity_group|'s entity_fields as a resolved
    entity carries them: each first-order field copied verbatim but flipped to
    STRUCTURAL, so the model emits one canonical value per entity instead of the
    first-order extracted-value wrapper.

    These are the fields that appear on every view built over the entity group's
    clustering output, so build_entity_resolution_output_schema and the views that read
    its results both go through here.
    """
    return [_to_structural_entity_field(field) for field in entity_group.entity_fields]


def build_entity_resolution_output_schema(
    *, entity_group: EntityGroupConfig
) -> LLMRequestOutputSchema:
    """Returns the clustering output schema for |entity_group|: a single
    `entities` ARRAY_OF_STRUCT field whose sub-fields are a STRUCTURAL `entity_id`
    (INTEGER, sequential), the group's `entity_fields` (their types, descriptions,
    and required-ness verbatim, flipped to STRUCTURAL), and a STRUCTURAL
    `entry_nums` integer-array (minItems 1). The schema declares no `is_relevant`
    and every field is STRUCTURAL, so the unchanged generators derive both the
    response JSON schema and the `{output_instructions}` from it.

    `entity_group.entity_fields` are the resolved parent output-schema field
    objects (resolved in `EntityGroupConfig.from_yaml_dict`), so copying them into
    the synthesized schema is what makes the ER version-ID cascade fall out of the
    normal schema-hash machinery — no reference to the parent schema is needed.
    """
    entity_id_field = PrimitiveScalarLLMRequestOutputSchemaField(
        name=ENTITY_ID_FIELD_NAME,
        description=ENTITY_ID_DESCRIPTION,
        required=True,
        inferred_field_config=None,
        scalar_type=LLMOutputFieldType.INTEGER,
    )
    # Each entity field key is always present, but its value is nullable unless
    # it was required in the first-order collection; forcing a value on a field no
    # mention filled (the [not provided] case) would make the model invent one.
    # TODO(OBT-32174): The ER result validator must reject an entity with *no*
    #  non-null entity_field value — an entity identified by none of its fields is
    #  meaningless. This schema can't express "at least one of these fields is
    #  non-null", so it is a validation-time check over the parsed output.
    entity_fields = entity_field_schema_fields(entity_group)
    entry_nums_field = ArrayOfIntegerLLMRequestOutputSchemaField(
        name=ENTRY_NUMS_FIELD_NAME,
        description=ENTRY_NUMS_DESCRIPTION,
        required=True,
        inferred_field_config=None,
        min_items=1,
    )
    entities_field = ArrayOfStructLLMRequestOutputSchemaField(
        name=ENTITIES_FIELD_NAME,
        description=_entities_field_description(entity_group),
        required=True,
        inferred_field_config=None,
        fields=[entity_id_field, *entity_fields, entry_nums_field],
        # An entity is identified by its canonical entity-field values (the same
        # values the duplicate-entity check forbids two entities from sharing),
        # so those are what pair expected vs. actual entities during golden eval.
        primary_keys=[field.name for field in entity_fields],
        min_items=1,
    )
    # Every generated description names the entity type through the group's
    # descriptor, never through `entity_group.name` — the name is a config
    # identifier, while the descriptor exists to name the thing in prose.
    descriptor = entity_group.entity_descriptor
    return LLMRequestOutputSchema(
        full_batch_description=(
            f"The resolved {descriptor.plural} for one root entity's composite "
            f"document."
        ),
        result_level_description=(
            f"The set of distinct {descriptor.plural} resolved from a single "
            f"composite document's numbered entries."
        ),
        relevance_criteria=None,
        user_defined_fields=[entities_field],
    )
