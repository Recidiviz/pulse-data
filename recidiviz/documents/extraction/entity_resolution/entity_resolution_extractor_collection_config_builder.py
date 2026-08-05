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
"""Synthesizes the state-agnostic `LLMExtractorCollectionConfig` for an
entity-resolution extractor — one per (first-order collection, entity group),
built entirely from collection-level inputs (there is no hand-authored ER
`collection.yaml`).
"""
from recidiviz.common.descriptor import Descriptor
from recidiviz.documents.extraction.entity_resolution.entity_resolution_collection_names import (
    entity_resolution_collection_name,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_output_schema_builder import (
    build_entity_resolution_output_schema,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    EntityGroupConfig,
    LLMExtractorCollectionConfig,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)

# Framework-fixed default model for entity-resolution extractors: a
# thinking-enabled Gemini 3.5 config (see `model_registry.yaml`). Not
# config-overridable for now — no use case yet justifies a per-group or per-state
# override.
ENTITY_RESOLUTION_DEFAULT_MODEL_CONFIG_NAME = "GEMINI_3_5_FLASH_MEDIUM_THINKING"


def build_entity_resolution_prompt_template(
    *,
    entity_descriptor: Descriptor,
    source_document_descriptor: Descriptor,
    has_reference_data: bool,
) -> str:
    """Returns entity resolution prompt template for an entity group whose
    entities are described by |entity_descriptor|, and were originally extracted from
    documents described by |source_document_descriptor|.

    When |has_reference_data| is True (the collection declares reference data), a
    preamble frames the reused `{reference_data}` block — the parent extractor's
    entries, rendered for ER as a flat name+alias dictionary — as background from the
    original extraction and directs the model to use it for clustering.

    The clustering *task* semantics — sequential `entity_id`, the exact-partition
    rule over `entry_nums`, "two entities may not share all field values", and any
    per-group `grouping_instructions` — live in the synthesized output schema's
    field descriptions and reach the model through `{output_instructions}`, so this
    template deliberately does not restate them.
    """
    reference_data_preamble = (
        f"The reference data below was provided to the original extraction step that "
        f"produced these entries. Use it to interpret the "
        f"{source_document_descriptor.plural} and recognize when differently-written "
        f"entries refer to the same {entity_descriptor.singular}. If "
        f"{entity_descriptor.indefinite} matches one of the entries listed below, use "
        f"the spelling listed below in the canonical entry.\n\n"
        if has_reference_data
        else ""
    )
    return f"""
You are an expert analyst resolving real-world {entity_descriptor.plural} from \
structured records.

You are given a composite document that collects every mention of \
{entity_descriptor.indefinite} drawn from a set of related \
{source_document_descriptor.plural}. The composite document is a chronological list of \
numbered entries, grouped under the {source_document_descriptor.singular} each came \
from: every {source_document_descriptor.singular} shows its date and text once, \
followed by the entries drawn from it. Each entry is marked \
"[Entry N]" and lists the field values extracted for that specific mention of \
{entity_descriptor.indefinite}. The same real-world \
{entity_descriptor.singular} is often mentioned in \
several entries, described differently each time.

Cluster the entries by the real-world {entity_descriptor.singular} they refer to, then \
emit one record per {entity_descriptor.singular}, exactly as described below.

{{output_instructions}}

{reference_data_preamble}{{reference_data}}

When setting an entity's field values, use the most complete, specific form the entries \
support, and never introduce a value that no entry states.
"""


def build_entity_resolution_extractor_collection_config(
    *,
    parent_collection: LLMExtractorCollectionConfig,
    entity_group: EntityGroupConfig,
    source_document_collection: DocumentCollectionConfig,
) -> LLMExtractorCollectionConfig:
    """Returns the ER extractor collection for one (parent collection, entity
    group). State-agnostic: the resolved per-state reference-data entries and any
    model override bind later, when the ER extractor is resolved for a state.

    |source_document_collection| is the first-order document collection whose
    mentions the composite documents are built from.
    """
    if entity_group not in parent_collection.entity_groups:
        raise ValueError(
            f"Entity group [{entity_group.name}] is not declared on collection "
            f"[{parent_collection.name}]."
        )
    return LLMExtractorCollectionConfig(
        # Share the composite-document collection's name: a state ER extractor
        # binds that collection as its input and this one as its extractor
        # collection, so they name the same (collection, group).
        name=entity_resolution_collection_name(
            first_order_extractor_collection_name=parent_collection.name,
            entity_group_name=entity_group.name,
        ),
        description=(
            f"Auto-generated entity-resolution extractor collection for the "
            f"[{entity_group.name}] entity group of the [{parent_collection.name}] "
            f"collection. Clusters that group's first-order mentions into canonical "
            f"entities, emitting one resolved record per entity."
        ),
        # The schema has no is_relevant field: every composite document is relevant by
        # construction.
        relevance_criteria=None,
        prompt_template=build_entity_resolution_prompt_template(
            entity_descriptor=entity_group.entity_descriptor,
            source_document_descriptor=source_document_collection.document_descriptor,
            has_reference_data=bool(
                parent_collection.reference_data_config.per_type_configs
            ),
        ),
        default_model_config_name=ENTITY_RESOLUTION_DEFAULT_MODEL_CONFIG_NAME,
        # No default confidence level - this schema's fields are all
        # STRUCTURAL, carrying no confidence metadata.
        minimum_confidence_level=None,
        output_schema=build_entity_resolution_output_schema(entity_group=entity_group),
        # The parent's state-agnostic reference-data render config; the org
        # aliases in it drive clustering once a state's entries are bound.
        reference_data_config=parent_collection.reference_data_config,
        # ER is terminal — it declares no entity groups that would require further
        # resolution.
        entity_groups=[],
    )
