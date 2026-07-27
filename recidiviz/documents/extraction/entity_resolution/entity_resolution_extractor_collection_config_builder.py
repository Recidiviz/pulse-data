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
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    entity_resolution_collection_name,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_output_schema_builder import (
    build_entity_resolution_output_schema,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    EntityGroupConfig,
    LLMExtractorCollectionConfig,
)

# Framework-fixed default model for entity-resolution extractors: a
# thinking-enabled Gemini 2.5 config (see `model_registry.yaml`). Not
# config-overridable for now — no use case yet justifies a per-group or per-state
# override.
ENTITY_RESOLUTION_DEFAULT_MODEL_CONFIG_NAME = "GEMINI_2_5_FLASH_THINKING"


def build_entity_resolution_extractor_collection_config(
    *,
    parent_collection: LLMExtractorCollectionConfig,
    entity_group: EntityGroupConfig,
) -> LLMExtractorCollectionConfig:
    """Returns the ER extractor collection for one (parent collection, entity
    group). State-agnostic: the resolved per-state reference-data entries and any
    model override bind later, when the ER extractor is resolved for a state.
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
        # TODO(OBT-36778): Replace with a fully-formed prompt for ER collections.
        # Hashed into collection_version_id like any template, so version IDs
        # shift when the real prompt lands — harmless pre-production.
        prompt_template="<FAKE PROMPT>",
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
