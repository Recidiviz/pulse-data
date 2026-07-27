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
"""Collectors that enumerate DocumentCollectionConfigs."""
from types import ModuleType

from recidiviz.common.constants.states import StateCode
from recidiviz.documents import config as default_config_module
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config_builder import (
    EntityResolutionDocumentCollectionConfigBuilder,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    load_first_order_llm_extractor_configs,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    load_first_order_document_collection_configs,
)


def collect_entity_resolution_document_collection_configs(
    *, first_order_configs: list[LLMExtractorConfig]
) -> list[DocumentCollectionConfig]:
    """Returns one generated entity-resolution composite-document collection per
    (first-order extractor config, declared entity group) — the same collections
    the ER extractor configs bind as their input document collections.
    """
    collections: list[DocumentCollectionConfig] = []
    for first_order_config in first_order_configs:
        for entity_group in first_order_config.extractor_collection.entity_groups:
            collections.append(
                EntityResolutionDocumentCollectionConfigBuilder(
                    first_order_config=first_order_config,
                    entity_group=entity_group,
                ).build()
            )
    return collections


def collect_all_document_collection_configs(
    state_code: StateCode,
    config_module: ModuleType | None = None,
) -> dict[str, DocumentCollectionConfig]:
    """Returns a map of collection name to its configuration for every document
    collection in the given state — the first-order (YAML-authored) collections
    plus the generated entity-resolution composite-document collections.
    """
    module = config_module or default_config_module
    document_configs = dict(
        load_first_order_document_collection_configs(state_code, module)
    )

    all_first_order_extractor_configs = load_first_order_llm_extractor_configs(module)
    if state_code not in all_first_order_extractor_configs:
        return document_configs

    first_order_extractor_configs = list(
        all_first_order_extractor_configs[state_code].values()
    )
    for er_collection in collect_entity_resolution_document_collection_configs(
        first_order_configs=first_order_extractor_configs
    ):
        if er_collection.name in document_configs:
            raise ValueError(
                f"Found multiple document collections with name [{er_collection.name}]."
            )
        document_configs[er_collection.name] = er_collection
    return document_configs


def get_document_collection_config(
    state_code: StateCode,
    collection_name: str,
    config_module: ModuleType | None = None,
) -> DocumentCollectionConfig:
    """Returns the DocumentCollectionConfig for |collection_name| in |state_code|,
    across both first-order and entity-resolution composite-document collections.
    """
    configs = collect_all_document_collection_configs(state_code, config_module)
    if collection_name not in configs:
        raise ValueError(
            f"No document collection [{collection_name}] in state "
            f"[{state_code.value}]. Known collections: {sorted(configs)}."
        )
    return configs[collection_name]
