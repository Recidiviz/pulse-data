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
"""Collectors that enumerate fully-resolved LLM extractor configs: the
first-order configs authored as `extractor.yaml` files, the entity-resolution
configs generated from them, and the combined "all extractors" list that
production source-table deployment and the view/sandbox loops consume.

These live above `models/llm_extractor_config.py` (which defines the config
object itself) so the combined enumeration can compose the entity-resolution
builders without the model module depending on the entity_resolution package.
"""
from functools import cache
from types import ModuleType

from recidiviz.common.constants.states import StateCode
from recidiviz.documents import config as default_config_module
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_extractor_collection_config_builder import (
    build_entity_resolution_extractor_collection_config,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_extractor_generator import (
    EntityResolutionExtractorGenerator,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    extractor_collections_dir,
    load_llm_extractor_collection_configs,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.models.llm_model_registry import (
    load_llm_model_registry,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_registry import (
    load_full_reference_data_registry,
)
from recidiviz.documents.store.document_collection_config import (
    load_first_order_document_collection_configs,
)

EXTRACTOR_CONFIG_FILENAME = "extractor.yaml"


def get_states_with_extractor_configs(
    config_module: ModuleType | None = None,
) -> set[StateCode]:
    """Returns the set of states that define at least one extractor under
    |config_module| (the production config package by default), derived from the
    `extractor_collections/{collection}/{state}/extractor.yaml` files without
    parsing them.
    """
    module = config_module or default_config_module
    collections_dir = extractor_collections_dir(module)
    if not collections_dir.is_dir():
        raise ValueError(
            f"Extractor collections directory does not exist: [{collections_dir}]."
        )
    return {
        LLMExtractorConfig.state_code_for_yaml_path(extractor_yaml_path)
        for extractor_yaml_path in collections_dir.glob(
            f"*/*/{EXTRACTOR_CONFIG_FILENAME}"
        )
    }


@cache
def load_first_order_llm_extractor_configs(
    config_module: ModuleType | None = None,
) -> dict[StateCode, dict[str, LLMExtractorConfig]]:
    """Returns the resolved first-order extractor configs parsed from every
    `extractor_collections/{collection}/{state}/extractor.yaml` within
    |config_module| (the production config package by default), each bound to its
    collection config, input document collection, and resolved model config, keyed
    by state code and then by collection name.
    """
    module = config_module or default_config_module
    collections_dir = extractor_collections_dir(module)
    model_registry = load_llm_model_registry(module)
    extractor_collections_by_name = load_llm_extractor_collection_configs(module)

    configs_by_state: dict[StateCode, dict[str, LLMExtractorConfig]] = {}
    for state_code in get_states_with_extractor_configs(module):
        document_collections_by_name = load_first_order_document_collection_configs(
            state_code, config_module=module
        )
        reference_data_registries = load_full_reference_data_registry(
            state_code, config_module=module
        )
        state_configs: dict[str, LLMExtractorConfig] = {}
        for extractor_yaml_path in sorted(
            collections_dir.glob(
                f"*/{state_code.value.lower()}/{EXTRACTOR_CONFIG_FILENAME}"
            )
        ):
            config = LLMExtractorConfig.from_yaml(
                extractor_yaml_path,
                extractor_collections_by_name=extractor_collections_by_name,
                model_registry=model_registry,
                document_collections_by_name=document_collections_by_name,
                reference_data_registries_by_data_type=reference_data_registries,
            )
            collection_name = config.extractor_collection.name
            if collection_name in state_configs:
                raise ValueError(
                    f"Found multiple extractor configs for collection "
                    f"[{collection_name}] in state [{state_code.value}]."
                )
            state_configs[collection_name] = config
        configs_by_state[state_code] = state_configs
    return configs_by_state


def get_first_order_llm_extractor_config(
    state_code: StateCode,
    collection_name: str,
    config_module: ModuleType | None = None,
) -> LLMExtractorConfig:
    """Returns the resolved first-order extractor config for |collection_name| in
    |state_code| within |config_module| (the production config package by
    default), raising if no such extractor exists.
    """
    state_configs = load_first_order_llm_extractor_configs(config_module).get(
        state_code, {}
    )
    if collection_name not in state_configs:
        raise ValueError(
            f"No extractor config for collection [{collection_name}] in state "
            f"[{state_code.value}]. Known collections for this state: "
            f"{sorted(state_configs)}."
        )
    return state_configs[collection_name]


def collect_entity_resolution_extractor_configs(
    *,
    first_order_configs: list[LLMExtractorConfig],
    config_module: ModuleType | None = None,
) -> list[tuple[LLMExtractorConfig, LLMExtractorConfig]]:
    """Returns one (parent first-order LLMExtractorConfig, generated ER
    LLMExtractorConfig) pair per (first-order config, declared entity group).
    """
    model_registry = load_llm_model_registry(config_module)
    generator = EntityResolutionExtractorGenerator()
    pairs: list[tuple[LLMExtractorConfig, LLMExtractorConfig]] = []
    for first_order_config in first_order_configs:
        for entity_group in first_order_config.extractor_collection.entity_groups:
            entity_resolution_document_collection = (
                EntityResolutionDocumentCollectionConfig(
                    first_order_config=first_order_config,
                    entity_group=entity_group,
                )
            )
            entity_resolution_collection = build_entity_resolution_extractor_collection_config(
                parent_collection=first_order_config.extractor_collection,
                entity_group=entity_group,
                source_document_collection=first_order_config.input_document_collection,
            )
            entity_resolution_config = generator.generate(
                entity_resolution_collection=entity_resolution_collection,
                entity_resolution_document_collection=entity_resolution_document_collection,
                model_registry=model_registry,
            )
            pairs.append((first_order_config, entity_resolution_config))
    return pairs


def collect_all_extractor_configs_by_state(
    *, config_module: ModuleType | None = None
) -> dict[StateCode, dict[str, LLMExtractorConfig]]:
    """Returns every extractor config — first-order (YAML-defined) plus any generated
    entity-resolution configs — keyed by state code and then by collection name.
    """

    first_order_configs: list[LLMExtractorConfig] = []
    configs_by_state: dict[StateCode, dict[str, LLMExtractorConfig]] = {}
    for (
        state_code,
        configs_by_collection_name,
    ) in load_first_order_llm_extractor_configs(config_module).items():
        first_order_configs.extend(configs_by_collection_name.values())
        configs_by_state[state_code] = dict(configs_by_collection_name)

    for (
        _first_order_config,
        entity_resolution_config,
    ) in collect_entity_resolution_extractor_configs(
        first_order_configs=first_order_configs, config_module=config_module
    ):
        state_configs = configs_by_state[entity_resolution_config.state_code]
        collection_name = entity_resolution_config.extractor_collection.name
        if collection_name in state_configs:
            raise ValueError(
                f"Generated entity-resolution extractor collection name "
                f"[{collection_name}] collides with an existing extractor config "
                f"for state [{entity_resolution_config.state_code.value}]."
            )
        state_configs[collection_name] = entity_resolution_config
    return configs_by_state
