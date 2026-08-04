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
"""Shared helpers for entity-resolution tests: loaders for the fake first-order
collection/extractor, lookups for their entity groups, and the ER composite-document
collection and extractor configs generated from them.
"""
from contextlib import AbstractContextManager
from unittest.mock import patch

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    collect_entity_resolution_extractor_configs,
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    EntityGroupConfig,
    LLMExtractorCollectionConfig,
    get_llm_extractor_collection_config,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.tests.documents import fake_config

FAKE_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"

# The ER composite-document collections generated from the fake first-order collection's
# `location`, `assignment` and `pay_rate` entity groups. The latter two are sourced from
# the same array, so they also cover multiple independent groups enriching one view.
FAKE_LOCATION_ER_COLLECTION_NAME = (
    "FAKE_EXTRACTOR_COLLECTION_LOCATION_ENTITY_RESOLUTION"
)
FAKE_ASSIGNMENT_ER_COLLECTION_NAME = (
    "FAKE_EXTRACTOR_COLLECTION_ASSIGNMENT_ENTITY_RESOLUTION"
)
FAKE_PAY_RATE_ER_COLLECTION_NAME = (
    "FAKE_EXTRACTOR_COLLECTION_PAY_RATE_ENTITY_RESOLUTION"
)
FAKE_ER_COLLECTION_NAMES = {
    FAKE_LOCATION_ER_COLLECTION_NAME,
    FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
    FAKE_PAY_RATE_ER_COLLECTION_NAME,
}

# The framework-fixed ENTITY_RESOLUTION_DEFAULT_MODEL_CONFIG_NAME only exists in the
# production model registry. Tests that generate real ER configs against the fake
# config module's fake registry patch the builder's default model config name to
# this fake-registry config instead.
FAKE_ENTITY_RESOLUTION_MODEL_CONFIG_NAME = "ACME_LARGE_FIXED_THINKING"
_ENTITY_RESOLUTION_MODEL_CONFIG_NAME_PATCH_TARGET = (
    "recidiviz.documents.extraction.entity_resolution."
    "entity_resolution_extractor_collection_config_builder."
    "ENTITY_RESOLUTION_DEFAULT_MODEL_CONFIG_NAME"
)


def patch_fake_entity_resolution_model_config_name() -> AbstractContextManager[str]:
    """Returns a patch that points the ER extractor collection builder's default
    model config name at a config that exists in the fake registry. Enter it (e.g.
    via self.enterContext(...)) around any code that generates ER configs from the
    fake config module.
    """
    return patch(
        _ENTITY_RESOLUTION_MODEL_CONFIG_NAME_PATCH_TARGET,
        FAKE_ENTITY_RESOLUTION_MODEL_CONFIG_NAME,
    )


def fake_first_order_collection() -> LLMExtractorCollectionConfig:
    """Returns the fake first-order extractor collection config."""
    return get_llm_extractor_collection_config(
        FAKE_COLLECTION_NAME, config_module=fake_config
    )


def fake_first_order_extractor_config() -> LLMExtractorConfig:
    """Returns the fake first-order extractor config for US_XX."""
    return get_first_order_llm_extractor_config(
        StateCode.US_XX, FAKE_COLLECTION_NAME, config_module=fake_config
    )


def get_entity_group_by_name(
    collection: LLMExtractorCollectionConfig, name: str
) -> EntityGroupConfig:
    """Returns the entity group named |name| declared on |collection|."""
    return {group.name: group for group in collection.entity_groups}[name]


def fake_entity_resolution_document_collection_config(
    group_name: str,
) -> EntityResolutionDocumentCollectionConfig:
    """Returns the ER composite-document collection config generated from the fake
    US_XX first-order extractor's |group_name| entity group.
    """
    first_order_config = fake_first_order_extractor_config()
    return EntityResolutionDocumentCollectionConfig(
        first_order_config=first_order_config,
        entity_group=get_entity_group_by_name(
            first_order_config.extractor_collection, group_name
        ),
    )


def fake_entity_resolution_extractor_config_pairs() -> (
    list[tuple[LLMExtractorConfig, LLMExtractorConfig]]
):
    """Returns one (parent first-order config, generated ER config) pair per entity
    group the fake US_XX first-order collection declares. Callers must have entered
    patch_fake_entity_resolution_model_config_name.
    """
    return collect_entity_resolution_extractor_configs(
        first_order_configs=[fake_first_order_extractor_config()],
        config_module=fake_config,
    )
