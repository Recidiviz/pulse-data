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
collection/extractor, lookups for their entity groups, the ER composite-document
collection and extractor configs generated from them, and builders for the fake
composite-document generation-output rows the ER pass materializes.
"""
import datetime
from contextlib import AbstractContextManager
from typing import Any
from unittest.mock import patch

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.entity_resolution.entity_resolution_composite_document_query_builder import (
    ENTRY_NUM_FIELD_NAME,
    ENTRY_SOURCE_MAP_COLUMN_NAME,
    SOURCE_ARRAY_INDEX_FIELD_NAME,
    SOURCE_DOCUMENT_CONTENTS_ID_FIELD_NAME,
    SOURCE_DOCUMENT_UPDATE_DATETIME_FIELD_NAME,
)
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
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
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


def fake_entity_resolution_extractor_config(group_name: str) -> LLMExtractorConfig:
    """Returns the generated ER extractor config for the fake US_XX first-order
    collection's |group_name| entity group. Callers must have entered
    patch_fake_entity_resolution_model_config_name.
    """
    entity_group = get_entity_group_by_name(fake_first_order_collection(), group_name)
    for _, entity_resolution_config in fake_entity_resolution_extractor_config_pairs():
        if entity_resolution_config.entity_group == entity_group:
            return entity_resolution_config
    raise ValueError(
        f"Found no generated entity-resolution config for entity group "
        f"[{group_name}]."
    )


def build_fake_entry_source_map_entry(
    *,
    entry_num: int,
    source_document_contents_id: str,
    source_document_update_datetime: datetime.datetime,
    source_array_index: int | None,
) -> dict[str, Any]:
    """Returns one element of a composite generation-output row's nested
    `entry_source_map` array, mapping a numbered composite entry back to the
    first-order mention occurrence it was rendered from. |source_array_index| is
    None for a top-level entity group.
    """
    return {
        ENTRY_NUM_FIELD_NAME: entry_num,
        SOURCE_DOCUMENT_CONTENTS_ID_FIELD_NAME: source_document_contents_id,
        SOURCE_DOCUMENT_UPDATE_DATETIME_FIELD_NAME: (
            source_document_update_datetime.isoformat()
        ),
        SOURCE_ARRAY_INDEX_FIELD_NAME: source_array_index,
    }


def build_fake_composite_generation_output_row(
    *,
    root_entity_id_column: str,
    root_entity_id: int,
    document_contents_id: str,
    document_update_datetime: datetime.datetime,
    entries: list[dict[str, Any]],
) -> dict[str, Any]:
    """Returns one row of an ER collection's composite-document generation output:
    the aggregated text a root entity's mentions render to, plus the
    `entry_source_map` linking each numbered entry back to its first-order
    occurrence. |entries| are built by build_fake_entry_source_map_entry.

    The row shape mirrors build_bq_document_generation_output_schema for an ER
    collection; the composite's own text is a fixed placeholder nothing under test
    reads.
    """
    return {
        root_entity_id_column: root_entity_id,
        DOCUMENT_CONTENTS_ID_COLUMN_NAME: document_contents_id,
        DOCUMENT_TEXT_COLUMN_NAME: (
            f"composite for {root_entity_id_column} {root_entity_id} "
            f"({document_contents_id})"
        ),
        DOCUMENT_UPDATE_DATETIME_COLUMN_NAME: document_update_datetime.isoformat(),
        ENTRY_SOURCE_MAP_COLUMN_NAME: entries,
    }
