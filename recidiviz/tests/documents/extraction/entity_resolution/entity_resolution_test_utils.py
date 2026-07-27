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
"""Shared helpers for entity-resolution builder tests: loaders for the fake
first-order collection/extractor and lookups for their entity groups.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
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
