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
"""Tests for document_collection_config_collectors.py.

Covers enumerating the generated entity-resolution composite-document collections
and the combined (first-order + ER) set, exercised against the fake config
module's US_XX collection (which declares an entity group) and guarding that every
real state's combined collections build.
"""
from unittest import TestCase

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config_builder import (
    ENTITY_RESOLUTION_COLLECTION_NAME_SUFFIX,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    collect_entity_resolution_extractor_configs,
)
from recidiviz.documents.store.document_collection_config import (
    load_first_order_document_collection_configs,
)
from recidiviz.documents.store.document_collection_config_collectors import (
    collect_all_document_collection_configs,
    collect_entity_resolution_document_collection_configs,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    FAKE_ER_COLLECTION_NAMES,
    fake_first_order_extractor_config,
)


class CollectEntityResolutionDocumentCollectionConfigsTest(TestCase):
    """Tests for collect_entity_resolution_document_collection_configs."""

    def test_one_per_declared_entity_group(self) -> None:
        er_collections = collect_entity_resolution_document_collection_configs(
            first_order_configs=[fake_first_order_extractor_config()]
        )
        self.assertEqual(FAKE_ER_COLLECTION_NAMES, {c.name for c in er_collections})

    def test_no_declared_entity_groups_returns_empty(self) -> None:
        first_order_config = fake_first_order_extractor_config()
        without_entity_groups = attr.evolve(
            first_order_config,
            extractor_collection=attr.evolve(
                first_order_config.extractor_collection, entity_groups=[]
            ),
        )
        self.assertEqual(
            [],
            collect_entity_resolution_document_collection_configs(
                first_order_configs=[without_entity_groups]
            ),
        )

    def test_matches_er_extractor_input_collection(self) -> None:
        # Each standalone-built ER document collection must equal the one the
        # corresponding ER extractor config binds as its input.
        first_order_configs = [fake_first_order_extractor_config()]
        er_collections_by_name = {
            c.name: c
            for c in collect_entity_resolution_document_collection_configs(
                first_order_configs=first_order_configs
            )
        }
        for (
            _first_order_config,
            er_config,
        ) in collect_entity_resolution_extractor_configs(
            first_order_configs=first_order_configs
        ):
            self.assertEqual(
                er_config.input_document_collection,
                er_collections_by_name[er_config.input_document_collection.name],
            )


class CollectAllDocumentCollectionConfigsTest(TestCase):
    """Tests for collect_all_document_collection_configs."""

    def test_first_order_excludes_entity_resolution(self) -> None:
        first_order = load_first_order_document_collection_configs(
            StateCode.US_XX, fake_config
        )
        self.assertFalse(
            any(
                name.endswith(f"_{ENTITY_RESOLUTION_COLLECTION_NAME_SUFFIX}")
                for name in first_order
            )
        )

    def test_combines_first_order_and_entity_resolution(self) -> None:
        first_order = load_first_order_document_collection_configs(
            StateCode.US_XX, fake_config
        )
        all_configs = collect_all_document_collection_configs(
            StateCode.US_XX, fake_config
        )
        # The first-order subset is preserved unchanged.
        for name, config in first_order.items():
            self.assertEqual(config, all_configs[name])
        # The only additions are the generated ER composite-document collections.
        self.assertEqual(FAKE_ER_COLLECTION_NAMES, set(all_configs) - set(first_order))

    def test_nonexistent_state_returns_empty(self) -> None:
        self.assertEqual(
            {}, collect_all_document_collection_configs(StateCode.US_LL, fake_config)
        )

    def test_load_all_real_configs(self) -> None:
        # Raises if any real state's combined (first-order + ER) document
        # collections fail to build. Does not assert on any state's specifics.
        for state_code in StateCode:
            collect_all_document_collection_configs(state_code)
