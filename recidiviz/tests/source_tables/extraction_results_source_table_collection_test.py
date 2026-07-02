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
"""Tests for collect_extraction_results_source_table_collections."""

import unittest

from more_itertools import one

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.dataset_config import (
    document_extraction_raw_results_dataset_for_region,
    document_extraction_validated_results_dataset_for_region,
    document_extraction_validation_audit_dataset_for_region,
)
from recidiviz.documents.extraction.extraction_results_columns import (
    build_raw_results_schema,
    build_validated_results_schema,
    build_validation_audit_schema,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    load_llm_extractor_configs,
)
from recidiviz.source_tables.extraction_results_source_table_collection import (
    collect_extraction_results_source_table_collections,
)
from recidiviz.source_tables.source_table_config import (
    ExtractionResultsSourceTableLabel,
    SourceTableCollectionUpdateConfig,
    StateSpecificSourceTableLabel,
)
from recidiviz.tests.documents import fake_config


class CollectExtractionResultsSourceTableCollectionsTest(unittest.TestCase):
    """Tests for collect_extraction_results_source_table_collections."""

    def test_empty_configs(self) -> None:
        self.assertEqual(
            [], collect_extraction_results_source_table_collections(configs={})
        )
        self.assertEqual(
            [],
            collect_extraction_results_source_table_collections(
                configs={StateCode.US_XX: {}}
            ),
        )

    def test_collections_per_state(self) -> None:
        collections = collect_extraction_results_source_table_collections(
            configs=load_llm_extractor_configs(config_module=fake_config)
        )

        datasets_to_schemas = {
            document_extraction_raw_results_dataset_for_region(
                StateCode.US_XX
            ): build_raw_results_schema(),
            document_extraction_validated_results_dataset_for_region(
                StateCode.US_XX
            ): build_validated_results_schema(),
            document_extraction_validation_audit_dataset_for_region(
                StateCode.US_XX
            ): build_validation_audit_schema(),
        }
        self.assertEqual(
            set(datasets_to_schemas),
            {collection.dataset_id for collection in collections},
        )

        for collection in collections:
            self.assertEqual(
                SourceTableCollectionUpdateConfig.protected(),
                collection.update_config,
            )
            self.assertCountEqual(
                [
                    StateSpecificSourceTableLabel(state_code=StateCode.US_XX),
                    ExtractionResultsSourceTableLabel(state_code=StateCode.US_XX),
                ],
                collection.labels,
            )

            # Expect one table since there is only one collection in the fake config
            table = one(collection.source_tables)
            self.assertEqual("fake_extractor_collection", table.address.table_id)
            self.assertEqual(
                datasets_to_schemas[collection.dataset_id], table.schema_fields
            )
