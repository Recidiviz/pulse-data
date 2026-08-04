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
"""Tests for collect_extraction_results_source_table_collections and
collect_golden_eval_results_source_table_collection.
"""

import unittest

from more_itertools import one

from recidiviz.big_query.constants import TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.dataset_config import (
    document_extraction_raw_results_dataset_for_region,
    document_extraction_validated_results_dataset_for_region,
    document_extraction_validation_audit_dataset_for_region,
)
from recidiviz.documents.extraction.eval.golden_eval_results_table import (
    GoldenEvalResultsBQTable,
)
from recidiviz.documents.extraction.llm_extraction_results_tables import (
    ExtractionRawResultsBQTable,
    ExtractionValidatedResultsBQTable,
    ExtractionValidationAuditBQTable,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    load_first_order_llm_extractor_configs,
)
from recidiviz.source_tables.extraction_results_source_table_collection import (
    collect_extraction_results_source_table_collections,
    collect_golden_eval_results_source_table_collection,
)
from recidiviz.source_tables.source_table_config import (
    ExtractionResultsSourceTableLabel,
    SourceTableCollectionUpdateConfig,
    StateSpecificSourceTableLabel,
)
from recidiviz.tests.documents import fake_config

_GOLDEN_EVAL_DATASET_ID = "document_extraction_golden_eval_results"
_GOLDEN_EVAL_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_GOLDEN_EVAL_TABLE_ID = "fake_extractor_collection"


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
            configs=load_first_order_llm_extractor_configs(config_module=fake_config)
        )

        datasets_to_schemas = {
            document_extraction_raw_results_dataset_for_region(
                StateCode.US_XX
            ): ExtractionRawResultsBQTable.schema(),
            document_extraction_validated_results_dataset_for_region(
                StateCode.US_XX
            ): ExtractionValidatedResultsBQTable.schema(),
            document_extraction_validation_audit_dataset_for_region(
                StateCode.US_XX
            ): ExtractionValidationAuditBQTable.schema(),
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


class CollectGoldenEvalResultsSourceTableCollectionTest(unittest.TestCase):
    """Tests for collect_golden_eval_results_source_table_collection."""

    def test_one_table_per_extractor_collection(self) -> None:
        collection = collect_golden_eval_results_source_table_collection(
            config_module=fake_config
        )

        self.assertEqual(_GOLDEN_EVAL_DATASET_ID, collection.dataset_id)
        # Rows cannot be reconstructed without re-calling the model, so the table
        # is append-only and protected against field deletions and recreation.
        self.assertEqual(
            SourceTableCollectionUpdateConfig.protected(), collection.update_config
        )
        self.assertIsNone(collection.default_table_expiration_ms)

        # Expect one table since there is only one collection in the fake config.
        # The table spans every state that runs the extractor — state_code is a
        # column, not part of the address.
        table = one(collection.source_tables)
        self.assertEqual(_GOLDEN_EVAL_TABLE_ID, table.address.table_id)
        self.assertEqual(GoldenEvalResultsBQTable.schema(), table.schema_fields)
        self.assertEqual(
            GoldenEvalResultsBQTable.description(
                collection_name=_GOLDEN_EVAL_COLLECTION_NAME
            ),
            table.description,
        )

    def test_as_sandbox_collection(self) -> None:
        collection = collect_golden_eval_results_source_table_collection(
            config_module=fake_config
        ).as_sandbox_collection("my_prefix")

        self.assertEqual(f"my_prefix_{_GOLDEN_EVAL_DATASET_ID}", collection.dataset_id)
        table = one(collection.source_tables)
        self.assertEqual(
            GoldenEvalResultsBQTable.address(
                collection_name=_GOLDEN_EVAL_COLLECTION_NAME,
                sandbox_prefix="my_prefix",
            ),
            table.address,
        )
        self.assertEqual(
            TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS, collection.table_expiration_ms
        )
