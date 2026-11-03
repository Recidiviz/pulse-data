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
"""Tests for document_generation_output_materializer.py."""

import unittest
from unittest.mock import MagicMock, patch

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_collection_config_collectors import (
    get_document_collection_config,
)
from recidiviz.documents.store.document_generation_output_materializer import (
    DocumentGenerationOutputMaterializer,
)
from recidiviz.documents.store.document_store_sandbox_context import (
    DocumentCollectionSandboxLocation,
    DocumentStoreSandboxContext,
)
from recidiviz.tests.documents import fake_config as fake_config_module
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.tests.documents.store.document_store_test_utils import (
    get_fake_first_order_document_collection_config,
)
from recidiviz.utils.types import assert_type

_PROJECT_ID = "recidiviz-testing"
_OUTPUT_ADDRESS = BigQueryAddress(
    dataset_id="some_dataset", table_id="generation_output"
)
_HYDRATOR_PATCH_TARGET = (
    "recidiviz.documents.store.document_generation_output_materializer."
    "EntityResolutionEntrySourceMapHydrator"
)


class DocumentGenerationOutputMaterializerTest(unittest.TestCase):
    """Tests for DocumentGenerationOutputMaterializer"""

    def setUp(self) -> None:
        self.bq_client = MagicMock()
        self.enterContext(patch_fake_entity_resolution_model_config_name())

    def _materializer(
        self,
        config: DocumentCollectionConfig,
        sandbox_context: DocumentStoreSandboxContext | None = None,
    ) -> DocumentGenerationOutputMaterializer:
        return DocumentGenerationOutputMaterializer(
            config=config,
            project_id=_PROJECT_ID,
            big_query_client=self.bq_client,
            sandbox_context=sandbox_context,
        )

    def _er_collection(self) -> EntityResolutionDocumentCollectionConfig:
        return assert_type(
            get_document_collection_config(
                StateCode.US_XX, FAKE_ASSIGNMENT_ER_COLLECTION_NAME, fake_config_module
            ),
            EntityResolutionDocumentCollectionConfig,
        )

    def _write_addresses(self) -> list[BigQueryAddress]:
        return [
            call.kwargs["address"]
            for call in self.bq_client.create_table_from_query.call_args_list
        ]

    def test_first_order_collection_writes_only_the_output_table(self) -> None:
        materializer = self._materializer(
            get_fake_first_order_document_collection_config()
        )

        output_address = materializer.materialize(output_address=_OUTPUT_ADDRESS)

        self.assertEqual(
            _OUTPUT_ADDRESS.to_project_specific_address(_PROJECT_ID), output_address
        )
        self.bq_client.create_table_from_query.assert_called_once()
        write_call = self.bq_client.create_table_from_query.call_args
        self.assertEqual(_OUTPUT_ADDRESS, write_call.kwargs["address"])
        self.assertIn("us_xx_raw_data.fake_input_notes", write_call.kwargs["query"])
        self.assertTrue(write_call.kwargs["overwrite"])
        self.assertFalse(write_call.kwargs["use_query_cache"])

    def test_entity_resolution_collection_hydrates_map_from_the_output_table(
        self,
    ) -> None:
        er_collection = self._er_collection()
        materializer = self._materializer(er_collection)

        materializer.materialize(output_address=_OUTPUT_ADDRESS)

        self.assertEqual(
            [
                _OUTPUT_ADDRESS,
                er_collection.entry_source_map_table_address(
                    sandbox_dataset_prefix=None
                ),
            ],
            self._write_addresses(),
        )
        map_write_call = self.bq_client.create_table_from_query.call_args_list[1]
        self.assertIn(_OUTPUT_ADDRESS.table_id, map_write_call.kwargs["query"])

    def test_sandbox_output_prefix_flows_to_map_hydrator(self) -> None:
        er_collection = self._er_collection()
        first_order_collection = (
            er_collection.first_order_config.input_document_collection
        )
        materializer = self._materializer(
            er_collection,
            sandbox_context=DocumentStoreSandboxContext(
                document_collection_locations={
                    er_collection.name: DocumentCollectionSandboxLocation(
                        output_prefix="sb", diff_read_prefix="sb"
                    ),
                    first_order_collection.name: None,
                },
                extractor_collection_read_prefixes={
                    er_collection.first_order_extractor_collection_name: "sb"
                },
            ),
        )

        with patch(_HYDRATOR_PATCH_TARGET) as mock_hydrator:
            materializer.materialize(output_address=_OUTPUT_ADDRESS)

        self.assertEqual("sb", mock_hydrator.call_args.kwargs["output_sandbox_prefix"])
        mock_hydrator.return_value.run.assert_called_once_with(
            _OUTPUT_ADDRESS.to_project_specific_address(_PROJECT_ID)
        )

    def test_production_run_hydrates_map_with_no_prefix(self) -> None:
        materializer = self._materializer(self._er_collection(), sandbox_context=None)

        with patch(_HYDRATOR_PATCH_TARGET) as mock_hydrator:
            materializer.materialize(output_address=_OUTPUT_ADDRESS)

        self.assertIsNone(mock_hydrator.call_args.kwargs["output_sandbox_prefix"])
