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
"""Tests for new_document_discovery.py."""

import contextlib
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.store.document_collection_config_collectors import (
    get_document_collection_config,
)
from recidiviz.documents.store.document_store_sandbox_context import (
    DocumentCollectionSandboxLocation,
    DocumentStoreSandboxContext,
)
from recidiviz.documents.store.new_document_discovery import NewDocumentDiscoverer
from recidiviz.tests.documents import fake_config as fake_config_module
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.tests.documents.store.document_store_test_utils import (
    get_fake_first_order_document_collection_config,
)
from recidiviz.utils.types import assert_type


class TestNewDocumentDiscovery(unittest.TestCase):
    """Tests for the NewDocumentDiscoverer class"""

    def setUp(self) -> None:
        self.bq_client = MagicMock()
        self.collection_name = get_fake_first_order_document_collection_config().name

        self.get_config_patcher = patch(
            "recidiviz.documents.store.new_document_discovery.get_document_collection_config",
            side_effect=lambda state_code, name: get_document_collection_config(
                state_code, name, config_module=fake_config_module
            ),
        )
        self.get_config_patcher.start()

        self.discovery = NewDocumentDiscoverer(
            state_code=StateCode.US_XX,
            collection_name=self.collection_name,
            project_id="recidiviz-testing",
            big_query_client=self.bq_client,
            run_id="test_run_id",
        )

    def tearDown(self) -> None:
        self.get_config_patcher.stop()

    def _mock_temp_table_row_counts(
        self,
        *,
        new_document_contents_rows: int,
        document_metadata_updates_rows: int,
    ) -> None:
        def create_table_side_effect(
            address: ProjectSpecificBigQueryAddress, **_kwargs: object
        ) -> MagicMock:
            mock = MagicMock()
            if "temp_new_document_contents" in address.table_id:
                mock.total_rows = new_document_contents_rows
            elif "temp_document_metadata_updates" in address.table_id:
                mock.total_rows = document_metadata_updates_rows
            elif "temp_document_generation_output" in address.table_id:
                pass
            else:
                raise ValueError(f"Unexpected table address: {address.to_str()}")
            return mock

        self.bq_client.create_table_from_query.side_effect = create_table_side_effect

    def test_no_metadata_updates_returns_none(self) -> None:
        mock_row_iterator = MagicMock()
        mock_row_iterator.total_rows = 0
        self.bq_client.create_table_from_query.return_value = mock_row_iterator

        result = self.discovery.run()

        self.assertIsNone(result)
        # Only the generation-output and metadata-updates temp tables are
        # written; the new-documents step is skipped when there are no
        # metadata updates.
        self.assertEqual(self.bq_client.create_table_from_query.call_count, 2)
        self.bq_client.run_query_async.assert_not_called()

    def test_writes_both_temp_tables_when_updates_exist(self) -> None:
        self._mock_temp_table_row_counts(
            new_document_contents_rows=6,
            document_metadata_updates_rows=10,
        )

        result = self.discovery.run()

        assert result is not None
        self.assertEqual(result.collection_name, self.collection_name)
        self.assertEqual(result.num_document_metadata_updates_rows, 10)
        self.assertEqual(result.num_new_document_contents_rows, 6)
        self.assertEqual(self.bq_client.create_table_from_query.call_count, 3)

    def test_metadata_updates_without_new_documents(self) -> None:
        self._mock_temp_table_row_counts(
            new_document_contents_rows=0,
            document_metadata_updates_rows=4,
        )

        result = self.discovery.run()

        assert result is not None
        self.assertEqual(result.num_document_metadata_updates_rows, 4)
        self.assertEqual(result.num_new_document_contents_rows, 0)

    @property
    def _generation_output_address(self) -> BigQueryAddress:
        return get_document_collection_config(
            StateCode.US_XX, self.collection_name, config_module=fake_config_module
        ).temp_document_generation_output_table_address("test_run_id")

    def test_materializes_generation_output_before_diff(self) -> None:
        self._mock_temp_table_row_counts(
            new_document_contents_rows=1,
            document_metadata_updates_rows=1,
        )

        self.discovery.run()

        (
            materialize_call,
            diff_call,
            _new_documents_call,
        ) = self.bq_client.create_table_from_query.call_args_list
        # The first write materializes the generation query's output to the
        # generation-output temp table.
        self.assertEqual(
            self._generation_output_address, materialize_call.kwargs["address"]
        )
        self.assertIn(
            "us_xx_raw_data.fake_input_notes", materialize_call.kwargs["query"]
        )
        # The diff reads the materialized snapshot, not the live generation
        # query.
        self.assertIn(
            self._generation_output_address.table_id, diff_call.kwargs["query"]
        )
        self.assertNotIn("us_xx_raw_data.fake_input_notes", diff_call.kwargs["query"])

    def test_deletes_generation_output_table_when_no_updates(self) -> None:
        mock_row_iterator = MagicMock()
        mock_row_iterator.total_rows = 0
        self.bq_client.create_table_from_query.return_value = mock_row_iterator

        self.discovery.run()

        self.bq_client.delete_table.assert_called_once_with(
            self._generation_output_address, not_found_ok=True
        )

    def test_deletes_generation_output_table_after_discovery(self) -> None:
        self._mock_temp_table_row_counts(
            new_document_contents_rows=1,
            document_metadata_updates_rows=1,
        )

        self.discovery.run()

        self.bq_client.delete_table.assert_called_once_with(
            self._generation_output_address, not_found_ok=True
        )

    def test_generation_output_table_survives_diff_failure(self) -> None:
        def create_table_side_effect(
            address: ProjectSpecificBigQueryAddress, **_kwargs: object
        ) -> MagicMock:
            if "temp_document_metadata_updates" in address.table_id:
                raise ValueError("diff query failed")
            return MagicMock()

        self.bq_client.create_table_from_query.side_effect = create_table_side_effect

        with self.assertRaisesRegex(ValueError, r"^diff query failed$"):
            self.discovery.run()

        # The generation-output snapshot deliberately survives a failed run for
        # debugging; the temp dataset's table expiration reaps it.
        self.bq_client.delete_table.assert_not_called()


class TestNewDocumentDiscoverySandboxScoping(unittest.TestCase):
    """Tests where discovery reads and writes each table across collection types and run
    scenarios, and how the entry→source map hydration is wired in for an ER collection."""

    def setUp(self) -> None:
        self.bq_client = MagicMock()
        self.exit_stack = contextlib.ExitStack()
        self.addCleanup(self.exit_stack.close)
        self.exit_stack.enter_context(patch_fake_entity_resolution_model_config_name())
        self.get_config_patcher = patch(
            "recidiviz.documents.store.new_document_discovery.get_document_collection_config",
            side_effect=lambda state_code, name: get_document_collection_config(
                state_code, name, config_module=fake_config_module
            ),
        )
        self.get_config_patcher.start()

    def tearDown(self) -> None:
        self.get_config_patcher.stop()

    def _run_discovery(
        self,
        collection_name: str,
        sandbox_context: DocumentStoreSandboxContext | None = None,
    ) -> None:
        NewDocumentDiscoverer(
            state_code=StateCode.US_XX,
            collection_name=collection_name,
            project_id="recidiviz-testing",
            big_query_client=self.bq_client,
            run_id="test_run_id",
            sandbox_context=sandbox_context,
        ).run()

    def _first_order_sandbox_context(
        self, collection_name: str, *, diff_read_prefix: str | None
    ) -> DocumentStoreSandboxContext:
        """Builds a context for a first-order collection whose documents this run writes
        to the sandbox prefix. |diff_read_prefix| is None when the collection already
        exists in production and discovery diffs against it, or the sandbox prefix when
        the collection is brand-new and discovery diffs against the empty sandbox."""
        return DocumentStoreSandboxContext(
            document_collection_locations={
                collection_name: DocumentCollectionSandboxLocation(
                    output_prefix="sb", diff_read_prefix=diff_read_prefix
                )
            },
            extractor_collection_read_prefixes={},
        )

    def _entity_resolution_sandbox_context(
        self, *, first_order_source_in_sandbox: bool
    ) -> DocumentStoreSandboxContext:
        """Builds a context for the fake ER collection. The composite collection always
        writes and diffs against its own sandbox copy; its first-order source contents
        are read from the sandbox when |first_order_source_in_sandbox|, else from
        production."""
        collection = assert_type(
            get_document_collection_config(
                StateCode.US_XX, FAKE_ASSIGNMENT_ER_COLLECTION_NAME, fake_config_module
            ),
            EntityResolutionDocumentCollectionConfig,
        )
        first_order_collection = collection.first_order_config.input_document_collection
        return DocumentStoreSandboxContext(
            document_collection_locations={
                collection.name: DocumentCollectionSandboxLocation(
                    output_prefix="sb", diff_read_prefix="sb"
                ),
                first_order_collection.name: DocumentCollectionSandboxLocation(
                    output_prefix="sb" if first_order_source_in_sandbox else None,
                    diff_read_prefix=None,
                ),
            },
            extractor_collection_read_prefixes={
                collection.first_order_extractor_collection_name: "sb"
            },
        )

    def _write_table_ids(self) -> list[str]:
        return [
            call.kwargs["address"].table_id
            for call in self.bq_client.create_table_from_query.call_args_list
        ]

    def test_er_collection_hydrates_map_between_materialize_and_diff(self) -> None:
        mock_row_iterator = MagicMock()
        mock_row_iterator.total_rows = 0
        self.bq_client.create_table_from_query.return_value = mock_row_iterator

        self._run_discovery(FAKE_ASSIGNMENT_ER_COLLECTION_NAME)

        er_collection = assert_type(
            get_document_collection_config(
                StateCode.US_XX, FAKE_ASSIGNMENT_ER_COLLECTION_NAME, fake_config_module
            ),
            EntityResolutionDocumentCollectionConfig,
        )
        generation_output_table_id = (
            er_collection.temp_document_generation_output_table_address(
                "test_run_id"
            ).table_id
        )
        self.assertEqual(
            [
                generation_output_table_id,
                er_collection.entry_source_map_table_address(
                    sandbox_dataset_prefix=None
                ).table_id,
                er_collection.temp_document_metadata_updates_table_address(
                    "test_run_id"
                ).table_id,
            ],
            self._write_table_ids(),
        )
        # The map is rebuilt from the same snapshot the diff reads.
        map_write_call = self.bq_client.create_table_from_query.call_args_list[1]
        self.assertIn(generation_output_table_id, map_write_call.kwargs["query"])

    def test_first_order_collection_never_touches_a_map_table(self) -> None:
        mock_row_iterator = MagicMock()
        mock_row_iterator.total_rows = 0
        self.bq_client.create_table_from_query.return_value = mock_row_iterator

        self._run_discovery(get_fake_first_order_document_collection_config().name)

        first_order_collection = get_fake_first_order_document_collection_config()
        self.assertEqual(
            [
                first_order_collection.temp_document_generation_output_table_address(
                    "test_run_id"
                ).table_id,
                first_order_collection.temp_document_metadata_updates_table_address(
                    "test_run_id"
                ).table_id,
            ],
            self._write_table_ids(),
        )

    def test_results_prefix_flows_to_map_hydrator(self) -> None:
        mock_row_iterator = MagicMock()
        mock_row_iterator.total_rows = 0
        self.bq_client.create_table_from_query.return_value = mock_row_iterator

        with patch(
            "recidiviz.documents.store.new_document_discovery."
            "EntityResolutionEntrySourceMapHydrator"
        ) as mock_hydrator:
            self._run_discovery(
                FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
                sandbox_context=self._entity_resolution_sandbox_context(
                    first_order_source_in_sandbox=True
                ),
            )

        # The map is one of the run's own outputs, so it is written under the collection's
        # output prefix into the sandbox metadata dataset.
        self.assertEqual("sb", mock_hydrator.call_args.kwargs["output_sandbox_prefix"])

    def _run_discovery_forcing_new_documents(
        self,
        collection_name: str,
        sandbox_context: DocumentStoreSandboxContext | None,
    ) -> list[str]:
        """Runs discovery with a forced non-empty diff so the new-documents step also
        runs, and returns every query the discovery issued."""

        def create_table_side_effect(**_kwargs: object) -> MagicMock:
            mock = MagicMock()
            mock.total_rows = 1
            return mock

        self.bq_client.create_table_from_query.side_effect = create_table_side_effect
        self._run_discovery(collection_name, sandbox_context=sandbox_context)
        return [
            call.kwargs["query"]
            for call in self.bq_client.create_table_from_query.call_args_list
        ]

    def test_first_order_existing_collection_diffs_production(self) -> None:
        # A first-order collection that already exists in production: discovery diffs its
        # freshly generated documents against the production (unprefixed) metadata table
        # and checks the production document_contents table for already-uploaded
        # contents, even though the run writes its outputs to the sandbox.
        collection_name = get_fake_first_order_document_collection_config().name
        queries = self._run_discovery_forcing_new_documents(
            collection_name,
            self._first_order_sandbox_context(collection_name, diff_read_prefix=None),
        )
        self.assertTrue(any("us_xx_document_store_metadata" in q for q in queries))
        self.assertFalse(any("sb_us_xx_document_store_metadata" in q for q in queries))
        self.assertFalse(any("sb_us_xx_document_contents" in q for q in queries))

    def test_first_order_new_collection_diffs_empty_sandbox(self) -> None:
        # A brand-new first-order collection with no production document store yet:
        # discovery diffs against the (empty) sandbox metadata table and checks the
        # sandbox document_contents table, so every generated document is discovered as
        # an addition and seeded from scratch.
        collection_name = get_fake_first_order_document_collection_config().name
        queries = self._run_discovery_forcing_new_documents(
            collection_name,
            self._first_order_sandbox_context(collection_name, diff_read_prefix="sb"),
        )
        self.assertTrue(any("sb_us_xx_document_store_metadata" in q for q in queries))
        self.assertTrue(any("sb_us_xx_document_contents" in q for q in queries))

    def test_entity_resolution_diffs_own_sandbox(self) -> None:
        # An ER collection always diffs against its own sandbox copy: the metadata table
        # the diff reads and the document_contents table the new-documents step reads are
        # both the sandbox-scoped tables the run writes.
        queries = self._run_discovery_forcing_new_documents(
            FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
            self._entity_resolution_sandbox_context(first_order_source_in_sandbox=True),
        )
        self.assertTrue(any("sb_us_xx_document_store_metadata" in q for q in queries))
        self.assertTrue(any("sb_us_xx_document_contents" in q for q in queries))

    def test_entity_resolution_first_order_source_read_independent_of_diff(
        self,
    ) -> None:
        # With the first-order documents left in production but the ER collection's own
        # tables in the sandbox, composite generation reads the production first-order
        # contents table while the diff still reads the sandbox metadata — guarding the
        # regression that pointed composite generation at a sandbox first-order contents
        # table that was never created.
        queries = self._run_discovery_forcing_new_documents(
            FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
            self._entity_resolution_sandbox_context(
                first_order_source_in_sandbox=False
            ),
        )
        generation_query = queries[0]
        self.assertIn(
            "us_xx_document_contents.fake_input_notes_document_contents",
            generation_query,
        )
        self.assertNotIn(
            "sb_us_xx_document_contents.fake_input_notes_document_contents",
            generation_query,
        )
        self.assertTrue(any("sb_us_xx_document_store_metadata" in q for q in queries))
