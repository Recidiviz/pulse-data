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

import unittest
from unittest.mock import MagicMock, patch

from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config_collectors import (
    get_document_collection_config,
)
from recidiviz.documents.store.new_document_discovery import NewDocumentDiscoverer
from recidiviz.tests.documents import fake_config as fake_config_module
from recidiviz.tests.documents.store.document_store_test_utils import (
    get_fake_first_order_document_collection_config,
)


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
