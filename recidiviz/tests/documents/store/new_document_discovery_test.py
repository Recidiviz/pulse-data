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

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    collect_document_collection_configs,
    get_document_collection_config,
)
from recidiviz.documents.store.new_document_discovery import NewDocumentDiscoverer
from recidiviz.tests.documents.store import config as fake_config_module


class TestNewDocumentDiscovery(unittest.TestCase):
    """Tests for the NewDocumentDiscoverer class"""

    def setUp(self) -> None:
        self.bq_client = MagicMock()
        self.collection_name = next(
            iter(
                collect_document_collection_configs(
                    StateCode.US_XX, config_module=fake_config_module
                )
            )
        )

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
        # Only the metadata-updates temp table is written; the new-documents
        # step is skipped when there are no metadata updates.
        self.assertEqual(self.bq_client.create_table_from_query.call_count, 1)
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
        self.assertEqual(self.bq_client.create_table_from_query.call_count, 2)

    def test_metadata_updates_without_new_documents(self) -> None:
        self._mock_temp_table_row_counts(
            new_document_contents_rows=0,
            document_metadata_updates_rows=4,
        )

        result = self.discovery.run()

        assert result is not None
        self.assertEqual(result.num_document_metadata_updates_rows, 4)
        self.assertEqual(result.num_new_document_contents_rows, 0)
