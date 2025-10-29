# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for FirestoreClientImpl"""
import unittest
from datetime import datetime
from unittest import mock
from unittest.mock import Mock, call

from google.cloud.firestore_admin_v1 import CreateIndexRequest

from recidiviz.firestore.firestore_client import FirestoreClientImpl


class FirestoreClientImplTest(unittest.TestCase):
    """Tests for FirestoreClientImpl"""

    def setUp(self) -> None:
        self.mock_project_id = "test-project-id"
        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.mock_project_id
        self.client_patcher = mock.patch(
            "recidiviz.firestore.firestore_client.firestore_v1.Client"
        )
        self.async_client_patcher = mock.patch(
            "recidiviz.firestore.firestore_client.firestore_v1.AsyncClient"
        )
        self.admin_client_patcher = mock.patch(
            "recidiviz.firestore.firestore_client.firestore_admin_v1.FirestoreAdminClient"
        )
        self.client_fn = self.client_patcher.start()
        self.mock_client = mock.MagicMock()
        self.mock_admin_client = mock.MagicMock()
        self.client_fn.return_value = self.mock_client
        self.admin_client_fn = self.admin_client_patcher.start()
        self.admin_client_fn.return_value = self.mock_admin_client
        self.async_client_patcher.start()

        self.firestore_client = FirestoreClientImpl(project_id=self.mock_project_id)

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.metadata_patcher.stop()
        self.admin_client_fn.stop()
        self.async_client_patcher.stop()

    @mock.patch(
        "recidiviz.firestore.firestore_client.assert_type",
        wraps=lambda value, _type: value,
    )
    @mock.patch("recidiviz.firestore.firestore_client.FieldFilter")
    def test_delete_old_documents(
        self, field_filter_mock: Mock, _assert_type_mock: Mock
    ) -> None:
        """Tests that the call to delete old documents actually filters for old documents."""
        self.firestore_client.delete_old_documents(
            "clients", "US_XX", "__lastUpdated", datetime(2022, 1, 1)
        )
        field_filter_mock.assert_has_calls(
            [
                call("stateCode", "==", "US_XX"),
                call().where("__lastUpdated", "<", datetime(2022, 1, 1)),
            ]
        )

    def test_list_collections_with_indexes(self) -> None:
        """Tests that list_collections_with_indexes is called with the correct args and correctly parses
        the index name for the collection name."""
        mock_index = Mock()
        mock_index.configure_mock(
            name="projects/test-project-id/databases/(default)/collectionGroups/clients/indexes/123"
        )
        self.mock_admin_client.list_indexes.return_value = [mock_index]
        collections = self.firestore_client.list_collections_with_indexes()
        self.mock_admin_client.list_indexes.assert_called_with(
            request={
                "parent": "projects/test-project-id/databases/"
                "(default)/collectionGroups/all"
            }
        )
        self.assertEqual(["clients"], collections)

    def test_index_exists_for_collection(self) -> None:
        """Tests that index_exists_for_collection returns expected values."""
        mock_index = Mock()
        mock_index.configure_mock(
            name="projects/test-project-id/databases/(default)/collectionGroups/clients/indexes/123"
        )
        self.mock_admin_client.list_indexes.return_value = [mock_index]

        self.assertTrue(self.firestore_client.index_exists_for_collection("clients"))
        self.assertFalse(
            self.firestore_client.index_exists_for_collection("other_collection")
        )

    def test_create_index(self) -> None:
        """Tests that create_index is called with the correct args."""
        self.firestore_client.create_index("clients", CreateIndexRequest())
        self.mock_admin_client.create_index.assert_called_once()
        self.assertEqual(
            self.mock_admin_client.mock_calls[0].kwargs["request"],
            CreateIndexRequest(
                parent="projects/test-project-id/databases/"
                "(default)/collectionGroups/clients"
            ),
        )
