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

from mock import call

from recidiviz.firestore.firestore_client import FirestoreClientImpl


class BigQueryClientImplTest(unittest.TestCase):
    """Tests for FirestoreClientImpl"""

    def setUp(self) -> None:
        self.mock_project_id = "test-project-id"
        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.mock_project_id
        self.client_patcher = mock.patch(
            "recidiviz.firestore.firestore_client.firestore.Client"
        )
        self.client_fn = self.client_patcher.start()
        self.mock_client = mock.MagicMock()
        self.client_fn.return_value = self.mock_client
        self.firestore_client = FirestoreClientImpl(project_id=self.mock_project_id)

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.metadata_patcher.stop()

    def test_delete_old_documents(self) -> None:
        self.mock_collection = mock.MagicMock()
        self.mock_client.collection.return_value = self.mock_collection
        self.firestore_client.delete_old_documents(
            "clients", "US_XX", "__lastUpdated", datetime(2022, 1, 1)
        )
        self.mock_collection.where.assert_has_calls(
            [
                call("stateCode", "==", "US_XX"),
                call().where("__lastUpdated", "<", datetime(2022, 1, 1)),
            ]
        )
