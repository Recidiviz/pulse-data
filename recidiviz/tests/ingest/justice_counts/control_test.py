# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for justice counts request handlers"""

import unittest
from typing import Dict

from flask import Flask
from mock import ANY, patch, Mock

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.justice_counts import control


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", Mock(return_value="123456789"))
class TestDirectIngestControl(unittest.TestCase):
    """Tests for requests to the Direct Ingest API."""

    def setUp(self) -> None:
        app = Flask(__name__)
        app.register_blueprint(control.justice_counts_control)
        app.config["TESTING"] = True
        self.client = app.test_client()

        self.storage_client_patcher = patch("google.cloud.storage.Client")
        self.storage_client_patcher.start()

    def tearDown(self) -> None:
        self.storage_client_patcher.stop()

    @patch("recidiviz.tools.justice_counts.manual_upload.ingest")
    def test_withManifest_succeeds(self, mock_ingest: unittest.mock.MagicMock) -> None:
        # Act
        request_args = {"manifest_path": "gs://fake-bucket/foo/manifest.yaml"}
        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get(
            "/ingest", query_string=request_args, headers=headers
        )

        # Assert
        self.assertEqual(200, response.status_code)
        mock_ingest.assert_called_with(
            ANY, GcsfsFilePath(bucket_name="fake-bucket", blob_name="foo/manifest.yaml")
        )

    def test_withoutManifest_rejectsQuery(self) -> None:
        # Act
        request_args: Dict[str, str] = {}
        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get(
            "/ingest", query_string=request_args, headers=headers
        )

        # Assert
        self.assertEqual(400, response.status_code)

    @patch("recidiviz.tools.justice_counts.manual_upload.ingest")
    def test_ingestFails_raisesError(
        self, mock_ingest: unittest.mock.MagicMock
    ) -> None:
        # Arrange
        mock_ingest.side_effect = ValueError("Malformed manifest")

        # Act
        request_args = {"manifest_path": "gs://fake-bucket/foo/manifest.yaml"}
        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get(
            "/ingest", query_string=request_args, headers=headers
        )

        # Assert
        self.assertEqual(500, response.status_code)
        self.assertEqual(
            "Error ingesting data: 'Malformed manifest'", response.get_data().decode()
        )
        mock_ingest.assert_called_with(
            ANY, GcsfsFilePath(bucket_name="fake-bucket", blob_name="foo/manifest.yaml")
        )
