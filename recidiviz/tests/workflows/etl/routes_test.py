# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for the practices_etl blueprint."""
import base64
import unittest
from http import HTTPStatus
from typing import Any
from unittest.mock import MagicMock, patch

from flask import Flask
from freezegun import freeze_time

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.workflows.etl.routes import get_workflows_etl_blueprint


class TestWorkflowsETLRoutes(unittest.TestCase):
    """Tests the practices_etl blueprint"""

    def setUp(self) -> None:
        self.fake_gcs = FakeGCSFileSystem()
        self.gcs_factory_patcher = patch(
            "recidiviz.workflows.etl.archive.GcsfsFactory.build"
        )
        self.gcs_factory_patcher.start().return_value = self.fake_gcs
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-test"

        self.requires_gae_auth_patcher = patch(
            "recidiviz.workflows.etl.routes.requires_gae_auth",
            side_effect=lambda route: route,
        )
        self.requires_gae_auth_patcher.start()
        self.headers: dict[str, Any] = {"x-goog-iap-jwt-assertion": {}}

        self.test_app = Flask(__name__)
        self.test_app.register_blueprint(
            get_workflows_etl_blueprint(), url_prefix="/practices-etl"
        )

    def tearDown(self) -> None:
        self.gcs_factory_patcher.stop()
        self.project_id_patcher.stop()
        self.requires_gae_auth_patcher.stop()

    @patch("recidiviz.workflows.etl.routes.CloudTaskQueueManager")
    def test_handle_workflows_firestore_etl(self, mock_task_manager: MagicMock) -> None:
        test_filename = "US_XX/test_file.json"

        with self.test_app.test_client() as client:
            response = client.post(
                "/practices-etl/handle_workflows_firestore_etl",
                headers=self.headers,
                json={
                    "message": {
                        "attributes": {
                            "bucketId": "recidiviz-test-practices-etl-data",
                            "objectId": test_filename,
                        },
                    }
                },
            )
            self.assertEqual(b"", response.data)
            self.assertEqual(HTTPStatus.OK, response.status_code)

            mock_task_manager.return_value.create_task.assert_called_with(
                relative_uri="/practices-etl/_run_firestore_etl",
                body={"filename": "test_file.json", "state_code": "US_XX"},
            )

    @patch("recidiviz.workflows.etl.routes.CloudTaskQueueManager")
    def test_handle_workflows_firestore_etl_missing_region_code(
        self, mock_task_manager: MagicMock
    ) -> None:
        test_filename = "test_file.json"

        with self.test_app.test_client() as client:
            response = client.post(
                "/practices-etl/handle_workflows_firestore_etl",
                headers=self.headers,
                json={
                    "message": {
                        "attributes": {
                            "bucketId": "recidiviz-test-practices-etl-data",
                            "objectId": test_filename,
                        },
                    }
                },
            )
            mock_task_manager.return_value.create_task.assert_not_called()
            self.assertEqual(b"Missing region, ignoring", response.data)
            self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_handle_workflows_firestore_etl_invalid_message(self) -> None:
        with self.test_app.test_client() as client:
            response = client.post(
                "/practices-etl/handle_workflows_firestore_etl",
                headers=self.headers,
                json={
                    "message": {
                        "attributes": None,
                    }
                },
            )
            self.assertEqual(b"Invalid Pub/Sub message", response.data)
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    @patch("recidiviz.workflows.etl.routes.get_workflows_delegates")
    def test_run_firestore_etl(self, mock_get_delegates: MagicMock) -> None:
        mock_delegate = MagicMock()
        mock_get_delegates.return_value = [mock_delegate]
        state_code = "US_XX"
        filename = "test_file.json"
        with self.test_app.test_client() as client:
            response = client.post(
                "/practices-etl/_run_firestore_etl",
                headers=self.headers,
                json={"state_code": state_code, "filename": filename},
            )
            mock_delegate.supports_file.assert_called_with(state_code, filename)
            mock_delegate.run_etl.assert_called_with(state_code, filename)
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(b"", response.data)

    @patch("recidiviz.workflows.etl.routes.get_workflows_delegates")
    def test_run_firestore_etl_unsupported_file(
        self, mock_get_delegates: MagicMock
    ) -> None:
        mock_delegate = MagicMock()
        mock_delegate.supports_file.return_value = False
        mock_get_delegates.return_value = [mock_delegate]
        with self.test_app.test_client() as client:
            response = client.post(
                "/practices-etl/_run_firestore_etl",
                headers=self.headers,
                json={"state_code": "US_XX", "filename": "test_file.json"},
            )
            mock_delegate.return_value.run_etl.assert_not_called()
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(b"", response.data)

    @patch("recidiviz.workflows.etl.routes.get_workflows_delegates")
    def test_run_firestore_etl_missing_param(
        self, _mock_get_delegates: MagicMock
    ) -> None:
        with self.test_app.test_client() as client:
            response = client.post(
                "/practices-etl/_run_firestore_etl",
                headers=self.headers,
                json={"state_code": None, "filename": "test_file.json"},
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Must include filename and state_code in the request body",
                response.data,
            )

    @freeze_time("2022-03-15 06:15")
    def test_archive_file(self) -> None:
        test_filename = "US_XX/test_file.json"
        test_data = "\n".join(['{"a": "b"}', '{"a": "z"}'])
        self.fake_gcs.upload_from_string(
            GcsfsFilePath.from_absolute_path(
                f"gs://recidiviz-test-practices-etl-data/{test_filename}"
            ),
            test_data,
            "text/json",
        )

        with self.test_app.test_client() as client:
            response = client.post(
                "/practices-etl/archive-file",
                json={
                    "message": {
                        "data": base64.b64encode(b"anything").decode(),
                        "attributes": {
                            "bucketId": "recidiviz-test-practices-etl-data",
                            "objectId": test_filename,
                        },
                    }
                },
            )
            self.assertEqual(response.status_code, 200)

            # An archive of report JSON is stored
            self.assertEqual(
                self.fake_gcs.download_as_string(
                    GcsfsFilePath.from_absolute_path(
                        f"gs://recidiviz-test-practices-etl-data-archive/2022-03-15/{test_filename}"
                    )
                ),
                test_data,
            )

    def test_archive_file_invalid_request(self) -> None:
        with self.test_app.test_client() as client:
            self.assertEqual(
                client.post("/practices-etl/archive-file").status_code, 400
            )

            self.assertEqual(
                client.post(
                    "/practices-etl/archive-file",
                    json={
                        "message": {
                            "attributes": {},
                        }
                    },
                ).status_code,
                400,
            )

            self.assertEqual(
                client.post(
                    "/practices-etl/archive-file",
                    json={"filename": "US_XX/test_file.json"},
                ).status_code,
                400,
            )

    @freeze_time("2022-03-15 06:15")
    def test_archive_file_ignore_staging(self) -> None:
        test_filename = "staging/US_XX/test_file.json"
        test_data = "\n".join(['{"a": "b"}', '{"a": "z"}'])
        self.fake_gcs.upload_from_string(
            GcsfsFilePath.from_absolute_path(
                f"gs://recidiviz-test-practices-etl-data/{test_filename}"
            ),
            test_data,
            "text/json",
        )

        with self.test_app.test_client() as client:
            response = client.post(
                "/practices-etl/archive-file",
                json={
                    "message": {
                        "data": base64.b64encode(b"anything").decode(),
                        "attributes": {
                            "bucketId": "recidiviz-test-practices-etl-data",
                            "objectId": test_filename,
                        },
                    }
                },
            )

            # We should get a successful response code, but nothing should actually happen.
            self.assertEqual(response.status_code, 200)
            self.assertFalse(
                self.fake_gcs.exists(
                    GcsfsFilePath.from_absolute_path(
                        f"gs://recidiviz-test-practices-etl-data-archive/2022-03-15/{test_filename}"
                    )
                )
            )
