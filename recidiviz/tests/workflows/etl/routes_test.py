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
from unittest.mock import patch

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

        self.test_app = Flask(__name__)
        self.test_app.register_blueprint(
            get_workflows_etl_blueprint(), url_prefix="/practices-etl"
        )

    def tearDown(self) -> None:
        self.gcs_factory_patcher.stop()
        self.project_id_patcher.stop()
        self.requires_gae_auth_patcher.stop()

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
