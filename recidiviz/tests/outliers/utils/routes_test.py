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
"""Tests for the outliers_utils blueprint."""
import base64
import unittest
from typing import Any
from unittest.mock import patch

from flask import Flask
from freezegun import freeze_time

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.outliers.utils.routes import get_outliers_utils_blueprint
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class TestOutliersUtilsRoutes(unittest.TestCase):
    """Tests the outliers-utils blueprint"""

    def setUp(self) -> None:
        self.fake_gcs = FakeGCSFileSystem()
        self.gcs_factory_patcher = patch("recidiviz.tools.archive.GcsfsFactory.build")
        self.gcs_factory_patcher.start().return_value = self.fake_gcs
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-test"

        self.headers: dict[str, Any] = {"x-goog-iap-jwt-assertion": {}}

        self.test_app = Flask(__name__)
        self.test_app.register_blueprint(
            get_outliers_utils_blueprint(), url_prefix="/outliers-utils"
        )

    def tearDown(self) -> None:
        self.gcs_factory_patcher.stop()
        self.project_id_patcher.stop()

    @freeze_time("2023-12-04 12:04")
    def test_archive_file(self) -> None:
        test_filename = "US_XX/test_file.json"
        test_data = "\n".join(['{"a": "b"}', '{"a": "z"}'])
        self.fake_gcs.upload_from_string(
            GcsfsFilePath.from_absolute_path(
                f"gs://recidiviz-test-insights-etl-data/{test_filename}"
            ),
            test_data,
            "text/json",
        )

        with self.test_app.test_client() as client:
            response = client.post(
                "/outliers-utils/archive-file",
                json={
                    "message": {
                        "data": base64.b64encode(b"anything").decode(),
                        "attributes": {
                            "bucketId": "recidiviz-test-insights-etl-data",
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
                        f"gs://recidiviz-test-insights-etl-data-archive/2023-12-04/{test_filename}"
                    )
                ),
                test_data,
            )

    def test_archive_file_invalid_request(self) -> None:
        with self.test_app.test_client() as client:
            # No message in request
            self.assertEqual(
                client.post("/outliers-utils/archive-file").status_code, 400
            )

            # No object id in pub/sub message
            self.assertEqual(
                client.post(
                    "/outliers-utils/archive-file",
                    json={
                        "message": {
                            "attributes": {},
                        }
                    },
                ).status_code,
                400,
            )

            # No attributes
            self.assertEqual(
                client.post(
                    "/outliers-utils/archive-file",
                    json={"filename": "US_XX/test_file.json"},
                ).status_code,
                400,
            )

    @freeze_time("2023-12-04 12:04")
    def test_archive_file_ignore_staging(self) -> None:
        test_filename = "staging/US_XX/test_file.json"
        test_data = "\n".join(['{"a": "b"}', '{"a": "z"}'])
        self.fake_gcs.upload_from_string(
            GcsfsFilePath.from_absolute_path(
                f"gs://recidiviz-test-insights-etl-data/{test_filename}"
            ),
            test_data,
            "text/json",
        )

        with self.test_app.test_client() as client:
            response = client.post(
                "/outliers-utils/archive-file",
                json={
                    "message": {
                        "data": base64.b64encode(b"anything").decode(),
                        "attributes": {
                            "bucketId": "recidiviz-test-insights-etl-data",
                            "objectId": test_filename,
                        },
                    }
                },
            )

            # No file actually archived since it was staging
            self.assertEqual(response.status_code, 200)
            self.assertFalse(
                self.fake_gcs.exists(
                    GcsfsFilePath.from_absolute_path(
                        f"gs://recidiviz-test-insights-etl-data-archive/2023-12-04/{test_filename}"
                    )
                )
            )
