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

"""Tests for ops_routes.py."""
from http import HTTPStatus
from typing import Any, Dict, Optional
from unittest import TestCase
from unittest.mock import ANY, MagicMock, patch

import flask
import pytest
from flask import Flask

from recidiviz.case_triage.ops_routes import case_triage_ops_blueprint
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.schema.case_triage.schema import ETLClient
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres import local_postgres_helpers


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", MagicMock(return_value="123456789"))
@patch(
    "recidiviz.utils.validate_jwt.validate_iap_jwt_from_app_engine",
    MagicMock(return_value=("test-user", "test-user@recidiviz.org", None)),
)
@pytest.mark.uses_db
class OpsRoutesTest(TestCase):
    """Integration tests of our flask auth endpoints"""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def setUp(self) -> None:
        self.app = Flask(__name__)
        self.app.register_blueprint(case_triage_ops_blueprint)
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()

        self.headers: Dict[str, Dict[Any, Any]] = {"x-goog-iap-jwt-assertion": {}}
        self.bucket = "test-project-case-triage-data"
        self.filename = "etl_clients.csv"
        self.gcs_csv_uri = GcsfsFilePath.from_absolute_path(
            f"{self.bucket}/{self.filename}"
        )
        self.columns = [col.name for col in ETLClient.__table__.columns]

        # Setup database
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

        with self.app.test_request_context():
            self.handle_gcs_imports = flask.url_for(
                "/case_triage_ops._handle_gcs_imports"
            )
            self.run_gcs_imports = flask.url_for("/case_triage_ops._run_gcs_imports")

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @patch("recidiviz.case_triage.ops_routes.CloudTaskQueueManager")
    def test_handle_gcs_imports(self, mock_task_manager: MagicMock) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.handle_gcs_imports,
                headers=self.headers,
                json={
                    "message": {
                        "attributes": {
                            "bucketId": self.bucket,
                            "objectId": self.filename,
                        },
                    }
                },
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(b"", response.data)

            mock_task_manager.return_value.create_task.assert_called_with(
                relative_uri=f"/case_triage_ops{self.run_gcs_imports}",
                body={"filename": self.filename},
            )

    def test_handle_gcs_imports_invalid_pubsub(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.handle_gcs_imports,
                headers=self.headers,
                json={
                    "message": {
                        "attributes": {},
                    }
                },
            )
            self.assertEqual(b"Invalid Pub/Sub message", response.data)
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_handle_gcs_imports_missing_filename(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.handle_gcs_imports,
                headers=self.headers,
                json={
                    "message": {
                        "attributes": {
                            "bucketId": self.bucket,
                        },
                    }
                },
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Pub/Sub message must include an objectId attribute", response.data
            )

    @patch("recidiviz.case_triage.ops_routes.CloudTaskQueueManager")
    def test_handle_gcs_imports_ignore_file(self, mock_task_manager: MagicMock) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.handle_gcs_imports,
                headers=self.headers,
                json={
                    "message": {
                        "attributes": {
                            "bucketId": self.bucket,
                            "objectId": "ignoreme.csv",
                        },
                    }
                },
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(b"", response.data)

            mock_task_manager.return_value.create_task.assert_not_called()

    @patch(
        "recidiviz.case_triage.ops_routes.import_gcs_csv_to_cloud_sql", autospec=True
    )
    def test_run_gcs_imports(self, mock_import_csv: MagicMock) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.run_gcs_imports,
                headers=self.headers,
                json={"filename": self.filename},
            )
            mock_import_csv.assert_called_with(
                database_key=self.database_key,
                model=ETLClient,
                gcs_uri=self.gcs_csv_uri,
                columns=ANY,  # columns are unordered so compare them separately
                seconds_to_wait=180,
            )
            self.assertCountEqual(mock_import_csv.call_args.args[3], self.columns)
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(b"", response.data)

    def test_run_gcs_imports_missing_filename(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.run_gcs_imports,
                headers=self.headers,
                json={},
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Must include `filename` in the json payload", response.data
            )

    @patch(
        "recidiviz.case_triage.ops_routes.import_gcs_csv_to_cloud_sql", autospec=True
    )
    def test_run_gcs_imports_ignored_csv(self, mock_import_csv: MagicMock) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.run_gcs_imports,
                headers=self.headers,
                json={"filename": "ignoreme.csv"},
            )
            mock_import_csv.assert_not_called()
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(b"", response.data)
