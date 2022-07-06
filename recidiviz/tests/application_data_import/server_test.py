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
"""Implements tests for the Application Data Import Flask server."""
import base64
from http import HTTPStatus
from unittest import TestCase
from unittest.mock import ANY, MagicMock, patch

from fakeredis import FakeRedis

from recidiviz.application_data_import.server import app
from recidiviz.case_triage.pathways.metric_queries import (
    LibertyToPrisonTransitionsCount,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.pathways.schema import (
    LibertyToPrisonTransitions,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
class TestApplicationDataImportRoutes(TestCase):
    """Implements tests for the Application Data Import Flask server."""

    def setUp(self) -> None:
        self.app = app
        self.client = self.app.test_client()
        self.bucket = "test-project-dashboard-event-level-data"
        self.pathways_view = "liberty_to_prison_transitions"
        self.columns = [
            col.name for col in LibertyToPrisonTransitions.__table__.columns
        ]

    @patch("recidiviz.application_data_import.server.CloudTaskQueueManager")
    def test_import_trigger_pathways(self, mock_task_manager: MagicMock) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                "/import/trigger_pathways",
                json={
                    "message": {
                        "data": base64.b64encode(b"anything").decode(),
                        "attributes": {
                            "bucketId": self.bucket,
                            "objectId": "US_XX/test-file.csv",
                        },
                    }
                },
            )
            self.assertEqual(b"", response.data)
            self.assertEqual(HTTPStatus.OK, response.status_code)

            mock_task_manager.return_value.create_task.assert_called_with(
                absolute_uri="http://localhost:5000/import/pathways/US_XX/test-file.csv",
                service_account_email="fake-acct@fake-project.iam.gserviceaccount.com",
                task_id="import-pathways-US_XX-test-file-csv",
            )

    def test_import_trigger_pathways_bad_message(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                "/import/trigger_pathways",
                json={"message": {}},
            )
            self.assertEqual(b"Invalid Pub/Sub message", response.data)
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_import_trigger_pathways_invalid_bucket(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                "/import/trigger_pathways",
                json={
                    "message": {
                        "attributes": {
                            "bucketId": "invalid-bucket",
                            "objectId": "US_XX/test-file.csv",
                        },
                    },
                    "subscription": "test-subscription",
                },
            )
            self.assertEqual(
                b"/trigger_pathways is only configured for the dashboard-event-level-data bucket, saw invalid-bucket",
                response.data,
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_import_trigger_pathways_invalid_object(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                "/import/trigger_pathways",
                json={
                    "message": {
                        "attributes": {
                            "bucketId": self.bucket,
                            "objectId": "staging/US_XX/test-file.csv",
                        },
                    },
                    "subscription": "test-subscription",
                },
            )
            self.assertEqual(
                b"Invalid object ID staging/US_XX/test-file.csv, must be of format <state_code>/<filename>",
                response.data,
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    @patch(
        "recidiviz.application_data_import.server.import_gcs_csv_to_cloud_sql",
        autospec=True,
    )
    @patch(
        "recidiviz.case_triage.pathways.metric_cache.PathwaysMetricCache", autospec=True
    )
    @patch(
        "recidiviz.case_triage.pathways.metric_cache.get_pathways_metric_redis",
        return_value=FakeRedis(),
    )
    def test_import_pathways_successful(
        self,
        mock_redis: MagicMock,
        mock_metric_cache: MagicMock,
        mock_import_csv: MagicMock,
    ) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                f"/import/pathways/US_XX/{self.pathways_view}.csv",
            )
            mock_import_csv.assert_called_with(
                database_key=SQLAlchemyDatabaseKey(
                    schema_type=SchemaType.PATHWAYS, db_name="us_xx"
                ),
                model=LibertyToPrisonTransitions,
                gcs_uri=GcsfsFilePath.from_bucket_and_blob_name(
                    self.bucket, f"US_XX/{self.pathways_view}.csv"
                ),
                columns=self.columns,
            )
            mock_redis.assert_called()
            mock_metric_cache.assert_called_with(
                state_code=StateCode.US_XX, metric_fetcher=ANY, redis=ANY
            )
            mock_metric_cache.return_value.reset_cache.assert_called_with(
                LibertyToPrisonTransitionsCount
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_import_pathways_invalid_state(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                f"/import/pathways/US_ABC/{self.pathways_view}.csv",
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Unknown state_code [US_ABC] received, must be a valid state code.",
                response.data,
            )

    def test_import_pathways_invalid_file(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                "/import/pathways/US_XX/unknown_file.csv",
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Invalid filename unknown_file.csv, must match a Pathways event-level view",
                response.data,
            )

    @patch("recidiviz.application_data_import.server.get_database_entity_by_table_name")
    def test_import_pathways_invalid_table(self, mock_get_database: MagicMock) -> None:
        error_message = f"Could not find model with table named {self.pathways_view}"
        with self.app.test_request_context():
            mock_get_database.side_effect = ValueError(error_message)
            response = self.client.post(
                f"/import/pathways/US_XX/{self.pathways_view}.csv",
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                error_message.encode("UTF-8"),
                response.data,
            )
