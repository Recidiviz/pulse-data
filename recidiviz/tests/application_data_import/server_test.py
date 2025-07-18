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
import os
from datetime import date
from http import HTTPStatus
from typing import Optional
from unittest import TestCase
from unittest.mock import ANY, MagicMock, patch

import pytest
from fakeredis import FakeRedis

from recidiviz.application_data_import.server import _dashboard_event_level_bucket, app
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.case_triage.pathways.metrics.metric_query_builders import (
    ALL_METRICS_BY_NAME,
)
from recidiviz.case_triage.pathways.pathways_database_manager import (
    PathwaysDatabaseManager,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.persistence.database.database_managers.state_segmented_database_manager import (
    StateSegmentedDatabaseManager,
)
from recidiviz.persistence.database.schema.insights.schema import (
    SupervisionOfficer as InsightsSupervisionOfficer,
)
from recidiviz.persistence.database.schema.pathways.schema import (
    LibertyToPrisonTransitions,
    MetricMetadata,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@pytest.mark.uses_db
class TestApplicationDataImportPathwaysRoutes(TestCase):
    """Implements tests for the Application Data Import Flask server."""

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
        self.app = app
        self.client = self.app.test_client()
        self.bucket = "test-project-dashboard-event-level-data"
        self.pathways_view = "liberty_to_prison_transitions"
        self.metric = "LibertyToPrisonTransitions"
        self.state_code = "US_XX"
        self.columns = [
            col.name for col in LibertyToPrisonTransitions.__table__.columns
        ]
        self.fs = FakeGCSFileSystem()
        self.fs_patcher = patch.object(GcsfsFactory, "build", return_value=self.fs)
        self.fs_patcher.start()
        self.database_key = PathwaysDatabaseManager.database_key_for_state(
            self.state_code
        )
        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)

    def tearDown(self) -> None:
        self.fs_patcher.stop()
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @patch("recidiviz.application_data_import.server.SingleCloudTaskQueueManager")
    def test_import_trigger_pathways(self, mock_task_manager: MagicMock) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                "/import/trigger_pathways",
                json={
                    "message": {
                        "data": base64.b64encode(b"anything").decode(),
                        "attributes": {
                            "bucketId": self.bucket,
                            "objectId": f"{self.state_code}/test-file.csv",
                        },
                        "messageId": "12345",
                    }
                },
            )
            self.assertEqual(b"", response.data)
            self.assertEqual(HTTPStatus.OK, response.status_code)

            mock_task_manager.return_value.create_task.assert_called_with(
                absolute_uri=f"http://localhost:5000/import/pathways/{self.state_code}/test-file.csv",
                service_account_email="fake-acct@fake-project.iam.gserviceaccount.com",
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
                            "objectId": f"{self.state_code}/test-file.csv",
                        },
                        "messageId": "12345",
                    },
                    "subscription": "test-subscription",
                },
            )
            self.assertEqual(
                b"/import/trigger_pathways is only configured for the gs://test-project-dashboard-event-level-data bucket, saw invalid-bucket",
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
                            "objectId": f"staging/{self.state_code}/test-file.csv",
                        },
                        "messageId": "12345",
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
                f"/import/pathways/{self.state_code}/{self.pathways_view}.csv",
            )
            mock_import_csv.assert_called_with(
                database_key=SQLAlchemyDatabaseKey(
                    schema_type=SchemaType.PATHWAYS, db_name="us_xx"
                ),
                model=LibertyToPrisonTransitions,
                gcs_uri=GcsfsFilePath.from_bucket_and_blob_name(
                    self.bucket, f"{self.state_code}/{self.pathways_view}.csv"
                ),
                columns=self.columns,
            )
            mock_redis.assert_called()
            mock_metric_cache.assert_called_with(
                state_code=StateCode.US_XX, metric_fetcher=ANY, redis=ANY
            )
            mock_metric_cache.return_value.reset_cache.assert_called_with(
                ALL_METRICS_BY_NAME["LibertyToPrisonTransitionsCount"]
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
                f"/import/pathways/{self.state_code}/unknown_file.csv",
            )
            self.assertEqual(
                HTTPStatus.BAD_REQUEST, response.status_code, response.data
            )
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
                f"/import/pathways/{self.state_code}/{self.pathways_view}.csv",
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                error_message.encode("UTF-8"),
                response.data,
            )

    @patch(
        "recidiviz.application_data_import.server.import_gcs_csv_to_cloud_sql",
        autospec=True,
    )
    @patch(
        "recidiviz.case_triage.pathways.metric_cache.get_pathways_metric_redis",
        return_value=FakeRedis(),
    )
    def test_import_pathways_metadata(
        self,
        mock_redis: MagicMock,
        mock_import_csv: MagicMock,
    ) -> None:
        filename = f"{self.pathways_view}.csv"
        filepath = GcsfsFilePath.from_absolute_path(
            os.path.join(
                _dashboard_event_level_bucket(),
                self.state_code + "/" + filename,
            )
        )
        self.fs.upload_from_string(filepath, "test", content_type="text/csv")
        self.fs.update_metadata(filepath, {"last_updated": "2022-01-01"})
        with self.app.test_request_context():
            self.client.post(
                f"/import/pathways/{self.state_code}/{self.pathways_view}.csv",
            )
            mock_import_csv.assert_called()
            mock_redis.assert_called()
            with SessionFactory.using_database(self.database_key) as session:
                result = session.query(MetricMetadata).one()
                self.assertEqual(result.metric, self.metric)
                self.assertEqual(result.last_updated, date(2022, 1, 1))

    @patch(
        "recidiviz.application_data_import.server.import_gcs_csv_to_cloud_sql",
        autospec=True,
    )
    @patch(
        "recidiviz.case_triage.pathways.metric_cache.get_pathways_metric_redis",
        return_value=FakeRedis(),
    )
    def test_import_pathways_metadata_overwrite_existing(
        self,
        mock_redis: MagicMock,
        mock_import_csv: MagicMock,
    ) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                MetricMetadata(
                    metric=self.metric,
                    last_updated=date(2021, 6, 15),
                )
            )

        filename = f"{self.pathways_view}.csv"
        filepath = GcsfsFilePath.from_absolute_path(
            os.path.join(
                _dashboard_event_level_bucket(),
                self.state_code + "/" + filename,
            )
        )
        self.fs.upload_from_string(filepath, "test", content_type="text/csv")
        self.fs.update_metadata(filepath, {"last_updated": "2022-01-01"})
        with self.app.test_request_context():
            self.client.post(
                f"/import/pathways/{self.state_code}/{self.pathways_view}.csv",
            )
            mock_import_csv.assert_called()
            mock_redis.assert_called()
            with SessionFactory.using_database(self.database_key) as session:
                result = session.query(MetricMetadata).one()
                self.assertEqual(result.metric, self.metric)
                self.assertEqual(result.last_updated, date(2022, 1, 1))

    @patch(
        "recidiviz.application_data_import.server.import_gcs_csv_to_cloud_sql",
        autospec=True,
    )
    @patch(
        "recidiviz.case_triage.pathways.metric_cache.get_pathways_metric_redis",
        return_value=FakeRedis(),
    )
    def test_import_pathways_missing_metadata(
        self,
        mock_redis: MagicMock,
        mock_import_csv: MagicMock,
    ) -> None:
        filename = f"{self.pathways_view}.csv"
        filepath = GcsfsFilePath.from_absolute_path(
            os.path.join(
                _dashboard_event_level_bucket(),
                self.state_code + "/" + filename,
            )
        )
        self.fs.upload_from_string(filepath, "test", content_type="text/csv")
        with self.app.test_request_context():
            self.client.post(
                f"/import/pathways/{self.state_code}/{self.pathways_view}.csv",
            )
            mock_import_csv.assert_called()
            mock_redis.assert_called()
            with SessionFactory.using_database(self.database_key) as session:
                destination_table_rows = session.query(MetricMetadata).all()
                self.assertFalse(destination_table_rows)


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch(
    "recidiviz.application_data_import.server._should_load_demo_data_into_insights",
    MagicMock(return_value=False),
)
@pytest.mark.uses_db
class TestApplicationDataImportInsightsRoutes(TestCase):
    """Implements tests for the Insights routes in the Application Data Import Flask server."""

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
        self.app = app
        self.client = self.app.test_client()
        self.bucket = "test-project-insights-etl-data"
        self.view = "supervision_officers"
        self.state_code = "US_XX"
        self.columns = [
            col.name for col in InsightsSupervisionOfficer.__table__.columns
        ]
        self.fs = FakeGCSFileSystem()
        self.fs_patcher = patch.object(GcsfsFactory, "build", return_value=self.fs)
        self.fs_patcher.start()
        self.database_manager = StateSegmentedDatabaseManager(
            get_outliers_enabled_states(), SchemaType.INSIGHTS
        )
        self.database_key = self.database_manager.database_key_for_state(
            self.state_code
        )
        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)

    def tearDown(self) -> None:
        self.fs_patcher.stop()
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @patch("recidiviz.application_data_import.server.SingleCloudTaskQueueManager")
    def test_import_trigger_insights(self, mock_task_manager: MagicMock) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                "/import/trigger_insights",
                json={
                    "message": {
                        "data": base64.b64encode(b"anything").decode(),
                        "attributes": {
                            "bucketId": self.bucket,
                            "objectId": f"{self.state_code}/test-file.json",
                        },
                        "messageId": "12345",
                    }
                },
            )
            self.assertEqual(b"", response.data)
            self.assertEqual(HTTPStatus.OK, response.status_code)

            mock_task_manager.return_value.create_task.assert_called_with(
                absolute_uri=f"http://localhost:5000/import/insights/{self.state_code}/test-file.json",
                service_account_email="fake-acct@fake-project.iam.gserviceaccount.com",
            )

    @patch("recidiviz.application_data_import.server.SingleCloudTaskQueueManager")
    def test_import_trigger_insights_demo(self, mock_task_manager: MagicMock) -> None:
        with self.app.test_request_context(), patch(
            "recidiviz.application_data_import.server._should_load_demo_data_into_insights"
        ) as mock:
            mock.return_value = True
            response = self.client.post(
                "/import/trigger_insights",
                json={
                    "message": {
                        "data": base64.b64encode(b"anything").decode(),
                        "attributes": {
                            "bucketId": "test-project-insights-etl-data-demo",
                            "objectId": f"{self.state_code}/test-file.json",
                        },
                        "messageId": "12345",
                    }
                },
            )
            self.assertEqual(b"", response.data)
            self.assertEqual(HTTPStatus.OK, response.status_code)

            mock_task_manager.return_value.create_task.assert_called_with(
                absolute_uri=f"http://localhost:5000/import/insights/{self.state_code}/test-file.json",
                service_account_email="fake-acct@fake-project.iam.gserviceaccount.com",
            )

    def test_import_trigger_insights_bad_message(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                "/import/trigger_insights",
                json={"message": {}},
            )
            self.assertEqual(b"Invalid Pub/Sub message", response.data)
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_import_trigger_insights_invalid_bucket(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                "/import/trigger_insights",
                json={
                    "message": {
                        "attributes": {
                            "bucketId": "invalid-bucket",
                            "objectId": f"{self.state_code}/test-file.json",
                        },
                        "messageId": "12345",
                    },
                    "subscription": "test-subscription",
                },
            )
            self.assertEqual(
                b"loadDemoDataIntoInsights is False but triggering notification is from bucket invalid-bucket",
                response.data,
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_import_trigger_insights_demo_mismatch(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                "/import/trigger_insights",
                json={
                    "message": {
                        "attributes": {
                            "bucketId": "test-project-insights-etl-data-demo",
                            "objectId": f"staging/{self.state_code}/test-file.json",
                        },
                        "messageId": "12345",
                    },
                },
            )
            self.assertEqual(
                b"loadDemoDataIntoInsights is False but triggering notification is from bucket test-project-insights-etl-data-demo",
                response.data,
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_import_trigger_insights_staging_mismatch(self) -> None:
        with self.app.test_request_context(), patch(
            "recidiviz.application_data_import.server._should_load_demo_data_into_insights"
        ) as mock:
            mock.return_value = True
            response = self.client.post(
                "/import/trigger_insights",
                json={
                    "message": {
                        "attributes": {
                            "bucketId": "test-project-insights-etl-data",
                            "objectId": f"staging/{self.state_code}/test-file.json",
                        },
                        "messageId": "12345",
                    },
                },
            )
            self.assertEqual(
                b"loadDemoDataIntoInsights is True but triggering notification is from bucket test-project-insights-etl-data",
                response.data,
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_import_trigger_insights_invalid_object(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                "/import/trigger_insights",
                json={
                    "message": {
                        "attributes": {
                            "bucketId": self.bucket,
                            "objectId": f"staging/{self.state_code}/test-file.json",
                        },
                        "messageId": "12345",
                    },
                    "subscription": "test-subscription",
                },
            )
            self.assertEqual(
                b"Invalid object ID staging/US_XX/test-file.json, must be of format <state_code>/<filename>",
                response.data,
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    @patch(
        "recidiviz.application_data_import.server.import_gcs_file_to_cloud_sql",
        autospec=True,
    )
    def test_import_insights_successful(
        self,
        mock_import_csv: MagicMock,
    ) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                f"/import/insights/{self.state_code}/{self.view}.json",
            )
            mock_import_csv.assert_called_with(
                database_key=SQLAlchemyDatabaseKey(
                    schema_type=SchemaType.INSIGHTS, db_name="us_xx"
                ),
                model=InsightsSupervisionOfficer,
                gcs_uri=GcsfsFilePath.from_bucket_and_blob_name(
                    self.bucket, f"{self.state_code}/{self.view}.json"
                ),
                columns=self.columns,
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)

    @patch(
        "recidiviz.application_data_import.server.import_gcs_csv_to_cloud_sql",
        autospec=True,
    )
    def test_import_insights_successful_skip(
        self,
        mock_import_csv: MagicMock,
    ) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                f"/import/insights/{self.state_code}/supervision_officers_archive.json",
            )
            mock_import_csv.assert_not_called()

            self.assertEqual(HTTPStatus.OK, response.status_code)

    @patch(
        "recidiviz.application_data_import.server.import_gcs_csv_to_cloud_sql",
        autospec=True,
    )
    def test_import_insights_successful_district_manager_skip(
        self,
        mock_import_csv: MagicMock,
    ) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                "/import/insights/US_MI/supervision_district_managers.json",
            )
            mock_import_csv.assert_not_called()

            self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_import_insights_invalid_state(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                f"/import/insights/US_ABC/{self.view}.json",
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Unknown state_code [US_ABC] received, must be a valid state code.",
                response.data,
            )

    def test_import_insights_invalid_file(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                f"/import/insights/{self.state_code}/unknown_file.json",
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Invalid filename unknown_file.json, must match a Outliers view",
                response.data,
            )

    @patch("recidiviz.application_data_import.server.get_database_entity_by_table_name")
    def test_import_insights_invalid_table(self, mock_get_database: MagicMock) -> None:
        error_message = f"Could not find model with table named {self.view}"
        with self.app.test_request_context():
            mock_get_database.side_effect = ValueError(error_message)
            response = self.client.post(
                f"/import/insights/{self.state_code}/{self.view}.json",
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                error_message.encode("UTF-8"),
                response.data,
            )
