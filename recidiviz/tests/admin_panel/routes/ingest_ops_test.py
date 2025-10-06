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
"""Tests for return all the ingest statuses to the frontend"""


from datetime import datetime
from unittest import TestCase, mock
from unittest.mock import call

import pytz
from flask import Blueprint, Flask
from mock import Mock, patch

from recidiviz.admin_panel.ingest_dataflow_operations import (
    DataflowPipelineMetadataResponse,
)
from recidiviz.admin_panel.routes.ingest_ops import add_ingest_ops_routes
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataLockActor,
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatusBucket,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileImportSummary,
    LatestDirectIngestRawFileImportRunSummary,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestRawDataResourceLock,
)
from recidiviz.tests.ingest.direct import fake_regions


@mock.patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-456"))
@mock.patch("recidiviz.utils.metadata.project_number", Mock(return_value="123456789"))
class IngestOpsEndpointTests(TestCase):
    """TestCase for returning all the ingest statuses to the frontend"""

    def setUp(self) -> None:
        app = Flask(__name__)
        blueprint = Blueprint("admin_panel_blueprint_test", __name__)
        app.config["TESTING"] = True
        self.client = app.test_client()

        self.get_admin_store_patcher = mock.patch(
            "recidiviz.admin_panel.routes.ingest_ops.get_ingest_operations_store"
        )
        self.mock_store = self.get_admin_store_patcher.start().return_value
        self.mock_current_jobs_statuses = mock.Mock()
        self.mock_store.get_most_recent_dataflow_job_statuses = (
            self.mock_current_jobs_statuses
        )
        add_ingest_ops_routes(blueprint)
        app.register_blueprint(blueprint)

    def tearDown(self) -> None:
        self.get_admin_store_patcher.stop()

    def test_all_dataflow_jobs(self) -> None:
        mock_response = {
            StateCode.US_XX: DataflowPipelineMetadataResponse(
                id="1234",
                project_id="recidiviz-456",
                name="us-xx-ingest",
                create_time=1695821110,
                start_time=1695821110,
                termination_time=1695821110,
                termination_state="JOB_STATE_DONE",
                location="us-west1",
            ),
            StateCode.US_YY: DataflowPipelineMetadataResponse(
                id="1236",
                project_id="recidiviz-456",
                name="us-yy-ingest",
                create_time=1695821110,
                start_time=1695821110,
                termination_time=1695821110,
                termination_state="JOB_STATE_DONE",
                location="us-west1",
            ),
        }

        self.mock_current_jobs_statuses.return_value = mock_response
        response = self.client.get(
            "/api/ingest_operations/get_all_latest_ingest_dataflow_jobs",
        )

        self.assertEqual(
            response.json,
            {
                "US_XX": {
                    "id": "1234",
                    "projectId": "recidiviz-456",
                    "name": "us-xx-ingest",
                    "createTime": 1695821110,
                    "startTime": 1695821110,
                    "terminationTime": 1695821110,
                    "terminationState": "JOB_STATE_DONE",
                    "location": "us-west1",
                    "duration": 0,
                },
                "US_YY": {
                    "id": "1236",
                    "projectId": "recidiviz-456",
                    "name": "us-yy-ingest",
                    "createTime": 1695821110,
                    "startTime": 1695821110,
                    "terminationTime": 1695821110,
                    "terminationState": "JOB_STATE_DONE",
                    "location": "us-west1",
                    "duration": 0,
                },
            },
        )

    def test_get_latest_ingest_dataflow_job(self) -> None:
        mock_response = {
            StateCode.US_XX: DataflowPipelineMetadataResponse(
                id="1234",
                project_id="recidiviz-456",
                name="us-xx-ingest",
                create_time=1695821110,
                start_time=1695821110,
                termination_time=1695821110,
                termination_state="JOB_STATE_DONE",
                location="us-west1",
            )
        }

        self.mock_current_jobs_statuses.return_value = mock_response

        response = self.client.get(
            "/api/ingest_operations/get_latest_ingest_dataflow_job/US_XX",
        )

        self.assertEqual(
            response.json,
            {
                "id": "1234",
                "projectId": "recidiviz-456",
                "name": "us-xx-ingest",
                "createTime": 1695821110,
                "startTime": 1695821110,
                "terminationTime": 1695821110,
                "terminationState": "JOB_STATE_DONE",
                "location": "us-west1",
                "duration": 0,
            },
        )

    @patch("recidiviz.admin_panel.routes.ingest_ops.get_latest_run_raw_data_watermarks")
    def test_get_latest_ingest_dataflow_raw_data_watermarks(
        self, mock_watermarks: mock.MagicMock
    ) -> None:
        # Arrange
        mock_watermarks.return_value = {
            "foo_tag": datetime(2020, 1, 1, 0, 0, 0),
            "bar_tag": datetime(2023, 9, 29, 3, 50, 47),
        }

        # Act
        response = self.client.get(
            "/api/ingest_operations/get_latest_ingest_dataflow_raw_data_watermarks/US_XX/PRIMARY",
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json,
            {"foo_tag": "2020-01-01T00:00:00", "bar_tag": "2023-09-29T03:50:47"},
        )

    @patch("recidiviz.admin_panel.routes.ingest_ops.get_latest_run_ingest_view_results")
    def test_get_latest_run_ingest_view_results(
        self, mock_ingest_view_counts: mock.MagicMock
    ) -> None:
        # Arrange
        mock_ingest_view_counts.return_value = {"foo_tag": 127, "bar_tag": 0}

        # Act
        response = self.client.get(
            "/api/ingest_operations/get_latest_run_ingest_view_results/US_XX/PRIMARY",
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, {"foo_tag": 127, "bar_tag": 0})

    @patch("recidiviz.admin_panel.routes.ingest_ops.get_latest_run_state_results")
    def test_get_latest_run_state_results(
        self, mock_state_counts: mock.MagicMock
    ) -> None:
        # Arrange
        mock_state_counts.return_value = {"state_person": 2341234, "state_staff": 1923}

        # Act
        response = self.client.get(
            "/api/ingest_operations/get_latest_run_state_results/US_XX/PRIMARY",
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, {"state_person": 2341234, "state_staff": 1923})

    def test_all_latest_raw_data_import_run_info(self) -> None:
        # Arrange
        self.mock_store.get_all_latest_raw_data_import_run_info.return_value = {
            StateCode.US_XX: LatestDirectIngestRawFileImportRunSummary(
                import_run_start=datetime(2022, 8, 29, tzinfo=pytz.UTC),
                count_by_status_bucket={
                    DirectIngestRawFileImportStatusBucket.SUCCEEDED: 10,
                    DirectIngestRawFileImportStatusBucket.FAILED: 5,
                },
            ),
            StateCode.US_YY: LatestDirectIngestRawFileImportRunSummary(
                import_run_start=None,
                count_by_status_bucket={},
            ),
        }

        # Act
        response = self.client.get(
            "/api/ingest_operations/all_latest_raw_data_import_run_info",
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json,
            {
                "US_XX": {
                    "importRunStart": "2022-08-29T00:00:00+00:00",
                    "countByStatusBucket": [
                        {"importStatus": "SUCCEEDED", "fileCount": 10},
                        {"importStatus": "FAILED", "fileCount": 5},
                    ],
                },
                "US_YY": {
                    "importRunStart": None,
                    "countByStatusBucket": [],
                },
            },
        )

    def test_all_current_lock_summaries(self) -> None:
        # Arrange
        self.mock_store.get_all_current_lock_summaries.return_value = {
            StateCode.US_XX: {
                DirectIngestRawDataResourceLockResource.BIG_QUERY_RAW_DATA_DATASET: None,
                DirectIngestRawDataResourceLockResource.BUCKET: None,
                DirectIngestRawDataResourceLockResource.OPERATIONS_DATABASE: None,
            },
            StateCode.US_YY: {
                DirectIngestRawDataResourceLockResource.BIG_QUERY_RAW_DATA_DATASET: DirectIngestRawDataLockActor.PROCESS,
                DirectIngestRawDataResourceLockResource.BUCKET: DirectIngestRawDataLockActor.PROCESS,
                DirectIngestRawDataResourceLockResource.OPERATIONS_DATABASE: DirectIngestRawDataLockActor.PROCESS,
            },
        }

        # Act
        response = self.client.get(
            "/api/ingest_operations/all_current_lock_summaries",
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json,
            {
                "US_XX": {
                    "BUCKET": None,
                    "OPERATIONS_DATABASE": None,
                    "BIG_QUERY_RAW_DATA_DATASET": None,
                },
                "US_YY": {
                    "BUCKET": "PROCESS",
                    "OPERATIONS_DATABASE": "PROCESS",
                    "BIG_QUERY_RAW_DATA_DATASET": "PROCESS",
                },
            },
        )

    @patch("recidiviz.admin_panel.routes.ingest_ops.DirectIngestRawFileImportManager")
    def test_get_latest_raw_data_import_runs(
        self, import_manager_mock: mock.MagicMock
    ) -> None:
        # Arrange
        import_manager_mock().get_n_most_recent_imports_for_file_tag.return_value = [
            DirectIngestRawFileImportSummary(
                import_run_id=1,
                file_id=1,
                dag_run_id="run1",
                import_status="FAILED_UNKNOWN",
                update_datetime=datetime(2022, 8, 29, tzinfo=pytz.UTC),
                import_run_start=datetime(2022, 8, 29, tzinfo=pytz.UTC),
                historical_diffs_active=False,
                raw_rows=0,
                net_new_or_updated_rows=None,
                deleted_rows=None,
                is_invalidated=False,
            ),
            DirectIngestRawFileImportSummary(
                import_run_id=2,
                file_id=2,
                dag_run_id="run2",
                import_status="SUCCEEDED",
                update_datetime=datetime(2022, 8, 30, tzinfo=pytz.UTC),
                import_run_start=datetime(2022, 8, 30, tzinfo=pytz.UTC),
                historical_diffs_active=False,
                raw_rows=10,
                net_new_or_updated_rows=None,
                deleted_rows=None,
                is_invalidated=False,
            ),
            DirectIngestRawFileImportSummary(
                import_run_id=2,
                file_id=3,
                dag_run_id="run2",
                import_status="SUCCEEDED",
                update_datetime=datetime(2022, 8, 30, tzinfo=pytz.UTC),
                import_run_start=datetime(2022, 8, 30, tzinfo=pytz.UTC),
                historical_diffs_active=True,
                raw_rows=10,
                net_new_or_updated_rows=5,
                deleted_rows=2,
                is_invalidated=False,
            ),
        ]

        # Act
        response = self.client.get(
            "/api/ingest_operations/get_latest_raw_data_imports/US_XX/SECONDARY/fake_tag",
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json,
            [
                {
                    "importRunId": 1,
                    "dagRunId": "run1",
                    "fileId": 1,
                    "historicalDiffsActive": False,
                    "importRunStart": "2022-08-29T00:00:00+00:00",
                    "importStatus": "FAILED_UNKNOWN",
                    "importStatusDescription": "The FAILED_UNKNOWN status is a catch-all for an import failing without the import DAG identifying what the specific issue is.",
                    "isInvalidated": False,
                    "rawRowCount": 0,
                    "netNewOrUpdatedRows": None,
                    "deletedRows": None,
                    "updateDatetime": "2022-08-29T00:00:00+00:00",
                },
                {
                    "importRunId": 2,
                    "dagRunId": "run2",
                    "fileId": 2,
                    "historicalDiffsActive": False,
                    "importRunStart": "2022-08-30T00:00:00+00:00",
                    "importStatus": "SUCCEEDED",
                    "importStatusDescription": "The SUCCEEDED status means that the an import has completed successfully. This means that new raw data is in the relevant BigQuery table, the import session table accurately reflects both the number of rows in the raw file as well as the rows added to the updated table and the raw data files associated with the file_id have been moved to storage.",
                    "isInvalidated": False,
                    "rawRowCount": 10,
                    "netNewOrUpdatedRows": None,
                    "deletedRows": None,
                    "updateDatetime": "2022-08-30T00:00:00+00:00",
                },
                {
                    "importRunId": 2,
                    "dagRunId": "run2",
                    "fileId": 3,
                    "historicalDiffsActive": True,
                    "importRunStart": "2022-08-30T00:00:00+00:00",
                    "importStatus": "SUCCEEDED",
                    "importStatusDescription": "The SUCCEEDED status means that the an import has completed successfully. This means that new raw data is in the relevant BigQuery table, the import session table accurately reflects both the number of rows in the raw file as well as the rows added to the updated table and the raw data files associated with the file_id have been moved to storage.",
                    "isInvalidated": False,
                    "rawRowCount": 10,
                    "netNewOrUpdatedRows": 5,
                    "deletedRows": 2,
                    "updateDatetime": "2022-08-30T00:00:00+00:00",
                },
            ],
        )

    def test_get_raw_file_config_for_file_tag(self) -> None:
        # Arrange
        self.mock_store.get_raw_file_config.return_value = (
            DirectIngestRegionRawFileConfig(
                region_code=StateCode.US_XX.value,
                region_module=fake_regions,
            ).raw_file_configs["file_tag_first"]
        )
        # Act
        response = self.client.get(
            "/api/ingest_operations/raw_file_config/US_XX/basic",
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json,
            {
                "encoding": "ISO-456-7",
                "exportLookbackWindow": "Two Week Incremental Lookback",
                "fileDescription": "First raw file.",
                "fileTag": "file_tag_first",
                "inferColumns": False,
                "manuallyPruned": False,
                "lineTerminator": "‡\n",
                "isChunkedFile": False,
                "isCodeFile": False,
                "separator": ",",
                "updateCadence": "Weekly",
            },
        )

    @patch(
        "recidiviz.admin_panel.routes.ingest_ops.DirectIngestRawDataFlashStatusManager"
    )
    def test_get_flash_status(self, manager_mock: mock.MagicMock) -> None:
        # Arrange
        manager_mock().is_flashing_in_progress.return_value = False
        # Act
        response = self.client.get(
            "/api/ingest_operations/is_flashing_in_progress/US_XX",
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, False)

    @patch(
        "recidiviz.admin_panel.routes.ingest_ops.DirectIngestRawDataFlashStatusManager"
    )
    def test_set_flash_status(self, manager_mock: mock.MagicMock) -> None:
        # Act
        response = self.client.post(
            "/api/ingest_operations/is_flashing_in_progress/update",
            json={
                "stateCode": "US_XX",
                "isFlashing": True,
            },
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, None)
        manager_mock().set_flashing_started.assert_called_once()
        manager_mock().set_flashing_finished.assert_not_called()

        response = self.client.post(
            "/api/ingest_operations/is_flashing_in_progress/update",
            json={
                "stateCode": "US_XX",
                "isFlashing": False,
            },
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, None)
        manager_mock().set_flashing_started.assert_called_once()
        manager_mock().set_flashing_finished.assert_called_once()

    @patch("recidiviz.admin_panel.routes.ingest_ops.DirectIngestRawFileMetadataManager")
    def test_get_stale_secondary(self, manager_mock: mock.MagicMock) -> None:
        # Arrange
        manager_mock().stale_secondary_raw_data.return_value = ["path_a", "path_b"]
        response = self.client.get(
            "/api/ingest_operations/stale_secondary/US_XX",
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, ["path_a", "path_b"])

    @patch(
        "recidiviz.admin_panel.routes.ingest_ops.DirectIngestRawDataResourceLockManager"
    )
    def test_acquire_resource_locks(self, manager_mock: mock.MagicMock) -> None:
        # Arrange
        manager_mock().acquire_all_locks.return_value = []
        response = self.client.post(
            "/api/ingest_operations/resource_locks/acquire_all",
            json={
                "stateCode": "US_XX",
                "rawDataInstance": "PRIMARY",
                "description": "test",
                "ttlSeconds": 123,
            },
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, None)

    @patch(
        "recidiviz.admin_panel.routes.ingest_ops.DirectIngestRawDataResourceLockManager"
    )
    def test_release_all_locks_for_state(self, manager_mock: mock.MagicMock) -> None:
        # Arrange
        response = self.client.post(
            "/api/ingest_operations/resource_locks/release_all",
            json={
                "stateCode": "US_XX",
                "rawDataInstance": "PRIMARY",
                "lockIds": [1, 2, 3],
            },
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, None)
        manager_mock().release_lock_by_id.assert_has_calls([call(1), call(2), call(3)])

    @patch(
        "recidiviz.admin_panel.routes.ingest_ops.DirectIngestRawDataResourceLockManager"
    )
    def test_list_all_resource_locks(self, manager_mock: mock.MagicMock) -> None:
        # Arrange
        manager_mock().get_most_recent_locks_for_all_resources.return_value = [
            DirectIngestRawDataResourceLock.new_with_defaults(
                lock_id=1,
                lock_actor=DirectIngestRawDataLockActor.ADHOC,
                lock_resource=DirectIngestRawDataResourceLockResource.BUCKET,
                region_code="US_XX",
                raw_data_source_instance=DirectIngestInstance.PRIMARY,
                lock_acquisition_time=datetime(2022, 8, 29, tzinfo=pytz.UTC),
                released=False,
                lock_description="testing!",
                lock_ttl_seconds=123,
            )
        ]
        response = self.client.get(
            "/api/ingest_operations/resource_locks/list_all/US_XX/PRIMARY",
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json,
            [
                {
                    "lockId": 1,
                    "rawDataInstance": "PRIMARY",
                    "lockAcquisitionTime": "2022-08-29T00:00:00+00:00",
                    "ttlSeconds": 123,
                    "description": "testing!",
                    "actor": "ADHOC",
                    "resource": "BUCKET",
                    "released": False,
                }
            ],
        )

    @patch("recidiviz.admin_panel.routes.ingest_ops.DirectIngestRawFileMetadataManager")
    def test_mark_instance_raw_data_invalidated(
        self, manager_mock: mock.MagicMock
    ) -> None:
        # Arrange
        response = self.client.post(
            "/api/ingest_operations/flash_primary_db/mark_instance_raw_data_invalidated",
            json={
                "stateCode": "US_XX",
                "rawDataInstance": "PRIMARY",
            },
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, None)
        manager_mock().mark_instance_data_invalidated.assert_has_calls([call()])

    @patch("recidiviz.admin_panel.routes.ingest_ops.SessionFactory")
    @patch("recidiviz.admin_panel.routes.ingest_ops.DirectIngestRawFileImportManager")
    @patch("recidiviz.admin_panel.routes.ingest_ops.DirectIngestRawFileMetadataManager")
    def test_transfer_raw_data_metadata_to_new_instance(
        self,
        file_manager_mock: mock.MagicMock,
        import_manager_mock: mock.MagicMock,
        _session_mock: mock.MagicMock,
    ) -> None:
        # Arrange
        response = self.client.post(
            "/api/ingest_operations/flash_primary_db/transfer_raw_data_metadata_to_new_instance",
            json={
                "stateCode": "US_XX",
                "destIngestInstance": "SECONDARY",
                "srcIngestInstance": "PRIMARY",
            },
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, None)
        file_manager_mock().transfer_metadata_to_new_instance.assert_called_once()
        import_manager_mock().transfer_metadata_to_new_instance.assert_called_once()

    @patch("recidiviz.admin_panel.routes.ingest_ops.trigger_raw_data_import_dag")
    def test_trigger_raw_data_dag(
        self,
        pubsub_mock: mock.MagicMock,
    ) -> None:
        # Arrange
        response = self.client.post(
            "/api/ingest_operations/trigger_raw_data_import_dag",
            json={
                "stateCode": "US_XX",
                "rawDataInstance": "SECONDARY",
            },
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, None)
        pubsub_mock.assert_has_calls(
            [
                call(
                    raw_data_instance=DirectIngestInstance.SECONDARY,
                    state_code_filter=StateCode.US_XX,
                )
            ]
        )

        with self.assertRaisesRegex(KeyError, "'stateCode'"):
            self.client.post(
                "/api/ingest_operations/flash_primary_db/transfer_raw_data_metadata_to_new_instance",
                json={
                    "rawDataInstance": "SECONDARY",
                },
            )
