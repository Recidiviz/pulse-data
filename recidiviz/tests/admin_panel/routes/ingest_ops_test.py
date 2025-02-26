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

import pytz
from flask import Blueprint, Flask
from freezegun import freeze_time
from mock import Mock, patch

from recidiviz.admin_panel.ingest_dataflow_operations import (
    DataflowPipelineMetadataResponse,
)
from recidiviz.admin_panel.routes.ingest_ops import add_ingest_ops_routes
from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.operations.entities import DirectIngestInstanceStatus


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
        self.mock_current_ingest_statuses = mock.Mock()
        self.mock_store.get_all_current_ingest_instance_statuses = (
            self.mock_current_ingest_statuses
        )
        add_ingest_ops_routes(blueprint)
        app.register_blueprint(blueprint)

    def tearDown(self) -> None:
        self.get_admin_store_patcher.stop()

    def test_succeeds(self) -> None:
        # Arrange

        self.mock_current_ingest_statuses.return_value = {}

        # Act

        response = self.client.get(
            "/api/ingest_operations/all_ingest_instance_statuses",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )

        # Assert
        self.assertEqual(response.json, {})
        self.assertEqual(200, response.status_code)

    @freeze_time(datetime(2022, 8, 29, tzinfo=pytz.UTC))
    def test_all_different_statuses(self) -> None:
        # Arrange
        timestamp = datetime(2022, 8, 29, tzinfo=pytz.UTC)

        self.mock_current_ingest_statuses.return_value = {
            StateCode.US_XX: {
                DirectIngestInstance.PRIMARY: DirectIngestInstanceStatus(
                    region_code=StateCode.US_XX.value,
                    instance=DirectIngestInstance.PRIMARY,
                    status=DirectIngestStatus.READY_TO_FLASH,
                    status_timestamp=timestamp,
                ),
                DirectIngestInstance.SECONDARY: DirectIngestInstanceStatus(
                    region_code=StateCode.US_XX.value,
                    instance=DirectIngestInstance.SECONDARY,
                    status=DirectIngestStatus.UP_TO_DATE,
                    status_timestamp=timestamp,
                ),
            },
            StateCode.US_YY: {
                DirectIngestInstance.PRIMARY: DirectIngestInstanceStatus(
                    region_code=StateCode.US_YY.value,
                    instance=DirectIngestInstance.PRIMARY,
                    status=DirectIngestStatus.STANDARD_RERUN_STARTED,
                    status_timestamp=timestamp,
                ),
                DirectIngestInstance.SECONDARY: DirectIngestInstanceStatus(
                    region_code=StateCode.US_YY.value,
                    instance=DirectIngestInstance.SECONDARY,
                    status=DirectIngestStatus.FLASH_IN_PROGRESS,
                    status_timestamp=timestamp,
                ),
            },
        }
        # Act

        response = self.client.get(
            "/api/ingest_operations/all_ingest_instance_statuses",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )

        # Assert
        self.assertEqual(
            response.json,
            {
                "US_XX": {
                    "primary": {
                        "instance": "PRIMARY",
                        "regionCode": "US_XX",
                        "status": "READY_TO_FLASH",
                        "statusTimestamp": "2022-08-29T00:00:00+00:00",
                    },
                    "secondary": {
                        "instance": "SECONDARY",
                        "regionCode": "US_XX",
                        "status": "UP_TO_DATE",
                        "statusTimestamp": "2022-08-29T00:00:00+00:00",
                    },
                },
                "US_YY": {
                    "primary": {
                        "instance": "PRIMARY",
                        "regionCode": "US_YY",
                        "status": "STANDARD_RERUN_STARTED",
                        "statusTimestamp": "2022-08-29T00:00:00+00:00",
                    },
                    "secondary": {
                        "instance": "SECONDARY",
                        "regionCode": "US_YY",
                        "status": "FLASH_IN_PROGRESS",
                        "statusTimestamp": "2022-08-29T00:00:00+00:00",
                    },
                },
            },
        )
        self.assertEqual(200, response.status_code)

    def test_all_dataflow_jobs(self) -> None:
        mock_response = {
            StateCode.US_XX: {
                DirectIngestInstance.PRIMARY: DataflowPipelineMetadataResponse(
                    id="1234",
                    project_id="recidiviz-456",
                    name="us-xx-ingest",
                    create_time=1695821110,
                    start_time=1695821110,
                    termination_time=1695821110,
                    termination_state="JOB_STATE_DONE",
                    location="us-west1",
                ),
                DirectIngestInstance.SECONDARY: None,
            },
            StateCode.US_YY: {
                DirectIngestInstance.PRIMARY: DataflowPipelineMetadataResponse(
                    id="1236",
                    project_id="recidiviz-456",
                    name="us-yy-ingest",
                    create_time=1695821110,
                    start_time=1695821110,
                    termination_time=1695821110,
                    termination_state="JOB_STATE_DONE",
                    location="us-west1",
                ),
                DirectIngestInstance.SECONDARY: DataflowPipelineMetadataResponse(
                    id="1237",
                    project_id="recidiviz-456",
                    name="us-yy-ingest-secondary",
                    create_time=1695821110,
                    start_time=1695821110,
                    termination_time=1695821110,
                    termination_state="JOB_STATE_DONE",
                    location="us-west1",
                ),
            },
        }

        with mock.patch(
            "recidiviz.admin_panel.routes.ingest_ops.get_all_latest_ingest_jobs",
            return_value=mock_response,
        ):
            response = self.client.get(
                "/api/ingest_operations/get_all_latest_ingest_dataflow_jobs",
                headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            )

            self.assertEqual(
                response.json,
                {
                    "US_XX": {
                        "primary": {
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
                        "secondary": None,
                    },
                    "US_YY": {
                        "primary": {
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
                        "secondary": {
                            "id": "1237",
                            "projectId": "recidiviz-456",
                            "name": "us-yy-ingest-secondary",
                            "createTime": 1695821110,
                            "startTime": 1695821110,
                            "terminationTime": 1695821110,
                            "terminationState": "JOB_STATE_DONE",
                            "location": "us-west1",
                            "duration": 0,
                        },
                    },
                },
            )

    def test_get_latest_ingest_dataflow_job_by_instance(self) -> None:
        mock_response = DataflowPipelineMetadataResponse(
            id="1234",
            project_id="recidiviz-456",
            name="us-xx-ingest",
            create_time=1695821110,
            start_time=1695821110,
            termination_time=1695821110,
            termination_state="JOB_STATE_DONE",
            location="us-west1",
        )

        with mock.patch(
            "recidiviz.admin_panel.routes.ingest_ops.get_latest_job_for_state_instance",
            return_value=mock_response,
        ):
            response = self.client.get(
                "/api/ingest_operations/get_latest_ingest_dataflow_job_by_instance/US_XX/PRIMARY",
                headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
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
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )

        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json,
            {"foo_tag": "2020-01-01T00:00:00", "bar_tag": "2023-09-29T03:50:47"},
        )
