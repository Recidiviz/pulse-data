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
from flask import Flask
from freezegun import freeze_time
from mock import Mock

from recidiviz.admin_panel.all_routes import admin_panel_blueprint
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
        app.register_blueprint(admin_panel_blueprint)
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
        add_ingest_ops_routes(admin_panel_blueprint)

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
                    timestamp=timestamp,
                ),
                DirectIngestInstance.SECONDARY: DirectIngestInstanceStatus(
                    region_code=StateCode.US_XX.value,
                    instance=DirectIngestInstance.SECONDARY,
                    status=DirectIngestStatus.UP_TO_DATE,
                    timestamp=timestamp,
                ),
            },
            StateCode.US_YY: {
                DirectIngestInstance.PRIMARY: DirectIngestInstanceStatus(
                    region_code=StateCode.US_YY.value,
                    instance=DirectIngestInstance.PRIMARY,
                    status=DirectIngestStatus.STANDARD_RERUN_STARTED,
                    timestamp=timestamp,
                ),
                DirectIngestInstance.SECONDARY: DirectIngestInstanceStatus(
                    region_code=StateCode.US_YY.value,
                    instance=DirectIngestInstance.SECONDARY,
                    status=DirectIngestStatus.FLASH_IN_PROGRESS,
                    timestamp=timestamp,
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
                        "timestamp": "2022-08-29T00:00:00+00:00",
                    },
                    "secondary": {
                        "instance": "SECONDARY",
                        "regionCode": "US_XX",
                        "status": "UP_TO_DATE",
                        "timestamp": "2022-08-29T00:00:00+00:00",
                    },
                },
                "US_YY": {
                    "primary": {
                        "instance": "PRIMARY",
                        "regionCode": "US_YY",
                        "status": "STANDARD_RERUN_STARTED",
                        "timestamp": "2022-08-29T00:00:00+00:00",
                    },
                    "secondary": {
                        "instance": "SECONDARY",
                        "regionCode": "US_YY",
                        "status": "FLASH_IN_PROGRESS",
                        "timestamp": "2022-08-29T00:00:00+00:00",
                    },
                },
            },
        )
        self.assertEqual(200, response.status_code)
