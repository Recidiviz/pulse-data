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


from unittest import TestCase, mock

from flask import Flask
from mock import Mock

from recidiviz.admin_panel.all_routes import admin_panel_blueprint
from recidiviz.admin_panel.routes.ingest_ops import add_ingest_ops_routes
from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


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

    def test_all_different_statuses(self) -> None:
        # Arrange

        self.mock_current_ingest_statuses.return_value = {
            StateCode.US_XX: {
                DirectIngestInstance.PRIMARY: DirectIngestStatus.READY_TO_FLASH,
                DirectIngestInstance.SECONDARY: DirectIngestStatus.UP_TO_DATE,
            },
            StateCode.US_YY: {
                DirectIngestInstance.PRIMARY: DirectIngestStatus.STANDARD_RERUN_STARTED,
                DirectIngestInstance.SECONDARY: DirectIngestStatus.FLASH_IN_PROGRESS,
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
                "US_XX": {"PRIMARY": "READY_TO_FLASH", "SECONDARY": "UP_TO_DATE"},
                "US_YY": {
                    "PRIMARY": "STANDARD_RERUN_STARTED",
                    "SECONDARY": "FLASH_IN_PROGRESS",
                },
            },
        )
        self.assertEqual(200, response.status_code)

    def test_status_none(self) -> None:
        # Arrange
        self.mock_current_ingest_statuses.return_value = {
            StateCode.US_YY: {
                DirectIngestInstance.PRIMARY: None,
                DirectIngestInstance.SECONDARY: None,
            }
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
                "US_YY": {
                    "PRIMARY": None,
                    "SECONDARY": None,
                }
            },
        )
        self.assertEqual(200, response.status_code)
