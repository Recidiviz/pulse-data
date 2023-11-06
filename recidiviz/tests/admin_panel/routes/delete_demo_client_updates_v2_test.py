# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for admin_panel/routes/line_staff_tools.py"""
from http import HTTPStatus
from unittest import TestCase
from unittest.mock import MagicMock, patch

from flask import Blueprint, Flask

from recidiviz.admin_panel.routes.line_staff_tools import add_line_staff_tools_routes


class AdminPanelDeleteDemoClientUpdatesV2EndpointTests(TestCase):
    """Integration tests of our flask endpoints"""

    def setUp(self) -> None:
        self.app = Flask(__name__)

        blueprint = Blueprint("delete_demo_client_updates_v2_test", __name__)

        self.client = self.app.test_client()

        add_line_staff_tools_routes(blueprint)
        self.app.register_blueprint(blueprint)

    @patch("recidiviz.admin_panel.routes.line_staff_tools.FirestoreClientImpl")
    def test_delete_demo_client_updates_v2_collection(
        self, mock_firestore: MagicMock
    ) -> None:
        response = self.client.delete("/api/line_staff_tools/demo_client_updates_v2")
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_firestore.return_value.delete_collection.assert_called_once_with(
            "DEMO_clientUpdatesV2"
        )

    @patch("recidiviz.admin_panel.routes.line_staff_tools.FirestoreClientImpl")
    def test_delete_demo_client_updates_v2_documents_for_state(
        self, mock_firestore: MagicMock
    ) -> None:
        response = self.client.delete(
            "/api/line_staff_tools/demo_client_updates_v2/us_oz"
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_firestore.return_value.delete_documents_with_state_code.assert_called_once_with(
            "DEMO_clientUpdatesV2", "us_oz"
        )

    @patch("recidiviz.admin_panel.routes.line_staff_tools.in_gcp_production")
    def test_delete_demo_client_updates_v2_fails_in_production(
        self, mock_in_prod: MagicMock
    ) -> None:
        mock_in_prod.return_value = True
        response = self.client.delete("/api/line_staff_tools/demo_client_updates_v2")
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
