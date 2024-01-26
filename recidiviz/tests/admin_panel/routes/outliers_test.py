# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for insights-specific routes in admin_panel/routes/line_staff_tools.py"""


from http import HTTPStatus
from unittest import TestCase
from unittest.mock import MagicMock, patch

from flask import Blueprint, Flask

from recidiviz.admin_panel.routes.line_staff_tools import add_line_staff_tools_routes


class OutliersAdminPanelEndpointTests(TestCase):
    """Tests of our Flask endpoints"""

    def setUp(self) -> None:
        self.app = Flask(__name__)
        blueprint = Blueprint("outliers_test", __name__)
        self.client = self.app.test_client()

        add_line_staff_tools_routes(blueprint)
        self.app.register_blueprint(blueprint)

    @patch(
        "recidiviz.admin_panel.routes.line_staff_tools.get_outliers_enabled_states",
    )
    def test_enabled_states(self, mock_enabled_states: MagicMock) -> None:
        mock_enabled_states.return_value = ["US_MI"]
        response = self.client.get("/api/line_staff_tools/outliers/enabled_state_codes")
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertEqual(response.json, [{"code": "US_MI", "name": "Michigan"}])
