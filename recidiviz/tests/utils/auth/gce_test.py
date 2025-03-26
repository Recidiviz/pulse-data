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
"""Tests for utils/gce.py."""
from typing import Callable, Tuple

from flask import Flask
from mock import Mock, patch

from recidiviz.utils.auth import gce

BEST_ALBUM = "Cavalcade of Glee"
APP_ID = "recidiviz-auth-test"


class TestAuthenticateRequest:
    """Tests for the Compute Engine auth decorator."""

    def setup_method(self, _test_method: Callable) -> None:
        """Setup that runs before each test."""
        self.project_id_patcher = patch(
            "recidiviz.utils.metadata.project_id", Mock(return_value="test-project")
        )
        self.project_id_patcher.start()

        self.project_number_patcher = patch(
            "recidiviz.utils.metadata.project_number", Mock(return_value="123456789")
        )
        self.project_number_patcher.start()

        self.get_secret_patcher = patch(
            "recidiviz.utils.auth.gce.get_secret", Mock(return_value="987654321")
        )
        self.get_secret_patcher.start()

        self.validate_iap_jwt_patcher = patch(
            "recidiviz.utils.validate_jwt.validate_iap_jwt_from_compute_engine"
        )
        self.mock_validate_iap_jwt = self.validate_iap_jwt_patcher.start()

        dummy_app = Flask(__name__)

        requires_authorization = gce.build_compute_engine_auth_decorator("test_secret")

        @dummy_app.route("/")
        @requires_authorization
        def venetian_snares_holder() -> Tuple[str, int]:
            return BEST_ALBUM, 200

        dummy_app.config["TESTING"] = True
        self.client = dummy_app.test_client()

    def teardown_method(self, _test_method: Callable) -> None:
        self.project_id_patcher.stop()
        self.project_number_patcher.stop()
        self.get_secret_patcher.stop()
        self.validate_iap_jwt_patcher.stop()

    def test_authenticate_request_from_iap(self) -> None:
        self.mock_validate_iap_jwt.return_value = ("user", "email", None)

        response = self.client.get("/", headers={"x-goog-iap-jwt-assertion": "0"})
        assert response.status_code == 200
        assert response.get_data().decode() == BEST_ALBUM

    def test_authenticate_request_from_iap_invalid(self) -> None:
        self.mock_validate_iap_jwt.return_value = (None, None, "INVALID TOKEN")

        response = self.client.get("/", headers={"x-goog-iap-jwt-assertion": "0"})
        assert response.status_code == 401
        assert response.get_data().decode() == "Error: INVALID TOKEN"

    def test_authenticate_request_unauthorized(self) -> None:
        response = self.client.get("/")
        assert response.status_code == 401
        assert response.get_data().decode() == "Failed: Unauthorized external request."
