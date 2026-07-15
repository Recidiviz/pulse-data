# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Unit tests for the call_identity_service_trigger_import Airflow task."""
import os
import unittest
from http import HTTPStatus
from unittest.mock import MagicMock, patch

from recidiviz.airflow.dags.identity_ingest.call_identity_service_trigger_import import (
    call_identity_service_trigger_import,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING

_MODULE = "recidiviz.airflow.dags.identity_ingest.call_identity_service_trigger_import"

_FAKE_IAP_CLIENT_ID = "fake-iap-client-id"
_FAKE_ID_TOKEN = "fake-id-token"  # nosec B105
_TENANT = "US_XX"


@patch.dict(os.environ, {"GCP_PROJECT": GCP_PROJECT_STAGING})
class TestCallIdentityServiceTriggerImport(unittest.TestCase):
    """Tests for call_identity_service_trigger_import."""

    def setUp(self) -> None:
        self.get_secret_patcher = patch(
            f"{_MODULE}.get_secret", return_value=_FAKE_IAP_CLIENT_ID
        )
        self.mock_get_secret = self.get_secret_patcher.start()

        self.fetch_id_token_patcher = patch(
            f"{_MODULE}.fetch_id_token", return_value=_FAKE_ID_TOKEN
        )
        self.mock_fetch_id_token = self.fetch_id_token_patcher.start()

        self.requests_post_patcher = patch(f"{_MODULE}.requests.post")
        self.mock_post = self.requests_post_patcher.start()

    def tearDown(self) -> None:
        self.get_secret_patcher.stop()
        self.fetch_id_token_patcher.stop()
        self.requests_post_patcher.stop()

    def _mock_response(self, status_code: HTTPStatus) -> MagicMock:
        response = MagicMock()
        response.status_code = status_code
        response.text = "response body"
        return response

    def test_202_treated_as_success(self) -> None:
        self.mock_post.return_value = self._mock_response(HTTPStatus.ACCEPTED)

        call_identity_service_trigger_import(tenant=_TENANT)

        self.mock_post.assert_called_once_with(
            "https://identity-service-staging.recidiviz.org/trigger_import",
            json={"tenant": _TENANT},
            headers={"Authorization": f"Bearer {_FAKE_ID_TOKEN}"},
            timeout=60,
        )

    def test_id_token_fetched_with_iap_client_id_audience(self) -> None:
        self.mock_post.return_value = self._mock_response(HTTPStatus.ACCEPTED)

        call_identity_service_trigger_import(tenant=_TENANT)

        self.mock_get_secret.assert_called_once_with(
            "iap_client_id", GCP_PROJECT_STAGING
        )
        _, kwargs = self.mock_fetch_id_token.call_args
        self.assertEqual(_FAKE_IAP_CLIENT_ID, kwargs["audience"])

    def test_non_202_raises(self) -> None:
        for status_code in [
            HTTPStatus.OK,
            HTTPStatus.BAD_REQUEST,
            HTTPStatus.UNAUTHORIZED,
            HTTPStatus.FORBIDDEN,
            HTTPStatus.INTERNAL_SERVER_ERROR,
        ]:
            with self.subTest(status_code=status_code):
                self.mock_post.return_value = self._mock_response(status_code)

                with self.assertRaisesRegex(
                    ValueError,
                    r"^Identity Service trigger_import request for tenant \[US_XX\] "
                    rf"failed with status \[{status_code.value}\]: \[response body\]$",
                ):
                    call_identity_service_trigger_import(tenant=_TENANT)

    def test_missing_iap_client_id_secret_raises(self) -> None:
        self.mock_get_secret.return_value = None

        with self.assertRaisesRegex(
            ValueError,
            r"^Could not find secret \[iap_client_id\] in project "
            r"\[recidiviz-staging\]$",
        ):
            call_identity_service_trigger_import(tenant=_TENANT)
        self.mock_post.assert_not_called()

    def test_unknown_project_raises(self) -> None:
        with patch.dict(os.environ, {"GCP_PROJECT": "some-other-project"}):
            with self.assertRaisesRegex(
                ValueError,
                r"^No Identity Service domain configured for project "
                r"\[some-other-project\] in \[.*service_domains\.yaml\]$",
            ):
                call_identity_service_trigger_import(tenant=_TENANT)
        self.mock_post.assert_not_called()
