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
"""Tests for the Identity Service Flask app."""
from http import HTTPStatus
from unittest import TestCase
from unittest.mock import patch

from recidiviz.common.constants.identity import IdentifierType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.services.identity.exceptions import IdentityHistoryIntegrityException
from recidiviz.services.identity.server import app
from recidiviz.tests.services.identity.test_utils import (
    DEFAULT_MAPPING,
    MAPPED_SERVICE_ACCOUNT,
    RECIDIVIZ_ID,
    STRANGER_SERVICE_ACCOUNT,
    build_full_identity,
    mock_iap_environment,
)

IAP_HEADERS = {"x-goog-iap-jwt-assertion": "anything"}

QUERIER_PATH = "recidiviz.services.identity.identity_blueprint.IdentityServiceQuerier"


class IdentityServiceServerTest(TestCase):
    """Tests for the Identity Service Flask app, including IAP auth wiring."""

    def setUp(self) -> None:
        self.app = app
        self.client = self.app.test_client()

    def test_health_with_mapped_caller_returns_200(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ):
            response = self.client.get("/health", headers=IAP_HEADERS)
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(b"", response.data)

    def test_health_without_iap_header_returns_401(self) -> None:
        with mock_iap_environment():
            response = self.client.get("/health")
            self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)

    def test_health_with_invalid_jwt_returns_401(self) -> None:
        with mock_iap_environment(invalid_jwt=True):
            response = self.client.get("/health", headers=IAP_HEADERS)
            self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)

    def test_health_with_unmapped_caller_returns_403(self) -> None:
        with mock_iap_environment(authenticated_as=STRANGER_SERVICE_ACCOUNT):
            response = self.client.get("/health", headers=IAP_HEADERS)
            self.assertEqual(HTTPStatus.FORBIDDEN, response.status_code)

    def test_health_in_development_returns_200(self) -> None:
        # In development the JWT check is bypassed and the dev caller default is
        # applied, so a request with no IAP header still succeeds.
        with mock_iap_environment(in_development=True):
            response = self.client.get("/health")
            self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_security_headers_applied(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ):
            response = self.client.get("/health", headers=IAP_HEADERS)
            self.assertEqual("DENY", response.headers["X-Frame-Options"])
            self.assertEqual("nosniff", response.headers["X-Content-Type-Options"])
            self.assertEqual(
                "frame-ancestors 'none'",
                response.headers["Content-Security-Policy"],
            )


class GetIdentityEndpointTest(TestCase):
    """Tests for GET /identity/<recidiviz_id>, with the querier mocked out."""

    def setUp(self) -> None:
        self.client = app.test_client()

    def test_returns_default_form(self) -> None:
        identity = build_full_identity().identity
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier_cls.return_value.get_identity.return_value = identity
            response = self.client.get(f"/identity/{RECIDIVIZ_ID}", headers=IAP_HEADERS)

        self.assertEqual(HTTPStatus.OK, response.status_code)
        body = response.get_json()
        self.assertEqual(str(RECIDIVIZ_ID), body["recidivizId"])
        self.assertEqual("ACTIVE", body["status"])
        self.assertNotIn("mergeEvents", body)
        self.assertNotIn("isActive", body["externalIds"][0])
        self.assertIn("dateOfBirth", body["attributes"])
        self.assertNotIn("datesOfBirth", body["attributes"])
        mock_querier_cls.return_value.get_identity.assert_called_once_with(
            RECIDIVIZ_ID, resolve_retired=True
        )

    def test_returns_full_form(self) -> None:
        identity_history = build_full_identity()
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier = mock_querier_cls.return_value
            mock_querier.get_identity.return_value = identity_history.identity
            mock_querier.get_identity_history.return_value = identity_history
            response = self.client.get(
                f"/identity/{RECIDIVIZ_ID}?full=true", headers=IAP_HEADERS
            )

        self.assertEqual(HTTPStatus.OK, response.status_code)
        body = response.get_json()
        self.assertTrue(body["externalIds"][0]["isActive"])
        self.assertIn("datesOfBirth", body["attributes"])
        self.assertEqual(1, len(body["mergeEvents"]))
        self.assertEqual(1, len(body["splitEvents"]))
        mock_querier.get_identity_history.assert_called_once_with(
            identity_history.identity
        )

    def test_returns_404_when_not_found(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier_cls.return_value.get_identity.return_value = None
            response = self.client.get(f"/identity/{RECIDIVIZ_ID}", headers=IAP_HEADERS)

        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_returns_500_for_corrupt_merge_chain(self) -> None:
        # A broken merged_into chain (an intermediate record's merge target
        # deleted) makes the querier raise IdentityHistoryIntegrityException rather
        # than return None. This must not leak out as an unhandled exception /
        # HTML error page -- it should come back as a structured JSON error.
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier_cls.return_value.get_identity.side_effect = (
                IdentityHistoryIntegrityException(
                    "Identity [11111111-1111-1111-1111-111111111111] referenced "
                    "via merged_into chain from "
                    "[22222222-2222-2222-2222-222222222222] does not exist."
                )
            )
            response = self.client.get(f"/identity/{RECIDIVIZ_ID}", headers=IAP_HEADERS)

        self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)
        body = response.get_json()
        assert body is not None
        self.assertEqual("identity_integrity_error", body["code"])
        self.assertIn("does not exist", body["description"])

    def test_returns_404_for_non_uuid_path(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ):
            response = self.client.get("/identity/not-a-uuid", headers=IAP_HEADERS)

        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_returns_400_for_invalid_full_param(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH):
            response = self.client.get(
                f"/identity/{RECIDIVIZ_ID}?full=banana", headers=IAP_HEADERS
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_403_for_unmapped_caller(self) -> None:
        with mock_iap_environment(authenticated_as=STRANGER_SERVICE_ACCOUNT):
            response = self.client.get(f"/identity/{RECIDIVIZ_ID}", headers=IAP_HEADERS)

        self.assertEqual(HTTPStatus.FORBIDDEN, response.status_code)


class GetIdentityByExternalIdEndpointTest(TestCase):
    """Tests for GET /identity?external_id=X&id_type=Y."""

    def setUp(self) -> None:
        self.client = app.test_client()

    def test_returns_default_form(self) -> None:
        identity = build_full_identity().identity
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier_cls.return_value.get_by_external_id.return_value = identity
            response = self.client.get(
                "/identity?external_id=A123&id_type=US_OZ_LOTR_ID",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.OK, response.status_code)
        body = response.get_json()
        self.assertEqual(str(RECIDIVIZ_ID), body["recidivizId"])
        self.assertEqual("ACTIVE", body["status"])
        self.assertNotIn("mergeEvents", body)
        self.assertNotIn("isActive", body["externalIds"][0])
        mock_querier_cls.return_value.get_by_external_id.assert_called_once_with(
            "A123", IdentifierType.US_OZ_LOTR_ID
        )

    def test_returns_full_form(self) -> None:
        identity_history = build_full_identity()
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier = mock_querier_cls.return_value
            mock_querier.get_by_external_id.return_value = identity_history.identity
            mock_querier.get_identity_history.return_value = identity_history
            response = self.client.get(
                "/identity?external_id=A123&id_type=US_OZ_LOTR_ID&full=true",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.OK, response.status_code)
        body = response.get_json()
        self.assertTrue(body["externalIds"][0]["isActive"])
        self.assertIn("datesOfBirth", body["attributes"])
        self.assertEqual(1, len(body["mergeEvents"]))
        self.assertEqual(1, len(body["splitEvents"]))
        mock_querier.get_identity_history.assert_called_once_with(
            identity_history.identity
        )

    def test_returns_404_when_not_found(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier_cls.return_value.get_by_external_id.return_value = None
            response = self.client.get(
                "/identity?external_id=UNKNOWN&id_type=US_OZ_LOTR_ID",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_returns_400_for_missing_external_id(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ):
            response = self.client.get(
                "/identity?id_type=US_OZ_LOTR_ID",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_400_for_bad_id_type(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ):
            response = self.client.get(
                "/identity?external_id=A123&id_type=NOT_A_REAL_TYPE",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_403_for_unmapped_caller(self) -> None:
        with mock_iap_environment(authenticated_as=STRANGER_SERVICE_ACCOUNT):
            response = self.client.get(
                "/identity?external_id=A123&id_type=US_OZ_LOTR_ID",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.FORBIDDEN, response.status_code)


class GetIdentityByEmailHashEndpointTest(TestCase):
    """Tests for GET /identity?tenant=X&email_hash=Y."""

    def setUp(self) -> None:
        self.client = app.test_client()

    def test_returns_default_form(self) -> None:
        identity = build_full_identity().identity
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier_cls.return_value.get_by_email_hash.return_value = identity
            response = self.client.get(
                "/identity?tenant=US_OZ&email_hash=hashtestfakecom",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.OK, response.status_code)
        body = response.get_json()
        self.assertEqual(str(RECIDIVIZ_ID), body["recidivizId"])
        self.assertEqual("ACTIVE", body["status"])
        self.assertNotIn("mergeEvents", body)
        self.assertNotIn("isActive", body["externalIds"][0])
        mock_querier_cls.return_value.get_by_email_hash.assert_called_once_with(
            "hashtestfakecom", Tenant.US_OZ
        )

    def test_returns_full_form(self) -> None:
        identity_history = build_full_identity()
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier = mock_querier_cls.return_value
            mock_querier.get_by_email_hash.return_value = identity_history.identity
            mock_querier.get_identity_history.return_value = identity_history
            response = self.client.get(
                "/identity?tenant=US_OZ&email_hash=hashtestfakecom&full=true",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.OK, response.status_code)
        body = response.get_json()
        self.assertTrue(body["externalIds"][0]["isActive"])
        self.assertIn("datesOfBirth", body["attributes"])
        self.assertEqual(1, len(body["mergeEvents"]))
        self.assertEqual(1, len(body["splitEvents"]))
        mock_querier.get_identity_history.assert_called_once_with(
            identity_history.identity
        )

    def test_returns_404_when_not_found(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier_cls.return_value.get_by_email_hash.return_value = None
            response = self.client.get(
                "/identity?tenant=US_OZ&email_hash=nosuchhash",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_returns_400_for_missing_tenant(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ):
            response = self.client.get(
                "/identity?email_hash=hashtestfakecom",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_400_for_missing_email_hash(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ):
            response = self.client.get(
                "/identity?tenant=US_OZ",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_400_for_bad_tenant(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ):
            response = self.client.get(
                "/identity?tenant=NOT_A_REAL_TENANT&email_hash=hashtestfakecom",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_400_for_both_modes_provided(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ):
            response = self.client.get(
                "/identity?external_id=A123&id_type=US_OZ_LOTR_ID&tenant=US_OZ&email_hash=hashtestfakecom",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_400_for_no_params(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ):
            response = self.client.get(
                "/identity",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_403_for_unmapped_caller(self) -> None:
        with mock_iap_environment(authenticated_as=STRANGER_SERVICE_ACCOUNT):
            response = self.client.get(
                "/identity?tenant=US_OZ&email_hash=hashtestfakecom",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.FORBIDDEN, response.status_code)


class PostImportEndpointTest(TestCase):
    """Tests for POST /import."""

    def setUp(self) -> None:
        self.client = app.test_client()

    def test_returns_202(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ):
            response = self.client.post(
                "/import", json={"tenant": "US_OZ"}, headers=IAP_HEADERS
            )

        self.assertEqual(HTTPStatus.ACCEPTED, response.status_code)

    def test_returns_400_for_missing_tenant(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ):
            response = self.client.post("/import", json={}, headers=IAP_HEADERS)

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_400_for_bad_tenant(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ):
            response = self.client.post(
                "/import", json={"tenant": "NOT_A_REAL_TENANT"}, headers=IAP_HEADERS
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_401_without_iap_header(self) -> None:
        with mock_iap_environment():
            response = self.client.post("/import", json={"tenant": "US_OZ"})

        self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)

    def test_returns_403_for_unmapped_caller(self) -> None:
        with mock_iap_environment(authenticated_as=STRANGER_SERVICE_ACCOUNT):
            response = self.client.post(
                "/import", json={"tenant": "US_OZ"}, headers=IAP_HEADERS
            )

        self.assertEqual(HTTPStatus.FORBIDDEN, response.status_code)
