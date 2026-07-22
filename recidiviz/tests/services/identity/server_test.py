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
from recidiviz.services.identity.authorization import CallerRole
from recidiviz.services.identity.constants import (
    DEV_CALLER_EMAIL_HEADER,
    DEV_IMPORTER_SERVICE_ACCOUNT,
    DEV_READER_SERVICE_ACCOUNT,
    IDENTITIES_ROUTE,
    TRIGGER_IMPORT_ROUTE,
)
from recidiviz.services.identity.exceptions import IdentityHistoryIntegrityException
from recidiviz.services.identity.identity_blueprint import identity_blueprint
from recidiviz.services.identity.server import ROLE_EXEMPT_ENDPOINTS, app
from recidiviz.tests.services.identity.test_utils import (
    DEFAULT_MAPPING,
    EDITOR_SERVICE_ACCOUNT,
    IMPORTER_SERVICE_ACCOUNT,
    READER_SERVICE_ACCOUNT,
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

    def test_health_returns_200_without_auth(self) -> None:
        # The Cloud Run startup probe and GCP uptime checks hit /health
        # directly on the container, bypassing the load balancer and IAP, so
        # /health is exempt from authentication and caller authorization.
        with mock_iap_environment():
            response = self.client.get("/health")
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(b"", response.data)

    def test_health_with_mapped_caller_returns_200(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ):
            response = self.client.get("/health", headers=IAP_HEADERS)
            self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_health_with_invalid_jwt_returns_200(self) -> None:
        with mock_iap_environment(invalid_jwt=True):
            response = self.client.get("/health", headers=IAP_HEADERS)
            self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_health_with_unmapped_caller_returns_200(self) -> None:
        with mock_iap_environment(authenticated_as=STRANGER_SERVICE_ACCOUNT):
            response = self.client.get("/health", headers=IAP_HEADERS)
            self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_health_in_development_returns_200(self) -> None:
        with mock_iap_environment(in_development=True):
            response = self.client.get("/health")
            self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_security_headers_applied(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ):
            response = self.client.get("/health", headers=IAP_HEADERS)
            self.assertEqual("DENY", response.headers["X-Frame-Options"])
            self.assertEqual("nosniff", response.headers["X-Content-Type-Options"])
            self.assertEqual(
                "frame-ancestors 'none'",
                response.headers["Content-Security-Policy"],
            )


class GetIdentityEndpointTest(TestCase):
    """Tests for GET /identities/<recidiviz_id>, with the querier mocked out."""

    def setUp(self) -> None:
        self.client = app.test_client()

    def test_returns_default_form(self) -> None:
        identity = build_full_identity().identity
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier_cls.return_value.get_identity.return_value = identity
            response = self.client.get(
                f"{IDENTITIES_ROUTE}/{RECIDIVIZ_ID}", headers=IAP_HEADERS
            )

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
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier = mock_querier_cls.return_value
            mock_querier.get_identity.return_value = identity_history.identity
            mock_querier.get_identity_history.return_value = identity_history
            response = self.client.get(
                f"{IDENTITIES_ROUTE}/{RECIDIVIZ_ID}?full=true", headers=IAP_HEADERS
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
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier_cls.return_value.get_identity.return_value = None
            response = self.client.get(
                f"{IDENTITIES_ROUTE}/{RECIDIVIZ_ID}", headers=IAP_HEADERS
            )

        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_returns_500_for_corrupt_merge_chain(self) -> None:
        # A broken merged_into chain (an intermediate record's merge target
        # deleted) makes the querier raise IdentityHistoryIntegrityException rather
        # than return None. This must not leak out as an unhandled exception /
        # HTML error page -- it should come back as a structured JSON error.
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier_cls.return_value.get_identity.side_effect = (
                IdentityHistoryIntegrityException(
                    "Identity [11111111-1111-1111-1111-111111111111] referenced "
                    "via merged_into chain from "
                    "[22222222-2222-2222-2222-222222222222] does not exist."
                )
            )
            response = self.client.get(
                f"{IDENTITIES_ROUTE}/{RECIDIVIZ_ID}", headers=IAP_HEADERS
            )

        self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)
        body = response.get_json()
        assert body is not None
        self.assertEqual("identity_integrity_error", body["code"])
        self.assertIn("does not exist", body["description"])

    def test_returns_404_for_non_uuid_path(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ):
            response = self.client.get(
                f"{IDENTITIES_ROUTE}/not-a-uuid", headers=IAP_HEADERS
            )

        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_returns_400_for_invalid_full_param(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH):
            response = self.client.get(
                f"{IDENTITIES_ROUTE}/{RECIDIVIZ_ID}?full=banana", headers=IAP_HEADERS
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_403_for_unmapped_caller(self) -> None:
        with mock_iap_environment(authenticated_as=STRANGER_SERVICE_ACCOUNT):
            response = self.client.get(
                f"{IDENTITIES_ROUTE}/{RECIDIVIZ_ID}", headers=IAP_HEADERS
            )

        self.assertEqual(HTTPStatus.FORBIDDEN, response.status_code)


class GetIdentityByExternalIdEndpointTest(TestCase):
    """Tests for GET /identities?external_id=X&id_type=Y."""

    def setUp(self) -> None:
        self.client = app.test_client()

    def test_returns_default_form(self) -> None:
        identity = build_full_identity().identity
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier_cls.return_value.get_by_external_id.return_value = identity
            response = self.client.get(
                f"{IDENTITIES_ROUTE}?external_id=A123&id_type=US_OZ_LOTR_ID",
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
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier = mock_querier_cls.return_value
            mock_querier.get_by_external_id.return_value = identity_history.identity
            mock_querier.get_identity_history.return_value = identity_history
            response = self.client.get(
                f"{IDENTITIES_ROUTE}?external_id=A123&id_type=US_OZ_LOTR_ID&full=true",
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
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier_cls.return_value.get_by_external_id.return_value = None
            response = self.client.get(
                f"{IDENTITIES_ROUTE}?external_id=UNKNOWN&id_type=US_OZ_LOTR_ID",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_returns_400_for_missing_external_id(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ):
            response = self.client.get(
                f"{IDENTITIES_ROUTE}?id_type=US_OZ_LOTR_ID",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_400_for_bad_id_type(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ):
            response = self.client.get(
                f"{IDENTITIES_ROUTE}?external_id=A123&id_type=NOT_A_REAL_TYPE",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_403_for_unmapped_caller(self) -> None:
        with mock_iap_environment(authenticated_as=STRANGER_SERVICE_ACCOUNT):
            response = self.client.get(
                f"{IDENTITIES_ROUTE}?external_id=A123&id_type=US_OZ_LOTR_ID",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.FORBIDDEN, response.status_code)


class GetIdentityByEmailHashEndpointTest(TestCase):
    """Tests for GET /identities?tenant=X&email_hash=Y."""

    def setUp(self) -> None:
        self.client = app.test_client()

    def test_returns_default_form(self) -> None:
        identity = build_full_identity().identity
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier_cls.return_value.get_by_email_hash.return_value = identity
            response = self.client.get(
                f"{IDENTITIES_ROUTE}?tenant=US_OZ&email_hash=hashtestfakecom",
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
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier = mock_querier_cls.return_value
            mock_querier.get_by_email_hash.return_value = identity_history.identity
            mock_querier.get_identity_history.return_value = identity_history
            response = self.client.get(
                f"{IDENTITIES_ROUTE}?tenant=US_OZ&email_hash=hashtestfakecom&full=true",
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
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier_cls.return_value.get_by_email_hash.return_value = None
            response = self.client.get(
                f"{IDENTITIES_ROUTE}?tenant=US_OZ&email_hash=nosuchhash",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_returns_400_for_missing_tenant(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ):
            response = self.client.get(
                f"{IDENTITIES_ROUTE}?email_hash=hashtestfakecom",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_400_for_missing_email_hash(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ):
            response = self.client.get(
                f"{IDENTITIES_ROUTE}?tenant=US_OZ",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_400_for_bad_tenant(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ):
            response = self.client.get(
                f"{IDENTITIES_ROUTE}?tenant=NOT_A_REAL_TENANT&email_hash=hashtestfakecom",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_400_for_both_modes_provided(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ):
            response = self.client.get(
                f"{IDENTITIES_ROUTE}?external_id=A123&id_type=US_OZ_LOTR_ID&tenant=US_OZ&email_hash=hashtestfakecom",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_400_for_no_params(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ):
            response = self.client.get(
                IDENTITIES_ROUTE,
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_403_for_unmapped_caller(self) -> None:
        with mock_iap_environment(authenticated_as=STRANGER_SERVICE_ACCOUNT):
            response = self.client.get(
                f"{IDENTITIES_ROUTE}?tenant=US_OZ&email_hash=hashtestfakecom",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.FORBIDDEN, response.status_code)


class PostImportEndpointTest(TestCase):
    """Tests for POST /trigger_import."""

    def setUp(self) -> None:
        self.client = app.test_client()

    def test_returns_202(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=IMPORTER_SERVICE_ACCOUNT
        ):
            response = self.client.post(
                TRIGGER_IMPORT_ROUTE, json={"tenant": "US_OZ"}, headers=IAP_HEADERS
            )

        self.assertEqual(HTTPStatus.ACCEPTED, response.status_code)

    def test_returns_400_for_missing_tenant(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=IMPORTER_SERVICE_ACCOUNT
        ):
            response = self.client.post(
                TRIGGER_IMPORT_ROUTE, json={}, headers=IAP_HEADERS
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_400_for_bad_tenant(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=IMPORTER_SERVICE_ACCOUNT
        ):
            response = self.client.post(
                TRIGGER_IMPORT_ROUTE,
                json={"tenant": "NOT_A_REAL_TENANT"},
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_returns_401_without_iap_header(self) -> None:
        with mock_iap_environment():
            response = self.client.post(TRIGGER_IMPORT_ROUTE, json={"tenant": "US_OZ"})

        self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)

    def test_returns_403_for_unmapped_caller(self) -> None:
        with mock_iap_environment(authenticated_as=STRANGER_SERVICE_ACCOUNT):
            response = self.client.post(
                TRIGGER_IMPORT_ROUTE, json={"tenant": "US_OZ"}, headers=IAP_HEADERS
            )

        self.assertEqual(HTTPStatus.FORBIDDEN, response.status_code)

    def test_returns_403_for_reader_caller(self) -> None:
        # A reader caller may read identities but not trigger_import.
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=READER_SERVICE_ACCOUNT
        ):
            response = self.client.post(
                TRIGGER_IMPORT_ROUTE, json={"tenant": "US_OZ"}, headers=IAP_HEADERS
            )

        self.assertEqual(HTTPStatus.FORBIDDEN, response.status_code)

    def test_returns_403_for_editor_caller(self) -> None:
        # An editor caller may hit the identity endpoints but not trigger_import.
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ):
            response = self.client.post(
                TRIGGER_IMPORT_ROUTE, json={"tenant": "US_OZ"}, headers=IAP_HEADERS
            )

        self.assertEqual(HTTPStatus.FORBIDDEN, response.status_code)

    def test_dev_caller_header_selects_importer_in_development(self) -> None:
        # In development a local request can pick the dev importer caller via
        # header to exercise trigger_import.
        with mock_iap_environment(in_development=True):
            response = self.client.post(
                TRIGGER_IMPORT_ROUTE,
                json={"tenant": "US_OZ"},
                headers={DEV_CALLER_EMAIL_HEADER: DEV_IMPORTER_SERVICE_ACCOUNT},
            )

        self.assertEqual(HTTPStatus.ACCEPTED, response.status_code)

    def test_dev_caller_header_selects_reader_in_development(self) -> None:
        # In development a local request can pick the dev reader caller via
        # header, which may read identities but not call trigger_import.
        identity = build_full_identity().identity
        reader_headers = {DEV_CALLER_EMAIL_HEADER: DEV_READER_SERVICE_ACCOUNT}
        with mock_iap_environment(in_development=True), patch(
            QUERIER_PATH
        ) as mock_querier_cls:
            mock_querier_cls.return_value.get_identity.return_value = identity
            read_response = self.client.get(
                f"{IDENTITIES_ROUTE}/{RECIDIVIZ_ID}", headers=reader_headers
            )
            import_response = self.client.post(
                TRIGGER_IMPORT_ROUTE,
                json={"tenant": "US_OZ"},
                headers=reader_headers,
            )

        self.assertEqual(HTTPStatus.OK, read_response.status_code)
        self.assertEqual(HTTPStatus.FORBIDDEN, import_response.status_code)

    def test_dev_request_without_caller_header_forbidden(self) -> None:
        # A development request must select its caller via the dev caller
        # header; one that does not is rejected as unknown.
        with mock_iap_environment(in_development=True):
            response = self.client.post(TRIGGER_IMPORT_ROUTE, json={"tenant": "US_OZ"})

        self.assertEqual(HTTPStatus.FORBIDDEN, response.status_code)

    def test_dev_caller_header_ignored_outside_development(self) -> None:
        # Outside development the caller comes from the validated IAP JWT; the
        # dev caller header must not let a caller escalate its role.
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=EDITOR_SERVICE_ACCOUNT
        ):
            response = self.client.post(
                TRIGGER_IMPORT_ROUTE,
                json={"tenant": "US_OZ"},
                headers=IAP_HEADERS
                | {DEV_CALLER_EMAIL_HEADER: IMPORTER_SERVICE_ACCOUNT},
            )

        self.assertEqual(HTTPStatus.FORBIDDEN, response.status_code)


class EndpointAuthorizationTest(TestCase):
    """Tests that endpoints enforce their per-caller role authorization."""

    def setUp(self) -> None:
        self.client = app.test_client()

    def test_importer_caller_forbidden_from_identity_read(self) -> None:
        # The importer role unlocks only trigger_import, not the read endpoints.
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=IMPORTER_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH):
            by_id = self.client.get(
                f"{IDENTITIES_ROUTE}/{RECIDIVIZ_ID}", headers=IAP_HEADERS
            )
            by_query = self.client.get(
                f"{IDENTITIES_ROUTE}?external_id=A123&id_type=US_OZ_LOTR_ID",
                headers=IAP_HEADERS,
            )

        self.assertEqual(HTTPStatus.FORBIDDEN, by_id.status_code)
        self.assertEqual(HTTPStatus.FORBIDDEN, by_query.status_code)

    def test_reader_caller_allowed_to_read_identity(self) -> None:
        # The reader role unlocks the read endpoints without write access.
        identity = build_full_identity().identity
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=READER_SERVICE_ACCOUNT
        ), patch(QUERIER_PATH) as mock_querier_cls:
            mock_querier_cls.return_value.get_identity.return_value = identity
            response = self.client.get(
                f"{IDENTITIES_ROUTE}/{RECIDIVIZ_ID}", headers=IAP_HEADERS
            )

        self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_non_methodview_endpoints_are_exactly_the_role_exempt_list(self) -> None:
        # Role authorization fails closed for endpoints that are not blueprint
        # MethodViews, except those on the explicit ROLE_EXEMPT_ENDPOINTS list.
        # Guard against a plain @app.route(...) handler being added without an
        # explicit authorization decision, and against stale exempt entries for
        # endpoints that no longer exist.
        non_methodview_endpoints = {
            endpoint
            for endpoint, view_func in app.view_functions.items()
            if not hasattr(view_func, "view_class")
        }
        self.assertEqual(ROLE_EXEMPT_ENDPOINTS, frozenset(non_methodview_endpoints))

    def test_every_blueprint_view_method_declares_allowed_roles(self) -> None:
        # Authorization fails closed: the middleware denies any MethodView
        # method with no entry in its class's ALLOWED_ROLES_BY_METHOD. Guard
        # against a new endpoint or method silently shipping with no callers
        # able to reach it, and against stale entries for methods that no
        # longer exist.
        blueprint_prefix = f"{identity_blueprint.name}."
        view_classes = {
            view_func.view_class
            for endpoint, view_func in app.view_functions.items()
            if endpoint.startswith(blueprint_prefix)
            and hasattr(view_func, "view_class")
        }
        self.assertTrue(view_classes)
        for view_class in view_classes:
            with self.subTest(view_class=view_class.__name__):
                roles_by_method = getattr(view_class, "ALLOWED_ROLES_BY_METHOD", None)
                assert isinstance(roles_by_method, dict)
                implemented_methods = set(view_class.methods or [])
                self.assertTrue(implemented_methods)
                self.assertEqual(implemented_methods, set(roles_by_method))
                for method, allowed_roles in roles_by_method.items():
                    with self.subTest(method=method):
                        self.assertIsInstance(allowed_roles, frozenset)
                        self.assertTrue(allowed_roles)
                        self.assertTrue(
                            all(isinstance(r, CallerRole) for r in allowed_roles)
                        )
