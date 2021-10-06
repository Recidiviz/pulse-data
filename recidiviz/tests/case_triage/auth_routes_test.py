# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Implements tests for the authorization blueprint."""
import unittest

from flask import Flask, session
from jwt import PyJWKSet

from recidiviz.case_triage.auth_routes import create_auth_blueprint
from recidiviz.tests.utils.auth.auth0_test import generate_keypair, get_public_jwk
from recidiviz.utils.auth.auth0 import Auth0Config


class TestAuthRoutes(unittest.TestCase):
    """ Tests the auth blueprint"""

    def setUp(self) -> None:
        _private_key, public_key = generate_keypair()
        jwks = PyJWKSet.from_dict({"keys": [get_public_jwk(public_key, "keyid")]})

        authorization_config = Auth0Config(
            {
                "algorithms": ["RS256"],
                "domain": "auth0.localhost",
                "clientId": "test_client_id",
                "audience": "http://localhost",
            },
            jwks=jwks,
        )

        auth_blueprint = create_auth_blueprint(authorization_config)

        self.test_app = Flask(__name__)
        self.test_app.secret_key = "not a secret"
        self.test_app.register_blueprint(auth_blueprint, url_prefix="/auth")

    def test_log_out(self) -> None:
        """
        Cobalt ID: #PT7441_2
        OWASP ASVS: 3.3.1
        CWE 613
        NIST 7.1
        """
        with self.test_app.test_client() as client:
            with client.session_transaction() as sess:
                sess["session_data"] = {}

            response = client.post("/auth/log_out")

            self.assertEqual(
                response.headers["Set-Cookie"],
                "session=; Expires=Thu, 01-Jan-1970 00:00:00 GMT; Max-Age=0; Path=/",
            )

            self.assertEqual(
                "https://auth0.localhost/v2/logout?client_id=test_client_id",
                response.headers["Location"],
            )
            self.assertEqual(0, len(session.keys()))
