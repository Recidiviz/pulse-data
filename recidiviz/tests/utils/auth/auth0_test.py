# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for Auth0 authorization."""
import json
import unittest
from typing import Tuple
from unittest.mock import Mock

import jwt
from cryptography.hazmat.backends import default_backend as crypto_default_backend
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from flask import Flask, Response, jsonify
from jwt.algorithms import RSAAlgorithm
from mock import patch

from recidiviz.utils.auth.auth0 import Auth0Config, build_auth0_authorization_decorator
from recidiviz.utils.flask_exception import FlaskException


def generate_keypair() -> Tuple[bytes, bytes]:
    key = rsa.generate_private_key(
        backend=crypto_default_backend(), public_exponent=65537, key_size=2048
    )

    private_key = key.private_bytes(
        crypto_serialization.Encoding.PEM,
        crypto_serialization.PrivateFormat.PKCS8,
        crypto_serialization.NoEncryption(),
    )

    public_key = key.public_key().public_bytes(
        crypto_serialization.Encoding.OpenSSH, crypto_serialization.PublicFormat.OpenSSH
    )

    return (private_key, public_key)


def get_public_jwk(public_key, key_id):
    key = json.loads(
        RSAAlgorithm.to_jwk(RSAAlgorithm(RSAAlgorithm.SHA256).prepare_key(public_key))
    )
    key["alg"] = "RS256"
    key["kid"] = key_id
    return key


class Auth0ModuleTest(unittest.TestCase):
    """Tests for auth0.py."""

    def setUp(self) -> None:
        self.private_key, self.public_key = generate_keypair()
        self.public_jwk = get_public_jwk(self.public_key, "12345")

        # Mock our `/.well-known/jwks.json` response
        self.urlopen_patcher = patch("recidiviz.utils.auth.auth0.urlopen")
        self.urlopen_patcher.start().return_value = Mock(
            read=lambda: json.dumps({"keys": [self.public_jwk]})
        )

        self.test_app = Flask(__name__)
        self.test_client = self.test_app.test_client()

        self.authorization_config = Auth0Config(
            {
                "algorithms": ["RS256"],
                "audience": "test_audience",
                "clientId": "test_clientId",
                "domain": "test_domain.com",
            }
        )

        authorization_decorator = build_auth0_authorization_decorator(
            self.authorization_config, (lambda payload, token: None)
        )

        @self.test_app.errorhandler(FlaskException)
        def _handle_auth_error(ex: FlaskException) -> Response:
            response = jsonify(
                {
                    "code": ex.code,
                    "description": ex.description,
                }
            )
            response.status_code = ex.status_code
            return response

        @self.test_app.route("/protected_route")
        @authorization_decorator
        def _index():
            return jsonify({"status": "OK"})

    def tearDown(self) -> None:
        self.urlopen_patcher.stop()

    def build_jwt(self, claims) -> str:
        return jwt.encode(
            claims,
            self.private_key.decode("utf-8"),
            algorithm="RS256",
            headers={"kid": self.public_jwk["kid"]},
        )

    def subject(self, headers=None) -> Response:
        return self.test_client.get("/protected_route", headers=headers)

    def test_jwt_authorization_header(self) -> None:
        # Test missing header, or no header value
        # doesnt start w bearer, empty value, or has more than 2 parts

        # The request is missing an Authorization header and should be rejected
        response = self.subject()
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["code"], "authorization_header_missing")
        self.assertEqual(
            response.get_json()["description"], "Authorization header is expected"
        )

        # The request is missing an Authorization header value and should be rejected
        response = self.subject({"Authorization": ""})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["code"], "authorization_header_missing")
        self.assertEqual(
            response.get_json()["description"], "Authorization header is expected"
        )

        # The request is not using a Bearer token and should be rejected
        response = self.subject({"Authorization": "Basic 123"})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["code"], "invalid_header")
        self.assertEqual(
            response.get_json()["description"],
            "Authorization header must start with Bearer",
        )

        # The request is not using a valid Bearer token and should be rejected
        response = self.subject({"Authorization": "Bearer"})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["code"], "invalid_header")
        self.assertEqual(response.get_json()["description"], "Token not found")

        # The request is not using a valid Bearer token and should be rejected
        response = self.subject({"Authorization": "Bearer 1 2 3"})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["code"], "invalid_header")
        self.assertEqual(
            response.get_json()["description"],
            "Authorization header must be Bearer token",
        )

    def test_jwt_contents(self) -> None:
        # Expired token
        token = self.build_jwt({"exp": 0})
        response = self.subject({"Authorization": f"Bearer {token}"})
        self.assertEqual(response.get_json()["code"], "token_expired")
        self.assertEqual(response.get_json()["description"], "token is expired")
        self.assertEqual(response.status_code, 401)

        # Valid issuer, no audience
        token = self.build_jwt({"iss": self.authorization_config.issuer})
        response = self.subject({"Authorization": f"Bearer {token}"})
        self.assertEqual(response.get_json()["code"], "invalid_claims")
        self.assertEqual(
            response.get_json()["description"],
            "incorrect claims, please check the audience and issuer",
        )
        self.assertEqual(response.status_code, 401)

        # No issuer, valid audience
        token = self.build_jwt({"aud": self.authorization_config.audience})
        response = self.subject({"Authorization": f"Bearer {token}"})
        self.assertEqual(response.get_json()["code"], "invalid_claims")
        self.assertEqual(
            response.get_json()["description"],
            "incorrect claims, please check the audience and issuer",
        )
        self.assertEqual(response.status_code, 401)

        # Invalid issuer, valid audience
        token = self.build_jwt(
            {"iss": "none", "aud": self.authorization_config.audience}
        )
        response = self.subject({"Authorization": f"Bearer {token}"})
        self.assertEqual(response.get_json()["code"], "invalid_claims")
        self.assertEqual(
            response.get_json()["description"],
            "incorrect claims, please check the audience and issuer",
        )
        self.assertEqual(response.status_code, 401)

        # Valid issuer, audience
        token = self.build_jwt(
            {
                "iss": self.authorization_config.issuer,
                "aud": self.authorization_config.audience,
            }
        )
        response = self.subject({"Authorization": f"Bearer {token}"})
        self.assertEqual(response.get_json(), {"status": "OK"})
        self.assertEqual(response.status_code, 200)
