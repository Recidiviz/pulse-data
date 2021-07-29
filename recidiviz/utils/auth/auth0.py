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
"""
This module contains various pieces related to the Case Triage authentication / authorization flow
"""
import json
from functools import wraps
from http import HTTPStatus
from typing import Any, Callable, Dict, List, Optional
from urllib.request import urlopen

import jwt
import requests
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
from flask import request
from jwt.api_jwk import PyJWKSet

from recidiviz.utils.flask_exception import FlaskException


class AuthorizationError(FlaskException):
    """Exception for when the authorization flow fails."""

    def __init__(self, code: str, description: str) -> None:
        super().__init__(code, description, HTTPStatus.UNAUTHORIZED)


class Auth0Config:
    """Data object for wrapping/validating our Auth0 configuration JSON"""

    def __init__(self, config_json: Dict[str, Any]) -> None:
        self.config_json = config_json

        # Algorithms to use when encoding / decoding JWTs
        self.algorithms: List[str] = self.config_json["algorithms"]

        # The "aud" (audience) claim identifies the recipients that the JWT is intended for.
        # If presented with a token that does not match this audience, we will raise an `AuthorizationError` exception
        self.audience: str = self.config_json["audience"]

        # The public identifier of the Application we are authenticating with.
        # This is used by the front-end for authenticating with the correct Auth0 application
        self.client_id: str = self.config_json["clientId"]

        # The domain of our Auth0 tenant
        # This is used by the front-end when redirecting to the hosted Auth0 authentication flow
        self.domain: str = self.config_json["domain"]

        # The "iss" (issuer) claim identifies the application that issued the token
        # If presented with a token that was not issued by this value, we will raise an `AuthorizationError` exception
        self.issuer: str = f"https://{self.domain}/"

        with urlopen(f"https://{self.domain}/.well-known/jwks.json") as json_response:
            jwks = PyJWKSet.from_json(json_response.read())
        self.jwks: Dict[str, RSAPublicKey] = {jwk.key_id: jwk.key for jwk in jwks.keys}

        # Validate `algorithms` input value
        if "RS256" not in self.algorithms or len(self.algorithms) > 1:
            raise ValueError(
                "Our Auth0 integration currently only supports the RS256 algorithm"
            )

    def get_key(self, key_id: str) -> Optional[RSAPublicKey]:
        return self.jwks.get(key_id, None)

    def as_public_config(self) -> Dict[str, str]:
        # Returns a set of variables used for configuring the Auth0 frontend.
        # All data returned by this function is assumed to be publically accessible
        return {
            "audience": self.audience,
            "clientId": self.client_id,
            "domain": self.domain,
        }


def get_token_auth_header() -> str:
    """Obtains the Access Token from the Authorization Header"""
    auth = request.headers.get("Authorization", None)
    if not auth:
        raise AuthorizationError(
            code="authorization_header_missing",
            description="Authorization header is expected",
        )

    parts = auth.split()

    if parts[0].lower() != "bearer":
        raise AuthorizationError(
            code="invalid_header",
            description="Authorization header must start with Bearer",
        )
    if len(parts) == 1:
        raise AuthorizationError(
            code="invalid_header",
            description="Token not found",
        )
    if len(parts) > 2:
        raise AuthorizationError(
            code="invalid_header",
            description="Authorization header must be Bearer token",
        )

    token = parts[1]
    return token


def build_auth0_authorization_decorator(
    authorization_config: Auth0Config, on_successful_authorization: Callable
) -> Callable:
    """Decorator builder for Auth0 authorization"""

    def decorated(route: Callable) -> Callable:
        @wraps(route)
        def inner(*args: List[Any], **kwargs: Dict[str, Any]) -> Any:
            """
            Determines if the access token provided in the request `Authorization` header is valid
            If it is not valid, raise an exception
            If it is valid, call our `on_successful_authorization` callback before executing the decorated route
            """
            token = get_token_auth_header()
            unverified_header = jwt.get_unverified_header(token)
            rsa_key = authorization_config.get_key(unverified_header["kid"])

            if rsa_key:
                try:
                    payload = jwt.decode(
                        token,
                        # `jwt.decode` specifies this argument as a string, but `RSAAlgorithm.prepare_key` accepts an
                        # instance of `RSAPublicKey`
                        rsa_key,  # type: ignore
                        algorithms=authorization_config.algorithms,
                        issuer=authorization_config.issuer,
                        audience=authorization_config.audience,
                    )
                except jwt.ExpiredSignatureError as e:
                    raise AuthorizationError(
                        code="token_expired",
                        description="token is expired",
                    ) from e
                except (
                    jwt.InvalidIssuerError,
                    jwt.InvalidAudienceError,
                    jwt.MissingRequiredClaimError,
                ) as e:
                    raise AuthorizationError(
                        code="invalid_claims",
                        description="incorrect claims, please check the audience and issuer",
                    ) from e
                except Exception as e:
                    raise AuthorizationError(
                        code="invalid_header",
                        description="Unable to parse authentication token.",
                    ) from e

                on_successful_authorization(payload, token)

                return route(*args, **kwargs)

            raise AuthorizationError(
                code="invalid_header",
                description="Unable to find appropriate key",
            )

        return inner

    return decorated


def get_userinfo(domain: str, token: str) -> Dict[str, str]:
    """Fetch the user's information from Auth0"""
    response = requests.get(
        f"https://{domain}/userinfo", headers={"Authorization": f"Bearer {token}"}
    )

    return json.loads(response.text)
