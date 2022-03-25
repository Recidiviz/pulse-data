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
"""Shared utilities to test authentication flows"""

from jwt import PyJWKSet

from recidiviz.tests.utils.auth.auth0_test import generate_keypair, get_public_jwk
from recidiviz.utils.auth.auth0 import Auth0Config


def get_test_auth0_config() -> Auth0Config:
    _private_key, public_key = generate_keypair()
    jwks = PyJWKSet.from_dict({"keys": [get_public_jwk(public_key, "keyid")]})
    return Auth0Config(
        {
            "algorithms": ["RS256"],
            "domain": "auth0.localhost",
            "clientId": "test_client_id",
            "audience": "http://localhost",
        },
        jwks=jwks,
    )
