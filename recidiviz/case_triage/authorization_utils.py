# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
This module contains a helper for authenticating users accessing pathways and workflows APIs hosted on the Case Triage
backend.
"""
import json
from typing import Callable

from recidiviz.utils.auth.auth0 import Auth0Config, build_auth0_authorization_handler
from recidiviz.utils.environment import in_offline_mode
from recidiviz.utils.secrets import get_secret


def build_auth_config(secret_name: str) -> Auth0Config:
    """Get the secret using the string provided and build the auth config"""
    auth0_configuration = get_secret(secret_name)

    if not auth0_configuration:
        raise ValueError("Missing Auth0 configuration secret")

    authorization_config = Auth0Config.from_config_json(json.loads(auth0_configuration))

    return authorization_config


def build_authorization_handler(
    on_successful_authorization: Callable, secret_name: str
) -> Callable:
    """Loads Auth0 configuration secret and builds the middleware"""

    if in_offline_mode():
        # Offline mode does not require authorization since it is only returning fixture data.
        return lambda: on_successful_authorization({}, offline_mode=True)

    authorization_config = build_auth_config(secret_name)

    return build_auth0_authorization_handler(
        authorization_config, on_successful_authorization
    )
