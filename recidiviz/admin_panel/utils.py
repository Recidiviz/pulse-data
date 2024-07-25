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
"""Helpers related to the admin panel."""

from typing import Dict

from google.auth.transport.requests import Request
from google.oauth2.id_token import fetch_id_token

from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, in_gcp
from recidiviz.utils.secrets import get_secret


def auth_header_for_request_to_prod() -> Dict[str, str]:
    """
    Returns the headers needed for a request to the admin panel in production from
    the admin panel backend in staging.
    """
    if not in_gcp():
        return {}

    # We need the to authenticate the admin panel staging service account when making a
    # request to an endpoint in the admin panel production service.
    # Since the service is an IAP-protected resource, we use the client id used by the
    # IAP to get an OIDC token to authenticate the service account.
    audience = get_secret("iap_client_id", GCP_PROJECT_PRODUCTION)

    _id_token = fetch_id_token(
        request=Request(),
        audience=audience,
    )
    return {
        "Authorization": f"Bearer {_id_token}",
    }
