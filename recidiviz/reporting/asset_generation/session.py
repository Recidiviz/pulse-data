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
"""Session object for making authorized requests to the asset generation service."""
from google.auth.transport.requests import Request
from google.oauth2.id_token import fetch_id_token
from requests import Session

from recidiviz.reporting.asset_generation.constants import ASSET_GENERATION_SERVICE_URI
from recidiviz.utils.environment import in_gcp

# dev environment bypasses auth
session = Session()

if in_gcp():
    _id_token = fetch_id_token(request=Request(), audience=ASSET_GENERATION_SERVICE_URI)
    session.headers.update(
        {
            "Authorization": f"Bearer {_id_token}",
        }
    )
