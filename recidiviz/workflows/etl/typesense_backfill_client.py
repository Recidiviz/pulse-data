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
"""Client for triggering the Typesense backfill Cloud Function once a Workflows
Firestore ETL has finished for a given (state_code, collection) pair.

The Typesense backfill function lives in a separate repo and GCP project. It is an
HTTP-triggered function that takes a state code and Firestore collection name and
re-indexes that collection into Typesense, so we invoke it with a service-to-service
OIDC token (audience = the function URL)."""
import logging

import requests
from google.auth.transport.requests import Request
from google.oauth2.id_token import fetch_id_token

from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import in_gcp
from recidiviz.utils.secrets import get_secret

# Secret Manager secret holding the HTTPS trigger URL of the Typesense backfill Cloud
# Function. Populated per-environment (staging and production).
TYPESENSE_BACKFILL_FUNCTION_URL_SECRET_ID = "TYPESENSE_BACKFILL_FUNCTION_URL"  # nosec B105 - Secret Manager resource name, not a credential

# Names of the parameters the backfill function expects in the request body.
STATE_CODE_PARAM = "stateCode"
COLLECTIONS_PARAM = "collections"

_REQUEST_TIMEOUT_SECONDS = 60


class TypesenseBackfillClient:
    """Client that triggers the Typesense backfill Cloud Function (in a separate GCP
    project) for a single (state_code, collection) pair once its Workflows Firestore
    ETL has finished."""

    def trigger_backfill(self, *, state_code: StateCode, collection: str) -> None:
        """Sends an authenticated request to the Typesense backfill function for the
        given state and Firestore collection.

        No-ops when not running in GCP (local/test) or when the function URL secret is
        unset, so the trigger stays dormant until the URL is configured."""
        if not in_gcp():
            return

        function_url = get_secret(TYPESENSE_BACKFILL_FUNCTION_URL_SECRET_ID)
        if not function_url:
            logging.warning(
                "Secret [%s] is not set; skipping Typesense backfill trigger for "
                "state_code=[%s] collection=[%s].",
                TYPESENSE_BACKFILL_FUNCTION_URL_SECRET_ID,
                state_code.value,
                collection,
            )
            return

        id_token = fetch_id_token(request=Request(), audience=function_url)
        response = requests.post(
            function_url,
            json={STATE_CODE_PARAM: state_code.value, COLLECTIONS_PARAM: [collection]},
            headers={"Authorization": f"Bearer {id_token}"},
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
