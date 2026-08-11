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
Firestore ETL has finished for a given Firestore collection.

The Typesense backfill function lives in a separate repo and GCP project. It is an
HTTP-triggered function that takes a state code and the Typesense collection(s) to
re-index, so we invoke it with a service-to-service OIDC token (audience = the function
URL).

Most Workflows collections map one-to-one from Firestore to Typesense. Opportunities do
not: every per-state, per-opportunity Firestore collection (e.g.
`US_TN-supervisionLevelDowngrade`) feeds the single Typesense `opportunities`
collection, so those requests additionally name the Firestore source collection they are
backfilling from."""
import logging
from typing import Any

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
SOURCE_COLLECTION_PARAM = "sourceCollection"

# Names of the fields the backfill function returns in a 200 response body.
TOTALS_FIELD = "totals"
IMPORTED_FIELD = "imported"

# The single Typesense collection that every Firestore opportunity collection feeds.
OPPORTUNITIES_TYPESENSE_COLLECTION = "opportunities"

_REQUEST_TIMEOUT_SECONDS = 60


class TypesenseBackfillClient:
    """Client that triggers the Typesense backfill Cloud Function (in a separate GCP
    project) for a single Firestore collection once its Workflows Firestore ETL has
    finished."""

    def trigger_backfill(self, *, state_code: StateCode, collection: str) -> None:
        """Sends an authenticated request to the Typesense backfill function for a
        collection that maps one-to-one from Firestore to Typesense."""
        self._post_backfill_request(
            state_code=state_code,
            typesense_collection=collection,
            source_collection=None,
        )

    def trigger_opportunities_backfill(
        self, *, state_code: StateCode, source_collection: str
    ) -> None:
        """Sends an authenticated request to the Typesense backfill function to
        re-index a single Firestore opportunity collection (e.g.
        `US_TN-supervisionLevelDowngrade`) into the shared `opportunities` Typesense
        collection.

        One call per source collection: the function builds its config from
        `source_collection` at request time, and its prune is scoped to that source's
        partition, so sources do not interfere with one another."""
        self._post_backfill_request(
            state_code=state_code,
            typesense_collection=OPPORTUNITIES_TYPESENSE_COLLECTION,
            source_collection=source_collection,
        )

    def _post_backfill_request(
        self,
        *,
        state_code: StateCode,
        typesense_collection: str,
        source_collection: str | None,
    ) -> None:
        """Posts a backfill request to the Typesense backfill function, including
        `sourceCollection` when the Typesense collection is fed by more than one
        Firestore collection.

        No-ops when not running in GCP (local/test) or when the function URL secret is
        unset, so the trigger stays dormant until the URL is configured."""
        if not in_gcp():
            return

        function_url = get_secret(TYPESENSE_BACKFILL_FUNCTION_URL_SECRET_ID)
        if not function_url:
            logging.warning(
                "Secret [%s] is not set; skipping Typesense backfill trigger for "
                "state_code=[%s] collection=[%s] source_collection=[%s].",
                TYPESENSE_BACKFILL_FUNCTION_URL_SECRET_ID,
                state_code.value,
                typesense_collection,
                source_collection,
            )
            return

        body: dict[str, Any] = {
            STATE_CODE_PARAM: state_code.value,
            COLLECTIONS_PARAM: [typesense_collection],
        }
        if source_collection is not None:
            body[SOURCE_COLLECTION_PARAM] = source_collection

        id_token = fetch_id_token(request=Request(), audience=function_url)
        response = requests.post(
            function_url,
            json=body,
            headers={"Authorization": f"Bearer {id_token}"},
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        self._warn_if_nothing_imported(
            response=response,
            state_code=state_code,
            typesense_collection=typesense_collection,
            source_collection=source_collection,
        )

    @staticmethod
    def _warn_if_nothing_imported(
        *,
        response: requests.Response,
        state_code: StateCode,
        typesense_collection: str,
        source_collection: str | None,
    ) -> None:
        """Logs a warning when the backfill function reports a successful run that
        imported nothing. The function returns 200 with `imported: 0` when it finds no
        documents to index (e.g. it was pointed at a source collection that does not
        exist), so a zero import is a signal that the request was misconfigured rather
        than evidence of success.

        This inspection is best-effort. The backfill function is deployed from a
        separate repo on its own schedule, so its response body is not a shape this
        client can rely on; a body we cannot read is reported and otherwise ignored. By
        the time we get here the backfill itself has already succeeded, so nothing about
        parsing the body should be allowed to look like a failed trigger."""
        try:
            imported = response.json()[TOTALS_FIELD][IMPORTED_FIELD]
        except Exception:
            logging.warning(
                "Could not read import totals from the Typesense backfill response for "
                "state_code=[%s] collection=[%s] source_collection=[%s]; the backfill "
                "itself succeeded. Response body was [%s].",
                state_code.value,
                typesense_collection,
                source_collection,
                response.text,
            )
            return

        if imported:
            return

        logging.warning(
            "Typesense backfill imported 0 documents for state_code=[%s] "
            "collection=[%s] source_collection=[%s]; response was [%s].",
            state_code.value,
            typesense_collection,
            source_collection,
            response.text,
        )
