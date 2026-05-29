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
"""Person resolution for the Edovo course-completion API.

Looks up a Colorado DOC offender number (US_CO_DOC_ID) in the normalized
state BigQuery dataset and returns the internal person_id used by the rest
of the platform.
"""

from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.utils.metadata import project_id

_STATE_CODE = "US_CO"
_ID_TYPE = "US_CO_DOC_ID"


class PersonNotFoundError(Exception):
    """Raised when no person can be found for the given DOC ID."""

    def __init__(self, doc_id: str) -> None:
        self.doc_id = doc_id
        super().__init__(
            f"No person found with external_id {doc_id!r} of type {_ID_TYPE!r}"
        )


def resolve_person_by_doc_id(bq_client: BigQueryClientImpl, doc_id: str) -> str:
    """Return the platform-internal person_id for a Colorado DOC offender number.

    Raises PersonNotFoundError if no matching record exists.
    """
    query = f"""
        SELECT person_id
        FROM `{project_id()}.normalized_state.state_person_external_id`
        WHERE state_code = @state_code
          AND id_type    = @id_type
          AND external_id = @external_id
        LIMIT 1
    """
    job = bq_client.run_query_async(
        query_str=query,
        use_query_cache=False,
        query_parameters=[
            bigquery.ScalarQueryParameter("state_code", "STRING", _STATE_CODE),
            bigquery.ScalarQueryParameter("id_type", "STRING", _ID_TYPE),
            bigquery.ScalarQueryParameter("external_id", "STRING", doc_id),
        ],
    )
    for row in job:
        return str(row["person_id"])
    raise PersonNotFoundError(doc_id)
