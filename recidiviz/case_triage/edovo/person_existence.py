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
"""Person existence check for the Edovo course-completion API.

Confirms that the external, DOC-facing identifier Edovo sends resolves to a
known person in the normalized state BigQuery dataset, so the endpoint can
return PERSON_NOT_FOUND for an unknown learner. The external-id type to match is
looked up per state from ``SUPPORTED_STATES``. This only verifies existence; we
persist the external id and resolve the internal person_id downstream during
earned-time credit processing.
"""

from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.case_triage.edovo.supported_states import SUPPORTED_STATES
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.metadata import project_id


class PersonNotFoundError(Exception):
    """Raised when no person can be found for the given external id."""

    def __init__(self, person_external_id: str) -> None:
        # Retained for programmatic use; deliberately kept out of the message
        # string so the external id (PII) is not written into logs/tracebacks.
        self.person_external_id = person_external_id
        super().__init__("No person found for the provided external_id.")


def assert_person_exists(
    bq_client: BigQueryClientImpl,
    state_code: StateCode,
    person_external_id: str,
) -> None:
    """Verify a person exists for the given state and external id.

    Raises PersonNotFoundError if no matching record exists.
    """
    id_type = SUPPORTED_STATES[state_code]
    # Only checking existence — select a constant, not person_id / the external
    # id, so nothing tempts downstream code to read an identifier from here.
    query = f"""
        SELECT 1
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
            bigquery.ScalarQueryParameter("state_code", "STRING", state_code.value),
            bigquery.ScalarQueryParameter("id_type", "STRING", id_type),
            bigquery.ScalarQueryParameter("external_id", "STRING", person_external_id),
        ],
    )
    for _ in job:
        return
    raise PersonNotFoundError(person_external_id)
