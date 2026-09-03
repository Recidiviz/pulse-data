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
"""Identity verification for the Edovo course-completion API.

Confirms that the external, DOC-facing identifier Edovo sends resolves to a
known person in the normalized state BigQuery dataset, and that the name Edovo
sends is that person's name. The external-id type to match is looked up per
state from ``SUPPORTED_STATES``.

The two failure modes are reported separately because they mean different things
to Edovo: an id we hold no record of (``PersonNotFoundError``) versus an id we do
hold that belongs to someone else (``PersonNameMismatchError``). The second is
the identifier drift Edovo asked to be told about — their record and ours
disagree about who this id is, and only they can reconcile it.

This verifies identity only; we persist the external id and resolve the internal
person_id downstream during earned-time credit processing.
"""
import logging

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.case_triage.edovo.course_completion_models import (
    FIRST_NAME_FIELD,
    LAST_NAME_FIELD,
    MismatchedNameField,
)
from recidiviz.case_triage.edovo.external_id_matching import (
    PERSON_EXTERNAL_ID_ADDRESS,
    zero_stripped,
)
from recidiviz.case_triage.edovo.name_matching import given_names_match, surnames_match
from recidiviz.case_triage.edovo.supported_states import SUPPORTED_STATES
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.metadata import project_id

PERSON_ADDRESS = BigQueryAddress(dataset_id="normalized_state", table_id="state_person")

_GIVEN_NAMES_COLUMN = "given_names"
_SURNAME_COLUMN = "surname"


class PersonNotFoundError(Exception):
    """Raised when no person can be found for the given external id."""

    def __init__(self, person_external_id: str) -> None:
        # Kept off the message string so the external id (PII) stays out of logs.
        self.person_external_id = person_external_id
        super().__init__("No person found for the provided external_id.")


class PersonNameMismatchError(Exception):
    """Raised when the external id resolves, but to a person of another name.

    ``mismatched_fields`` names the submitted fields that matched no candidate
    person, so the endpoint can tell Edovo which part of the name disagrees
    without echoing either name back.
    """

    def __init__(
        self, *, person_external_id: str, mismatched_fields: list[MismatchedNameField]
    ) -> None:
        # As above, the id and names stay off the message string.
        self.person_external_id = person_external_id
        self.mismatched_fields = mismatched_fields
        super().__init__(
            "The provided name does not match our record for this external_id."
        )


def _fetch_stored_names(
    *,
    bq_client: BigQueryClientImpl,
    state_code: StateCode,
    id_type: str,
    person_external_id: str,
) -> list[dict[str, str]]:
    """Returns a name we hold for every person |person_external_id| resolves to.

    An external id is expected to resolve to exactly one person, but the query
    does not assume it: it returns every match so a name check cannot fail
    merely because a second record exists. The join is on the full primary key
    of ``state_person``, so it adds no rows of its own.

    The join is a LEFT join so that an external id whose person row is missing
    still counts as a person we hold, with no name to compare — which is what
    this check did before it compared names. The foreign key that should make
    that impossible is enforced in Postgres, not in the materialized dataset
    queried here, and turning such a row into PERSON_NOT_FOUND would reject a
    completion for a learner we do have.

    A name part we hold no value for comes back as an empty string rather than
    None, so the matching rules never have to special-case a missing column.
    """
    # Zero-stripped to match the credit calculator's resolution query.
    query = f"""
        SELECT
            JSON_EXTRACT_SCALAR(person.full_name, '$.given_names') AS {_GIVEN_NAMES_COLUMN},
            JSON_EXTRACT_SCALAR(person.full_name, '$.surname') AS {_SURNAME_COLUMN}
        FROM `{project_id()}.{PERSON_EXTERNAL_ID_ADDRESS.to_str()}` external_id
        LEFT JOIN `{project_id()}.{PERSON_ADDRESS.to_str()}` person
            USING (state_code, person_id)
        WHERE external_id.state_code = @state_code
          AND external_id.id_type    = @id_type
          AND {zero_stripped("external_id.external_id")} = {zero_stripped("@external_id")}
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
    return [
        {
            _GIVEN_NAMES_COLUMN: row[_GIVEN_NAMES_COLUMN] or "",
            _SURNAME_COLUMN: row[_SURNAME_COLUMN] or "",
        }
        for row in job
    ]


def verify_person_identity(
    *,
    bq_client: BigQueryClientImpl,
    state_code: StateCode,
    person_external_id: str,
    first_name: str,
    last_name: str,
) -> None:
    """Verify that |person_external_id| resolves to a person named |first_name| |last_name|.

    Raises PersonNotFoundError if the external id matches no person, and
    PersonNameMismatchError if it matches a person whose name is not the one
    submitted.
    """
    id_type = SUPPORTED_STATES[state_code]
    stored_names = _fetch_stored_names(
        bq_client=bq_client,
        state_code=state_code,
        id_type=id_type,
        person_external_id=person_external_id,
    )
    if not stored_names:
        raise PersonNotFoundError(person_external_id)

    first_name_agrees = [
        given_names_match(
            submitted_first_name=first_name,
            stored_given_names=stored[_GIVEN_NAMES_COLUMN],
        )
        for stored in stored_names
    ]
    surname_agrees = [
        surnames_match(
            submitted_last_name=last_name, stored_surname=stored[_SURNAME_COLUMN]
        )
        for stored in stored_names
    ]
    if any(
        first_ok and last_ok
        for first_ok, last_ok in zip(first_name_agrees, surname_agrees)
    ):
        return

    # Report only the parts that matched nothing, so a part that does line up
    # is not blamed.
    mismatched_fields: list[MismatchedNameField] = []
    if not any(first_name_agrees):
        mismatched_fields.append(FIRST_NAME_FIELD)
    if not any(surname_agrees):
        mismatched_fields.append(LAST_NAME_FIELD)
    if not mismatched_fields:
        # Each part matched someone, but no one person matched both.
        mismatched_fields = [FIRST_NAME_FIELD, LAST_NAME_FIELD]

    logging.warning(
        "Edovo identifier drift: external id of type [%s] in [%s] resolved to "
        "[%s] person record(s), none matching the submitted name. Mismatched "
        "fields: [%s].",
        id_type,
        state_code.value,
        len(stored_names),
        ", ".join(mismatched_fields),
    )
    raise PersonNameMismatchError(
        person_external_id=person_external_id, mismatched_fields=mismatched_fields
    )
