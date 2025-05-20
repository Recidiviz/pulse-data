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
"""Generate a CTE for client metadata or resident metadata to be used in workflows_views.client_record
or workflows_views.resident_record."""

from typing import List

from recidiviz.common.constants.states import StateCode


def generate_person_metadata_cte(
    person_type: str, person_cte: str, states_with_metadata: List[StateCode]
) -> str:
    """
    Given a list of state codes, generates a CTE that maps from person_id to
    json-formatted metadata blob when available.

    State-specific metadata is expected to live in `workflows_views.us_xx_<person_type>_metadata`.

    This CTE will take all columns in the state-specific views for each state and pack them
    into a json blob in the `metadata` column. The state-specific views should only
    have one entry per person, but this CTE does some deduping to prevent run-time errors.

    When no metadata is found for a person, they do not have an entry in this CTE. Hence,
    we make sure to set a fallback for the metadata blob to `{}` when joining the
    rest of the person record to this CTE.
    """
    dedup_clauses = ", ".join(
        f"""
        deduped_{state_code.value.lower()}_metadata AS (
            SELECT *
            FROM `{{project_id}}.{{workflows_dataset}}.{state_code.value.lower()}_{person_type}_metadata_materialized`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id) = 1
        )
    """
        for state_code in states_with_metadata
    )

    metadata_to_json_clauses = "    UNION ALL\n".join(
        f"""
        SELECT
            person.person_id,
            TO_JSON_STRING(
                (SELECT AS STRUCT m.* EXCEPT (person_id), "{state_code.value.upper()}" AS state_code
                    FROM deduped_{state_code.value.lower()}_metadata m
                    WHERE person.person_id = m.person_id)
            ) as metadata,
        FROM {person_cte} person
        INNER JOIN  deduped_{state_code.value.lower()}_metadata
        USING (person_id)
        WHERE person.state_code = "{state_code.value.upper()}"
    """
        for state_code in states_with_metadata
    )

    return f"""
    metadata AS (
        WITH {dedup_clauses}
        {metadata_to_json_clauses}
    ),
    """
