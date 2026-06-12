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
"""Generate a CTE for state-specific data to be included alongside a more generic person view,
e.g. workflows_views.resident_record, workflows_views.client_record, jii_views.resident
"""

from recidiviz.common.constants.states import StateCode


def generate_person_state_specific_data_cte(
    source_dataset: str,
    source_table_name_without_state_prefix: str,
    person_source_table: str,
    states_to_include: list[StateCode],
    dest_column_and_cte_name: str,
) -> str:
    """
    Given a list of state codes, generates a CTE that maps from person_id to
    json-formatted data blob when available.

    This CTE will take all columns in the state-specific views for each state and pack them
    into a json blob in the specified column. The state-specific views should only
    have one entry per person, but this CTE does some deduping to prevent run-time errors.

    When no state-specific data is found for a person, they do not have an entry in this CTE. Hence,
    we make sure to set a fallback for the JSON blob when joining the rest of the person record to this CTE.
    """
    dedup_clauses = ", ".join(
        f"""
        deduped_{state_code.value.lower()}_data AS (
            SELECT *
            FROM `{{project_id}}.{source_dataset}.{state_code.value.lower()}_{source_table_name_without_state_prefix}`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id) = 1
        )
    """
        for state_code in states_to_include
    )

    metadata_to_json_clauses = "    UNION ALL\n".join(
        f"""
        SELECT
            person.person_id,
            TO_JSON_STRING(
                (SELECT AS STRUCT m.* EXCEPT (person_id), "{state_code.value.upper()}" AS state_code
                    FROM deduped_{state_code.value.lower()}_data m
                    WHERE person.person_id = m.person_id)
            ) as {dest_column_and_cte_name},
        FROM {person_source_table} person
        INNER JOIN deduped_{state_code.value.lower()}_data
        USING (person_id)
        WHERE person.state_code = "{state_code.value.upper()}"
    """
        for state_code in states_to_include
    )

    return f"""
    {dest_column_and_cte_name} AS (
        WITH {dedup_clauses}
        {metadata_to_json_clauses}
    )
    """
