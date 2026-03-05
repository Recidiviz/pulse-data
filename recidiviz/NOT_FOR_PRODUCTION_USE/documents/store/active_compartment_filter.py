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
"""SQL helpers for filtering entities to those with active compartment sessions."""

import re

from recidiviz.common.constants.states import StateCode
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.document_collection_config import (
    DocumentRootEntityIdType,
)


def validate_active_in_compartment(active_in_compartment: str) -> None:
    """Validates the active_in_compartment value to prevent SQL injection."""
    if not re.match(r"^[A-Z_]+$", active_in_compartment):
        raise ValueError(
            f"Invalid active_in_compartment value: '{active_in_compartment}'. "
            "Must contain only uppercase letters and underscores (e.g., SUPERVISION, INCARCERATION)."
        )


def build_active_entities_cte_sql(
    root_entity_type: DocumentRootEntityIdType,
    project_id: str,
    state_code: StateCode,
    active_in_compartment: str,
) -> str:
    """Builds the SQL for an `active_entities` CTE that identifies entities with
    an active session in the given compartment.

    For PERSON_EXTERNAL_ID: joins compartment_sessions_materialized to
    state_person_external_id to resolve external IDs.
    For PERSON_ID: direct join on person_id.
    For STAFF_*: raises ValueError (staff don't have compartment sessions).
    """
    validate_active_in_compartment(active_in_compartment)

    match root_entity_type:
        case DocumentRootEntityIdType.PERSON_EXTERNAL_ID:
            return f"""active_entities AS (
            SELECT DISTINCT ext_id.external_id AS person_external_id,
                            ext_id.id_type AS person_external_id_type
            FROM `{project_id}.sessions.compartment_sessions_materialized` cs
            INNER JOIN `{project_id}.normalized_state.state_person_external_id` ext_id
                ON cs.person_id = ext_id.person_id
                AND ext_id.state_code = '{state_code.value}'
            WHERE cs.compartment_level_1 = '{active_in_compartment}'
              AND cs.end_date_exclusive IS NULL
              AND cs.state_code = '{state_code.value}'
        )"""
        case DocumentRootEntityIdType.PERSON_ID:
            return f"""active_entities AS (
            SELECT DISTINCT cs.person_id
            FROM `{project_id}.sessions.compartment_sessions_materialized` cs
            WHERE cs.compartment_level_1 = '{active_in_compartment}'
              AND cs.end_date_exclusive IS NULL
              AND cs.state_code = '{state_code.value}'
        )"""
        case DocumentRootEntityIdType.STAFF_ID | DocumentRootEntityIdType.STAFF_EXTERNAL_ID:
            raise ValueError(
                f"active_in_compartment filtering is not supported for "
                f"root entity type {root_entity_type.value}. "
                f"Staff entities do not have compartment sessions."
            )
        case _:
            raise NotImplementedError(
                f"active_in_compartment filtering not implemented for {root_entity_type.value}"
            )


def build_active_entities_exists_filter(
    root_entity_type: DocumentRootEntityIdType,
    table_alias: str,
) -> str:
    """Returns an EXISTS clause that filters rows to only those matching active entities.

    Args:
        root_entity_type: The root entity type to determine join columns.
        table_alias: The alias of the table being filtered (e.g., 'generated_docs', 'm').
    """
    match root_entity_type:
        case DocumentRootEntityIdType.PERSON_EXTERNAL_ID:
            return (
                f"EXISTS (SELECT 1 FROM active_entities "
                f"WHERE active_entities.person_external_id = {table_alias}.person_external_id "
                f"AND active_entities.person_external_id_type = {table_alias}.person_external_id_type)"
            )
        case DocumentRootEntityIdType.PERSON_ID:
            return (
                f"EXISTS (SELECT 1 FROM active_entities "
                f"WHERE active_entities.person_id = {table_alias}.person_id)"
            )
        case DocumentRootEntityIdType.STAFF_ID | DocumentRootEntityIdType.STAFF_EXTERNAL_ID:
            raise ValueError(
                f"active_in_compartment filtering is not supported for "
                f"root entity type {root_entity_type.value}."
            )
        case _:
            raise NotImplementedError(
                f"active_in_compartment filtering not implemented for {root_entity_type.value}"
            )
