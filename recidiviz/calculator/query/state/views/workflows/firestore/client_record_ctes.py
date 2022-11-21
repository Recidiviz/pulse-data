# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""CTEs used across multiple states' client record queries."""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause


def client_record_supervision_cte(state_code: str) -> str:
    return f"""
    {state_code.lower()}_supervision_cases AS (
        SELECT
          sessions.person_id,
          pei.external_id AS person_external_id,
          sessions.compartment_level_2 AS supervision_type,
            -- Pull the officer ID from compartment_sessions instead of supervision_officer_sessions
            -- to make sure we choose the officer that aligns with other compartment session attributes.
          sessions.supervising_officer_external_id_end AS officer_id,
          locations.level_2_supervision_location_name AS district,
          projected_end.projected_completion_date_max AS expiration_date
        FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sessions
        INNER JOIN `{{project_id}}.{{state_dataset}}.state_person_external_id` pei
            ON sessions.person_id = pei.person_id
            AND sessions.state_code = pei.state_code
            AND {{state_id_type}} = pei.id_type
        INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_projected_completion_date_spans_materialized` projected_end
            ON sessions.state_code = projected_end.state_code
            AND sessions.person_id = projected_end.person_id
            AND CURRENT_DATE('US/Eastern')
                BETWEEN projected_end.start_date
                    AND {nonnull_end_date_exclusive_clause('projected_end.end_date')}
        -- Remove clients who previously had an active officer, but no longer do.
        INNER JOIN (
            SELECT DISTINCT
                state_code,
                person_id
            FROM `{{project_id}}.{{sessions_dataset}}.supervision_officer_sessions_materialized`
            WHERE end_date IS NULL
                AND supervising_officer_external_id IS NOT NULL
        ) active_officer
            ON sessions.state_code = active_officer.state_code
            AND sessions.person_id = active_officer.person_id
        LEFT JOIN (
            SELECT DISTINCT
                state_code,
                level_2_supervision_location_external_id,
                level_2_supervision_location_name
            FROM `{{project_id}}.{{reference_views_dataset}}.supervision_location_ids_to_names_materialized`
        ) locations
            ON locations.state_code = sessions.state_code
            AND locations.level_2_supervision_location_external_id = SPLIT(sessions.compartment_location_end, "|")[OFFSET(1)]
        WHERE sessions.state_code = '{state_code}'
          AND sessions.compartment_level_1 = "SUPERVISION"
          AND sessions.end_date IS NULL
          AND sessions.supervising_officer_external_id_end IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY person_id
            ORDER BY person_external_id
        ) = 1
    ),
    """
