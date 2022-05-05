#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Helpers for querying user events logged from Practices"""
from typing import List


def user_event_template(table_name: str, add_columns: List[str] = None) -> str:
    if add_columns is None:
        add_columns = []

    return f"""
    WITH 
    -- reidentifies clients from hash
    clients AS (
        SELECT DISTINCT
            person_id,
            person_external_id,
            -- this incorporates state code so we don't need to join on it explicitly
            pseudonymized_id AS client_id,
        FROM `{{project_id}}.{{practices_views_dataset}}.client_record_archive_materialized` client_records
    )

    SELECT
        person_id,
        state_code,
        person_external_id,
        timestamp,
        session_id,
        user_external_id,
        {','.join([f"events.{c}" for c in add_columns])},
    FROM (
        SELECT 
            -- dedupes events loaded more than once
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) AS __row_number, 
            -- default columns for all views
            client_id,
            timestamp,
            session_id,
            context_page_url,
            user_id,
            {','.join(add_columns)},
        FROM `{{project_id}}.{{segment_dataset}}.{table_name}`
        -- events from prod deployment only
        WHERE context_page_url LIKE '%://dashboard.recidiviz.org/%'
    ) events
    -- inner join to filter out recidiviz users and others unidentified (if any)
    INNER JOIN `{{project_id}}.{{reference_views_dataset}}.us_tn_reidentified_users` users 
        USING (user_id)
    LEFT JOIN clients USING (client_id)
    WHERE __row_number = 1
    """
