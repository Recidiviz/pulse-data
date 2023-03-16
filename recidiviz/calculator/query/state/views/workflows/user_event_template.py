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
"""Helpers for querying user events logged from Workflows"""
from typing import List, Optional

# The first US_IX export for workflows was on 1/11 in staging and 1/17 in prod.
# For simplicity, use the prod date.
first_ix_export_date = "2023-01-17"


def user_event_template(
    table_name: str,
    add_columns: Optional[List[str]] = None,
    should_check_client_id: bool = True,
) -> str:
    if add_columns is None:
        add_columns = []

    # TODO(#17297): Remove frontend_referral_form_copied_to_clipboard conditional once these events are fired with justice_involved_person_id
    return f"""
    WITH 
    -- reidentifies clients from hash
    clients AS (
        SELECT DISTINCT
            person_id,
            person_external_id,
            pseudonymized_id,
            -- Join on state_code because we want to be able to distinguish between US_ID and US_IX person_ids
            state_code,
        FROM `{{project_id}}.{{workflows_views_dataset}}.client_record_archive_materialized` client_records

        UNION ALL

        SELECT DISTINCT
            person_id,
            person_external_id,
            pseudonymized_id,
            state_code,
        FROM `{{project_id}}.{{workflows_views_dataset}}.resident_record_archive_materialized` resident_records
    )

    SELECT
        person_id,
        state_code,
        person_external_id,
        timestamp,
        session_id,
        user_external_id,
        district,
        {','.join([f"events.{c}" for c in add_columns])}{',' if add_columns else ''}
    FROM (
        SELECT
            -- default columns for all views
            -- this field was renamed, fall back to previous name for older records
            {"IFNULL(justice_involved_person_id, client_id)" 
                if should_check_client_id
                else "justice_involved_person_id"
            } AS pseudonymized_id,
            timestamp,
            session_id,
            context_page_url,
            user_id,
            {','.join(add_columns)}{',' if add_columns else ''}
        FROM `{{project_id}}.{{segment_dataset}}.{table_name}`
        -- events from prod deployment only
        WHERE context_page_url LIKE '%://dashboard.recidiviz.org/%'
        -- dedupes events loaded more than once
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) = 1
    ) events
    -- inner join to filter out recidiviz users and others unidentified (if any)
    INNER JOIN `{{project_id}}.{{workflows_views_dataset}}.reidentified_dashboard_users`
        USING (user_id)
    INNER JOIN clients USING (state_code, pseudonymized_id)
    -- We get the state_code above from `reidentified_dashboard_users`, which could have have an
    -- entry for a user for both US_ID and US_IX. We can't use the pseudonymized id to distinguish
    -- because they may match between both states. Instead, use the timestamp of the event to
    -- determine whether it is a US_ID event or a US_IX event.
    WHERE state_code != "US_ID" OR timestamp < "{first_ix_export_date}"
    """
