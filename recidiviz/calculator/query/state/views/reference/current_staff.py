# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""View containing attributes of current staff members in a state."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    get_pseudonymized_id_query_str,
    today_between_start_date_and_nullable_end_date_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CURRENT_STAFF_VIEW_NAME = "current_staff"

CURRENT_STAFF_DESCRIPTION = (
    """View containing attributes of current staff members in a state."""
)

CURRENT_STAFF_QUERY_TEMPLATE = f"""
    SELECT
        ss.state_code,
        LOWER(email) as email,
        CASE
            -- Transform MI district IDs from "10 - CENTRAL" --> "10" and "4A" --> "4a" so they
            -- match the districts in the current staff record / admin panel, which makes it easier
            -- to compare the change adding this to what currently exists.
            -- TODO(#25398) Remove this transformation
            WHEN ss.state_code="US_MI" THEN SPLIT(LOWER(JSON_VALUE(location_metadata.location_metadata, "$.supervision_district_id")), " ")[SAFE_OFFSET(0)]
            ELSE JSON_VALUE(location_metadata.location_metadata, "$.supervision_district_id")
            END AS supervision_district_id,
        JSON_VALUE(location_metadata.location_metadata, "$.supervision_district_name") AS supervision_district_name,
        id.external_id AS external_id,
        JSON_VALUE(full_name, "$.given_names") AS given_names,
        JSON_VALUE(full_name, "$.surname") AS surname,
        officer_sessions.supervising_officer_external_id IS NOT NULL AS is_supervision_officer,
        supervisors_of_officers.supervisor_staff_external_id IS NOT NULL AS is_supervision_officer_supervisor,
        {get_pseudonymized_id_query_str("ss.state_code || id.external_id")} AS pseudonymized_id,
    FROM `{{project_id}}.normalized_state.state_staff` ss
    LEFT JOIN (
        SELECT state_code, staff_id, location_external_id, start_date
        FROM `{{project_id}}.normalized_state.state_staff_location_period`
        WHERE {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
    ) location
        USING (state_code, staff_id)
    LEFT JOIN (
        SELECT state_code, staff_id, role_type, role_subtype, start_date
        FROM `{{project_id}}.normalized_state.state_staff_role_period`
        WHERE {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
    ) role_period
        USING (state_code, staff_id)
    LEFT JOIN `{{project_id}}.normalized_state.state_staff_external_id` id
        USING (state_code, staff_id)
    LEFT JOIN `{{project_id}}.reference_views.location_metadata_materialized` location_metadata
        USING (state_code, location_external_id)
    LEFT JOIN (
        SELECT DISTINCT state_code, supervising_officer_external_id
        FROM `{{project_id}}.sessions.supervision_officer_sessions_materialized`
        WHERE {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
    ) officer_sessions
        ON ss.state_code = officer_sessions.state_code
        AND id.external_id = officer_sessions.supervising_officer_external_id
    LEFT JOIN (
        SELECT DISTINCT sp.state_code, supervisor_staff_external_id
        FROM `{{project_id}}.normalized_state.state_staff_supervisor_period` sp
        INNER JOIN `{{project_id}}.normalized_state.state_staff_external_id` sid
            USING (state_code, staff_id)
        INNER JOIN `{{project_id}}.sessions.supervision_officer_sessions_materialized` os
            ON sid.state_code = os.state_code
            AND sid.external_id = os.supervising_officer_external_id
        WHERE
            {today_between_start_date_and_nullable_end_date_clause("sp.start_date", "sp.end_date")}
            AND {today_between_start_date_and_nullable_end_date_clause("os.start_date", "os.end_date")}
    ) supervisors_of_officers
        ON ss.state_code = supervisors_of_officers.state_code
        AND id.external_id = supervisors_of_officers.supervisor_staff_external_id
    WHERE
        -- Only include users who we can identify as likely supervision staff so we don't end up
        -- with completely irrelevant users in the admin panel or our product DBs. We don't
        -- necessarily trust that these role subtypes accurately describe someone's job function
        -- (we use supervision_officer_sessions and state_staff_supervisor_period for that) so we
        -- only use it to limit the number of people we might include instead of determining access
        -- based on it.
        (
            role_period.role_type IN ("SUPERVISION_OFFICER")
            OR role_period.role_subtype IN ("SUPERVISION_OFFICER", "SUPERVISION_OFFICER_SUPERVISOR", "SUPERVISION_DISTRICT_MANAGER", "SUPERVISION_REGIONAL_MANAGER", "SUPERVISION_STATE_LEADERSHIP")
            OR officer_sessions.supervising_officer_external_id IS NOT NULL -- is_supervision_officer
            OR supervisors_of_officers.supervisor_staff_external_id IS NOT NULL -- is_supervision_officer_supervisor
        )
        AND CASE ss.state_code
            WHEN "US_MI" THEN id_type = "US_MI_OMNI_USER"
            WHEN "US_IX" THEN id_type = "US_IX_EMPLOYEE"
            ELSE TRUE
            END
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY state_code, staff_id
        ORDER BY
            location.start_date DESC,
            role_period.start_date DESC,
            is_supervision_officer_supervisor DESC,
            is_supervision_officer DESC,
            external_id DESC
    ) = 1
"""


CURRENT_STAFF_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=CURRENT_STAFF_VIEW_NAME,
    view_query_template=CURRENT_STAFF_QUERY_TEMPLATE,
    description=CURRENT_STAFF_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CURRENT_STAFF_VIEW_BUILDER.build_and_print()
