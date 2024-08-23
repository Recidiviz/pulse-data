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
    WITH attribute_sessions_mapped_ids AS (
        -- Some staff members have multiple valid external IDs for a single external ID type,
        -- so use `agent_multiple_ids_map` to pick a single consistent ID for use in products.
        -- The workflows client record also uses this transformation, so this assures we can join to
        -- it correctly in the workflows staff record.
        SELECT
            attrs.* EXCEPT (officer_id),
            COALESCE(map.external_id_mapped, attrs.officer_id) AS officer_id
        FROM `{{project_id}}.sessions.supervision_staff_attribute_sessions_materialized` attrs
        LEFT JOIN `{{project_id}}.static_reference_tables.agent_multiple_ids_map` map
            ON attrs.state_code = map.state_code
            AND attrs.officer_id = map.external_id_to_map
    )
    SELECT
        ss.state_code,
        email,
        CASE
            -- Transform IX districts from the Atlas format to the pre-Atlas format so they
            -- match the districts in the current staff record / admin panel, which makes it
            -- easier to compare the change adding this to what currently exists and reduces
            -- the number of changes that need to be coordinated at the moment.
            -- TODO(#29474) Remove this transformation
            WHEN attrs.state_code="US_IX" THEN 
                CASE UPPER(attrs.supervision_district_name)
                    WHEN "DISTRICT 1" THEN "DISTRICT OFFICE 1, COEUR D'ALENE"
                    WHEN "DISTRICT 2" THEN "DISTRICT OFFICE 2, LEWISTON"
                    WHEN "DISTRICT 3" THEN "DISTRICT OFFICE 3, CALDWELL"
                    WHEN "DISTRICT 4" THEN "DISTRICT OFFICE 4, BOISE"
                    WHEN "DISTRICT 5" THEN "DISTRICT OFFICE 5, TWIN FALLS"
                    WHEN "DISTRICT 6" THEN "DISTRICT OFFICE 6, POCATELLO"
                    WHEN "DISTRICT 7" THEN "DISTRICT OFFICE 7, IDAHO FALLS"
                END
            -- Transform MI district IDs from "10 - CENTRAL" --> "10" and "4A" --> "4a" so they
            -- match the districts in the current staff record / admin panel, which makes it easier
            -- to compare the change adding this to what currently exists.
            -- TODO(#25398) Remove this transformation
            WHEN attrs.state_code="US_MI" THEN SPLIT(LOWER(attrs.supervision_district_id), " ")[SAFE_OFFSET(0)]
            WHEN attrs.state_code IN ("US_CA", "US_ND") THEN
                COALESCE(attrs.supervision_office_name, attrs.supervision_office_name_inferred)
            -- Set ME districts to null so they match the districts in the admin panel
            -- Set OR districts to null because "staff" in this case are actually caseloads,
            -- and districts are not relevant.
            WHEN attrs.state_code IN ("US_ME", "US_OR") THEN NULL
            ELSE attrs.supervision_district_id
        END AS district,
        UPPER(attrs.officer_id) AS external_id,
        JSON_VALUE(ss.full_name, "$.given_names") AS given_names,
        JSON_VALUE(ss.full_name, "$.surname") AS surname,
        attrs.role_subtype_primary,
        attrs.supervisor_staff_external_id_primary AS supervisor_external_id,
        attrs.supervisor_staff_external_id_array AS supervisor_external_ids,
        officer_sessions.supervising_officer_external_id IS NOT NULL AS is_supervision_officer,
        supervisors_of_officers.supervisor_staff_external_id IS NOT NULL AS is_supervision_officer_supervisor,
        {get_pseudonymized_id_query_str("IF(ss.state_code = 'US_IX', 'US_ID', ss.state_code) || attrs.officer_id")} AS pseudonymized_id,
    FROM `{{project_id}}.normalized_state.state_staff` ss
    LEFT JOIN `{{project_id}}.sessions.supervision_staff_attribute_sessions_materialized` attrs
        USING (staff_id)
    LEFT JOIN (
        SELECT DISTINCT state_code, supervising_officer_external_id
        FROM `{{project_id}}.sessions.supervision_officer_sessions_materialized`
        WHERE {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
    ) officer_sessions
        ON ss.state_code = officer_sessions.state_code
        AND attrs.officer_id = officer_sessions.supervising_officer_external_id
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
        AND attrs.officer_id = supervisors_of_officers.supervisor_staff_external_id
    WHERE
        -- Only include users who we can identify as likely supervision staff so we don't end up
        -- with completely irrelevant users in the admin panel or our product DBs. We don't
        -- necessarily trust that these role subtypes accurately describe someone's job function
        -- (we use supervision_officer_sessions and state_staff_supervisor_period for that) so we
        -- only use it to limit the number of people we might include instead of determining access
        -- based on it.
        (
            "SUPERVISION_OFFICER" IN UNNEST(attrs.role_type_array)
            OR (SELECT COUNT(1) FROM (SELECT * FROM UNNEST(attrs.role_subtype_array) INTERSECT DISTINCT SELECT * FROM UNNEST(ARRAY["SUPERVISION_OFFICER", "SUPERVISION_OFFICER_SUPERVISOR", "SUPERVISION_DISTRICT_MANAGER", "SUPERVISION_REGIONAL_MANAGER", "SUPERVISION_STATE_LEADERSHIP"]))) >= 1
            OR officer_sessions.supervising_officer_external_id IS NOT NULL -- is_supervision_officer
            OR supervisors_of_officers.supervisor_staff_external_id IS NOT NULL -- is_supervision_officer_supervisor
        )
        AND {today_between_start_date_and_nullable_end_date_clause("attrs.start_date", "attrs.end_date_exclusive")}
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
