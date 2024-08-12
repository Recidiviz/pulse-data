#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
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
"""View to prepare supervision staff records for Workflows for export to the frontend."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import (
    today_between_start_date_and_nullable_end_date_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_STAFF_RECORD_VIEW_NAME = "supervision_staff_record"

SUPERVISION_STAFF_RECORD_DESCRIPTION = """
    Supervision staff records to be exported to Firestore to power Workflows.
    """

SUPERVISION_STAFF_RECORD_QUERY_TEMPLATE = f"""
    WITH
        caseload_staff_external_ids AS (
            SELECT DISTINCT
                state_code,
                officer_id AS external_id,
            FROM `{{project_id}}.workflows_views.client_record_materialized` client
        )
        , attribute_sessions_mapped_ids AS (
            -- Some staff members have multiple valid external IDs for a single external ID type,
            -- so use `agent_multiple_ids_map` to pick a single consistent ID for use in workflows.
            -- The client record also uses this transformation, so this assures we can join to it
            -- correctly.
            SELECT
                attrs.* EXCEPT (officer_id),
                COALESCE(map.external_id_mapped, attrs.officer_id) AS officer_id
            FROM `{{project_id}}.sessions.supervision_staff_attribute_sessions_materialized` attrs
            LEFT JOIN `{{project_id}}.static_reference_tables.agent_multiple_ids_map` map
                ON attrs.state_code = map.state_code
                AND attrs.officer_id = map.external_id_to_map
        )
        , staff_query AS (
            SELECT
                UPPER(ids.external_id) AS id,
                ids.state_code,
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
                ss.email,
                JSON_VALUE(ss.full_name, "$.given_names") AS given_names,
                JSON_VALUE(ss.full_name, "$.surname") AS surname,
                IF(attrs.state_code = "US_CA", attrs.role_subtype_primary, CAST(NULL AS STRING)) AS role_subtype,
                attrs.supervisor_staff_external_id_primary AS supervisor_external_id,
                attrs.supervisor_staff_external_id_array AS supervisor_external_ids,
            FROM caseload_staff_external_ids ids
            LEFT JOIN attribute_sessions_mapped_ids attrs
                ON ids.external_id = attrs.officer_id
                AND ids.state_code = attrs.state_code
                AND {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date_exclusive")}
            LEFT JOIN `{{project_id}}.normalized_state.state_staff` ss
                USING (staff_id)
        )

    SELECT {{columns}}
    FROM staff_query
"""

SUPERVISION_STAFF_RECORD_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=SUPERVISION_STAFF_RECORD_VIEW_NAME,
    view_query_template=SUPERVISION_STAFF_RECORD_QUERY_TEMPLATE,
    description=SUPERVISION_STAFF_RECORD_DESCRIPTION,
    columns=[
        "id",
        "state_code",
        "district",
        "email",
        "given_names",
        "surname",
        "role_subtype",
        "supervisor_external_id",
        "supervisor_external_ids",
    ],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_STAFF_RECORD_VIEW_BUILDER.build_and_print()
