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
"""
View that preprocesses state staff periods to extract relevant attributes and external id's.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
    generate_largest_value_query_fragment,
    nonnull_end_date_clause,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_VIEW_NAME = (
    "supervision_officer_attribute_sessions"
)

SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_VIEW_DESCRIPTION = """
View that preprocesses state staff periods to extract relevant attributes and external id's.
"""

_SUPERVISION_OFFICER_ATTRIBUTES = [
    "supervision_district",
    "supervision_office",
    "supervision_unit",
    "supervision_unit_name",
    "supervision_district_inferred",
    "supervision_office_inferred",
    "specialized_caseload_type",
    "supervisor_staff_external_id",
    "supervisor_staff_id",
    "role_type",
]

SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_QUERY_TEMPLATE = f"""
WITH all_staff_attribute_periods AS (
    -- location periods
    SELECT
        state_code,
        staff_id,
        start_date,
        end_date,
        JSON_EXTRACT_SCALAR(location_metadata, "$.supervision_district_id") AS supervision_district,
        JSON_EXTRACT_SCALAR(location_metadata, "$.supervision_office_id") AS supervision_office,
        JSON_EXTRACT_SCALAR(location_metadata, "$.supervision_unit_id") AS supervision_unit,
        JSON_EXTRACT_SCALAR(location_metadata, "$.supervision_unit_name") AS supervision_unit_name,
        NULL AS supervision_district_inferred,
        NULL AS supervision_office_inferred,
        NULL AS role_type,
        NULL AS role_subtype,
        NULL AS specialized_caseload_type,
        NULL AS supervisor_staff_external_id,
        NULL AS supervisor_staff_id,
    FROM
        `{{project_id}}.normalized_state.state_staff_location_period` a
    LEFT JOIN
        `{{project_id}}.reference_views.location_metadata_materialized` b
    USING
        (state_code, location_external_id)

    UNION ALL

    -- inferred location periods (based on client locations)
    SELECT
        a.state_code,
        b.staff_id,
        a.start_date,
        a.end_date_exclusive AS end_date,
        NULL AS supervision_district,
        NULL AS supervision_office,
        NULL AS supervision_unit,
        NULL AS supervision_unit_name,
        a.primary_district AS supervision_district_inferred,
        a.primary_office AS supervision_office_inferred,
        NULL AS role_type,
        NULL AS role_subtype,
        NULL AS specialized_caseload_type,
        NULL AS supervisor_staff_external_id,
        NULL AS supervisor_staff_id,
    FROM
        `{{project_id}}.sessions.supervision_officer_inferred_location_sessions_materialized` a
    INNER JOIN
        `{{project_id}}.normalized_state.state_staff_external_id` b
    ON
        #TODO(#21702): Replace join with `staff_id` once refactor is complete
        a.state_code = b.state_code
        AND a.supervising_officer_external_id = b.external_id

    UNION ALL

    -- role periods
    SELECT
        state_code,
        staff_id,
        start_date,
        end_date,
        NULL AS supervision_district,
        NULL AS supervision_office,
        NULL AS supervision_unit,
        NULL AS supervision_unit_name,
        NULL AS supervision_district_inferred,
        NULL AS supervision_office_inferred,
        role_type,
        role_subtype,
        NULL AS specialized_caseload_type,
        NULL AS supervisor_staff_external_id,
        NULL AS supervisor_staff_id,
    FROM
        `{{project_id}}.normalized_state.state_staff_role_period`

    UNION ALL

    -- specialized caseload type periods
    SELECT
        state_code,
        staff_id,
        start_date,
        end_date,
        NULL AS supervision_district,
        NULL AS supervision_office,
        NULL AS supervision_unit,
        NULL AS supervision_unit_name,
        NULL AS supervision_district_inferred,
        NULL AS supervision_office_inferred,
        NULL AS role_type,
        NULL AS role_subtype,
        state_staff_specialized_caseload_type AS specialized_caseload_type,
        NULL AS supervisor_staff_external_id,
        NULL AS supervisor_staff_id,
    FROM
        `{{project_id}}.normalized_state.state_staff_caseload_type_period`

    UNION ALL

    -- supervisor periods
    SELECT
        a.state_code,
        a.staff_id,
        a.start_date,
        a.end_date,
        NULL AS supervision_district,
        NULL AS supervision_office,
        NULL AS supervision_unit,
        NULL AS supervision_unit_name,
        NULL AS supervision_district_inferred,
        NULL AS supervision_office_inferred,
        NULL AS role_type,
        NULL AS role_subtype,
        NULL AS specialized_caseload_type,
        a.supervisor_staff_external_id,
        b.staff_id AS supervisor_staff_id,
    FROM
        `{{project_id}}.normalized_state.state_staff_supervisor_period` a
    INNER JOIN
        `{{project_id}}.normalized_state.state_staff_external_id` b
    ON
        a.state_code = b.state_code
        AND a.supervisor_staff_external_id = b.external_id
        AND a.supervisor_staff_external_id_type = b.id_type
)
,
{create_sub_sessions_with_attributes(table_name="all_staff_attribute_periods",index_columns=["state_code","staff_id"])}
,
-- Dedupes to the max non-null value. In theory all state staff periods should be non-overlapping
-- on a single attribute, so the ordered deduplication is just an extra safeguard.
sub_sessions_dedup AS (
    SELECT
        state_code,
        staff_id,
        start_date,
        end_date AS end_date_exclusive,
        {generate_largest_value_query_fragment(
            table_columns=_SUPERVISION_OFFICER_ATTRIBUTES, 
            partition_columns=["state_code", "staff_id", "start_date"]
        )},
        FIRST_VALUE(role_subtype IGNORE NULLS) OVER (
            PARTITION BY {list_to_query_string(["state_code", "staff_id", "start_date"])}
            ORDER BY COALESCE(role_subtype_priority, 99), role_subtype DESC
        ) AS role_subtype
    FROM
        sub_sessions_with_attributes
    LEFT JOIN `{{project_id}}.sessions.state_staff_role_subtype_dedup_priority` subtype
        USING(role_subtype)
    -- Remove zero-day sessions
    WHERE
        start_date < {nonnull_end_date_clause("end_date")}
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY state_code, staff_id, start_date) = 1
)
SELECT
    b.external_id AS officer_id, 
    a.*,
FROM
    sub_sessions_dedup a
LEFT JOIN
    `{{project_id}}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` b
USING
    (staff_id)
WHERE
    role_type = "SUPERVISION_OFFICER"
"""

SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "staff_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_VIEW_BUILDER.build_and_print()
