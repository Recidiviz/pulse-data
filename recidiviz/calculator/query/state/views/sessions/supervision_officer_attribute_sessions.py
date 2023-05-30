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
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
    generate_largest_value_query_fragment,
    list_to_query_string,
    nonnull_end_date_clause,
)
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    REFERENCE_VIEWS_DATASET,
    SESSIONS_DATASET,
)
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
    "role_type",
    "role_subtype",
    "specialized_caseload_type",
    "supervisor_staff_external_id",
]

# Specify states that do not have unit location information, where we'll use staff supervisor information instead
_STATES_WITH_UNIT_SUPERVISOR_OVERRIDE = ["US_ND"]

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
        NULL AS role_type,
        NULL AS role_subtype,
        NULL AS specialized_caseload_type,
        NULL AS supervisor_staff_external_id,
    FROM
        `{{project_id}}.{{normalized_state_dataset}}.state_staff_location_period` a
    LEFT JOIN
        `{{project_id}}.{{reference_views_dataset}}.location_metadata_materialized` b
    USING
        (state_code, location_external_id)

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
        role_type,
        role_subtype,
        NULL AS specialized_caseload_type,
        NULL AS supervisor_staff_external_id,
    FROM
        `{{project_id}}.{{normalized_state_dataset}}.state_staff_role_period`

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
        NULL AS role_type,
        NULL AS role_subtype,
        state_staff_specialized_caseload_type AS specialized_caseload_type,
        NULL AS supervisor_staff_external_id,
    FROM
        `{{project_id}}.{{normalized_state_dataset}}.state_staff_caseload_type_period`

    UNION ALL

    -- supervisor periods
    SELECT
        state_code,
        staff_id,
        start_date,
        end_date,
        NULL AS supervision_district,
        NULL AS supervision_office,
        NULL AS supervision_unit,
        NULL AS supervision_unit_name,
        NULL AS role_type,
        NULL AS role_subtype,
        NULL AS specialized_caseload_type,
        supervisor_staff_external_id,
    FROM
        `{{project_id}}.{{normalized_state_dataset}}.state_staff_supervisor_period`
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
    FROM
        sub_sessions_with_attributes
    -- Remove zero-day sessions
    WHERE
        start_date < {nonnull_end_date_clause("end_date")}
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY state_code, staff_id, start_date) = 1
)
SELECT
    b.external_id AS officer_id, 
    a.* EXCEPT (supervision_unit),
    -- Substitute unit location with unit supervisor in states where only supervisor is hydrated
    IF(state_code IN ({list_to_query_string(_STATES_WITH_UNIT_SUPERVISOR_OVERRIDE, quoted=True)}), supervisor_staff_external_id, supervision_unit) AS supervision_unit,
FROM
    sub_sessions_dedup a
LEFT JOIN
    `{{project_id}}.{{normalized_state_dataset}}.state_staff_external_id` b
USING
    (state_code, staff_id)
WHERE
    role_type = "SUPERVISION_OFFICER"
"""

SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_VIEW_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
    clustering_fields=["state_code", "officer_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_VIEW_BUILDER.build_and_print()
