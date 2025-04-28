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
from typing import Dict, List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
    generate_largest_value_query_fragment,
    nonnull_end_date_clause,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_STAFF_ATTRIBUTE_SESSIONS_VIEW_NAME = "supervision_staff_attribute_sessions"

SUPERVISION_STAFF_ATTRIBUTE_SESSIONS_VIEW_DESCRIPTION = """
View that preprocesses state staff periods to extract relevant attributes and external id's.
"""

# All dictionary values below should specify a list of values by which to sort rows for deduplication.
# All columns referenced in a given list should be queryable within the `sub_sessions_dedup` cte below.
_SUPERVISION_STAFF_ATTRIBUTES_NO_OVERLAPS: Dict[str, List[str]] = {
    "supervision_district_id": [],
    "supervision_district_name": [],
    "supervision_office_id": [],
    "supervision_office_name": [],
    "supervision_unit": [],
    "supervision_unit_name": [],
    "supervision_district_id_inferred": [],
    "supervision_district_name_inferred": [],
    "supervision_office_id_inferred": [],
    "supervision_office_name_inferred": [],
    "is_supervision_officer": [],
    "is_supervision_officer_supervisor": [],
    "is_in_critically_understaffed_location": [],
}

_SUPERVISION_STAFF_ATTRIBUTES_WITH_OVERLAPS: Dict[str, List[str]] = {
    "role_subtype": ["COALESCE(role_subtype_priority, 99)"],
    "role_type": [],
    "specialized_caseload_type": ['specialized_caseload_type = "TRANSITIONAL"'],
    "supervisor_staff_external_id": [
        "supervisor_staff_external_id",
        "supervisor_staff_id",
    ],
    "supervisor_staff_id": ["supervisor_staff_external_id", "supervisor_staff_id"],
}

# Must be a subset of the keys of _SUPERVISION_STAFF_ATTRIBUTES_WITH_OVERLAPS
_SUPERVISION_STAFF_ATTRIBUTES_WITH_OVERLAPS_NUMERIC = ["supervisor_staff_id"]

SUPERVISION_STAFF_ATTRIBUTE_SESSIONS_QUERY_TEMPLATE = f"""
WITH all_staff_attribute_periods AS (
    -- location periods
    SELECT
        state_code,
        staff_id,
        start_date,
        end_date,
        JSON_EXTRACT_SCALAR(location_metadata, "$.supervision_district_id") AS supervision_district_id,
        JSON_EXTRACT_SCALAR(location_metadata, "$.supervision_district_name") AS supervision_district_name,
        JSON_EXTRACT_SCALAR(location_metadata, "$.supervision_office_id") AS supervision_office_id,
        JSON_EXTRACT_SCALAR(location_metadata, "$.supervision_office_name") AS supervision_office_name,
        JSON_EXTRACT_SCALAR(location_metadata, "$.supervision_unit_id") AS supervision_unit,
        JSON_EXTRACT_SCALAR(location_metadata, "$.supervision_unit_name") AS supervision_unit_name,
        NULL AS supervision_district_id_inferred,
        NULL AS supervision_district_name_inferred,
        NULL AS supervision_office_id_inferred,
        NULL AS supervision_office_name_inferred,
        NULL AS role_type,
        NULL AS role_subtype,
        NULL AS specialized_caseload_type,
        NULL AS supervisor_staff_external_id,
        NULL AS supervisor_staff_id,
        NULL AS is_supervision_officer,
        NULL AS is_supervision_officer_supervisor,
        NULL AS is_in_critically_understaffed_location,
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
        NULL AS supervision_district_id,
        NULL AS supervision_district_name,
        NULL AS supervision_office_id,
        NULL AS supervision_office_name,
        NULL AS supervision_unit,
        NULL AS supervision_unit_name,
        a.primary_district AS supervision_district_id_inferred,
        a.primary_district_name AS supervision_district_name_inferred,
        a.primary_office AS supervision_office_id_inferred,
        a.primary_office_name AS supervision_office_name_inferred,
        NULL AS role_type,
        NULL AS role_subtype,
        NULL AS specialized_caseload_type,
        NULL AS supervisor_staff_external_id,
        NULL AS supervisor_staff_id,
        NULL AS is_supervision_officer,
        NULL AS is_supervision_officer_supervisor,
        NULL AS is_in_critically_understaffed_location,
    FROM
        `{{project_id}}.sessions.supervision_officer_inferred_location_sessions_materialized` a
    INNER JOIN
        `{{project_id}}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` b
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
        NULL AS supervision_district_id,
        NULL AS supervision_district_name,
        NULL AS supervision_office_id,
        NULL AS supervision_office_name,
        NULL AS supervision_unit,
        NULL AS supervision_unit_name,
        NULL AS supervision_district_id_inferred,
        NULL AS supervision_district_name_inferred,
        NULL AS supervision_office_id_inferred,
        NULL AS supervision_office_name_inferred,
        role_type,
        role_subtype,
        NULL AS specialized_caseload_type,
        NULL AS supervisor_staff_external_id,
        NULL AS supervisor_staff_id,
        NULL AS is_supervision_officer,
        NULL AS is_supervision_officer_supervisor,
        NULL AS is_in_critically_understaffed_location,
    FROM
        `{{project_id}}.normalized_state.state_staff_role_period`

    UNION ALL

    -- specialized caseload type periods
    SELECT
        state_code,
        staff_id,
        start_date,
        end_date,
        NULL AS supervision_district_id,
        NULL AS supervision_district_name,
        NULL AS supervision_office_id,
        NULL AS supervision_office_name,
        NULL AS supervision_unit,
        NULL AS supervision_unit_name,
        NULL AS supervision_district_id_inferred,
        NULL AS supervision_district_name_inferred,
        NULL AS supervision_office_id_inferred,
        NULL AS supervision_office_name_inferred,
        NULL AS role_type,
        NULL AS role_subtype,
        caseload_type AS specialized_caseload_type,
        NULL AS supervisor_staff_external_id,
        NULL AS supervisor_staff_id,
        NULL AS is_supervision_officer,
        NULL AS is_supervision_officer_supervisor,
        NULL AS is_in_critically_understaffed_location,
    FROM
        `{{project_id}}.normalized_state.state_staff_caseload_type_period`

    UNION ALL

    -- supervisor periods
    SELECT
        a.state_code,
        a.staff_id,
        a.start_date,
        a.end_date,
        NULL AS supervision_district_id,
        NULL AS supervision_district_name,
        NULL AS supervision_office_id,
        NULL AS supervision_office_name,
        NULL AS supervision_unit,
        NULL AS supervision_unit_name,
        NULL AS supervision_district_id_inferred,
        NULL AS supervision_district_name_inferred,
        NULL AS supervision_office_id_inferred,
        NULL AS supervision_office_name_inferred,
        NULL AS role_type,
        NULL AS role_subtype,
        NULL AS specialized_caseload_type,
        a.supervisor_staff_external_id,
        b.staff_id AS supervisor_staff_id,
        NULL AS is_supervision_officer,
        NULL AS is_supervision_officer_supervisor,
        NULL AS is_in_critically_understaffed_location,
    FROM
        `{{project_id}}.normalized_state.state_staff_supervisor_period` a
    INNER JOIN
        `{{project_id}}.normalized_state.state_staff_external_id` b
    ON
        a.state_code = b.state_code
        AND a.supervisor_staff_external_id = b.external_id
        AND a.supervisor_staff_external_id_type = b.id_type
    
    UNION ALL

    -- periods where the staff member is or is not a supervision officer
    SELECT
        a.state_code,
        b.staff_id,
        a.start_date,
        a.end_date,
        NULL AS supervision_district_id,
        NULL AS supervision_district_name,
        NULL AS supervision_office_id,
        NULL AS supervision_office_name,
        NULL AS supervision_unit,
        NULL AS supervision_unit_name,
        NULL AS supervision_district_id_inferred,
        NULL AS supervision_district_name_inferred,
        NULL AS supervision_office_id_inferred,
        NULL AS supervision_office_name_inferred,
        NULL AS role_type,
        NULL AS role_subtype,
        NULL AS specialized_caseload_type,
        NULL AS supervisor_staff_external_id,
        NULL AS supervisor_staff_id,
        a.caseload_count > 0 AS is_supervision_officer,
        NULL AS is_supervision_officer_supervisor,
        NULL AS is_in_critically_understaffed_location,
    FROM `{{project_id}}.aggregated_metrics.supervision_officer_caseload_count_spans_materialized` a
    INNER JOIN
        `{{project_id}}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` b
    ON
        #TODO(#21702): Replace join with `staff_id` once refactor is complete
        a.state_code = b.state_code
        AND a.officer_id = b.external_id
    
    UNION ALL

    -- periods where the staff member does or does not supervise a supervision officer
    SELECT
        a.state_code,
        b.staff_id,
        a.start_date,
        a.end_date_exclusive AS end_date,
        NULL AS supervision_district_id,
        NULL AS supervision_district_name,
        NULL AS supervision_office_id,
        NULL AS supervision_office_name,
        NULL AS supervision_unit,
        NULL AS supervision_unit_name,
        NULL AS supervision_district_id_inferred,
        NULL AS supervision_district_name_inferred,
        NULL AS supervision_office_id_inferred,
        NULL AS supervision_office_name_inferred,
        NULL AS role_type,
        NULL AS role_subtype,
        NULL AS specialized_caseload_type,
        NULL AS supervisor_staff_external_id,
        NULL AS supervisor_staff_id,
        NULL AS is_supervision_officer,
        TRUE AS is_supervision_officer_supervisor,
        NULL AS is_in_critically_understaffed_location,
    FROM `{{project_id}}.sessions.supervisor_of_officer_sessions_materialized` a
    INNER JOIN
        `{{project_id}}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` b
    ON
        #TODO(#21702): Replace join with `staff_id` once refactor is complete
        a.state_code = b.state_code
        AND a.staff_external_id = b.external_id

    UNION ALL

    -- critically understaffed locations
    SELECT
        state_code,
        staff_id,
        start_date,
        end_date_exclusive AS end_date,
        NULL AS supervision_district_id,
        NULL AS supervision_district_name,
        NULL AS supervision_office_id,
        NULL AS supervision_office_name,
        NULL AS supervision_unit,
        NULL AS supervision_unit_name,
        NULL AS supervision_district_id_inferred,
        NULL AS supervision_district_name_inferred,
        NULL AS supervision_office_id_inferred,
        NULL AS supervision_office_name_inferred,
        NULL AS role_type,
        NULL AS role_subtype,
        NULL AS specialized_caseload_type,
        NULL AS supervisor_staff_external_id,
        NULL AS supervisor_staff_id,
        NULL AS is_supervision_officer,
        NULL AS is_supervision_officer_supervisor,
        TRUE AS is_in_critically_understaffed_location,
    FROM
        `{{project_id}}.analyst_data.supervision_staff_in_critically_understaffed_location_sessions_preprocessed_materialized`
)
,
{create_sub_sessions_with_attributes(table_name="all_staff_attribute_periods",index_columns=["state_code","staff_id"])}
,
-- Dedupes to the max non-null value by default. In theory all state staff periods should be non-overlapping
-- on a single attribute, so the ordered deduplication is just an extra safeguard.
sub_sessions_dedup AS (
    SELECT
        state_code,
        staff_id,
        start_date,
        end_date AS end_date_exclusive,
        -- Apply an arbitrary dedup to attributes that we don't expect to overlap, mostly as an added protection
        {generate_largest_value_query_fragment(
            table_columns_with_priority_columns=_SUPERVISION_STAFF_ATTRIBUTES_NO_OVERLAPS, 
            partition_columns=["state_code", "staff_id", "start_date"],
        )},
        -- For attributes that might have overlap, dedup via the configured priority order and suffix with "_primary"
        {generate_largest_value_query_fragment(
            table_columns_with_priority_columns=_SUPERVISION_STAFF_ATTRIBUTES_WITH_OVERLAPS, 
            partition_columns=["state_code", "staff_id", "start_date"],
            column_suffix="_primary"
        )},
    FROM
        sub_sessions_with_attributes
    LEFT JOIN `{{project_id}}.sessions.state_staff_role_subtype_dedup_priority` subtype
        USING (role_subtype)
    -- Remove zero-day sessions
    WHERE
        start_date < {nonnull_end_date_clause("end_date")}
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY state_code, staff_id, start_date) = 1
)
,
-- Aggregates arrays of all staff attributes where overlaps could be present
attribute_arrays AS (
    SELECT
        state_code,
        staff_id,
        start_date,
        end_date AS end_date_exclusive,
        -- Aggregate them into sorted JSON strings to make it possible to aggregate adjacent spans
        -- after.
        {list_to_query_string(
            [
                f"TO_JSON_STRING(ARRAY_AGG(DISTINCT {attr} IGNORE NULLS ORDER BY {attr})) AS {attr}_json_array"
                for attr in _SUPERVISION_STAFF_ATTRIBUTES_WITH_OVERLAPS
            ]
        )}
    FROM
        sub_sessions_with_attributes
    -- Remove zero-day sessions
    WHERE
        start_date < {nonnull_end_date_clause("end_date")}
    GROUP BY 1, 2, 3, 4
),
full_staff_data AS (
    SELECT
        b.external_id AS officer_id,
        a.*,
        {list_to_query_string([f"c.{attr}_json_array" for attr in _SUPERVISION_STAFF_ATTRIBUTES_WITH_OVERLAPS])},
    FROM
        sub_sessions_dedup a
    LEFT JOIN
        `{{project_id}}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` b
    USING
        (state_code, staff_id)
    LEFT JOIN
        attribute_arrays c
    USING
        (state_code, staff_id, start_date)
),
aggregated_data AS (
    {aggregate_adjacent_spans(
        table_name="full_staff_data",
        index_columns=["state_code", "staff_id", "officer_id"],
        attribute=list(_SUPERVISION_STAFF_ATTRIBUTES_NO_OVERLAPS)
            + [f"{attr}_primary" for attr in _SUPERVISION_STAFF_ATTRIBUTES_WITH_OVERLAPS]
            + [f"{attr}_json_array" for attr in _SUPERVISION_STAFF_ATTRIBUTES_WITH_OVERLAPS],
        end_date_field_name='end_date_exclusive'
    )}
)
SELECT
    -- Turn JSON arrays back into regular arrays
    * EXCEPT ({list_to_query_string([f"{attr}_json_array" for attr in _SUPERVISION_STAFF_ATTRIBUTES_WITH_OVERLAPS])}),
    {list_to_query_string(
        [
            f"JSON_VALUE_ARRAY({attr}_json_array) AS {attr}_array"
            for attr in _SUPERVISION_STAFF_ATTRIBUTES_WITH_OVERLAPS if attr not in _SUPERVISION_STAFF_ATTRIBUTES_WITH_OVERLAPS_NUMERIC
        ]
    )},
    {list_to_query_string(
        [
            f"ARRAY(SELECT CAST(element AS INT64) FROM UNNEST(JSON_VALUE_ARRAY({attr}_json_array)) AS element) AS {attr}_array"
            for attr in _SUPERVISION_STAFF_ATTRIBUTES_WITH_OVERLAPS_NUMERIC
        ]
    )},
FROM aggregated_data
"""

SUPERVISION_STAFF_ATTRIBUTE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SUPERVISION_STAFF_ATTRIBUTE_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_STAFF_ATTRIBUTE_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISION_STAFF_ATTRIBUTE_SESSIONS_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "staff_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_STAFF_ATTRIBUTE_SESSIONS_VIEW_BUILDER.build_and_print()
