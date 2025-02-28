# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
View that combines and sessionizes attributes about incarceration staff with active
caseloads.
"""
from typing import Dict, List

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

_VIEW_NAME = "incarceration_staff_attribute_sessions"

_VIEW_DESCRIPTION = """
View that combines and sessionizes attributes about incarceration staff with active
caseloads.
"""

# All dictionary values below should specify a list of values by which to sort rows for deduplication.
# All columns referenced in a given list should be queryable within the `sub_sessions_dedup` cte below.
_INCARCERATION_STAFF_ATTRIBUTES_NO_OVERLAPS: Dict[str, List[str]] = {
    "facility": [],
    "facility_inferred": [],
    "is_incarceration_staff": [],
}

_INCARCERATION_STAFF_ATTRIBUTES_WITH_OVERLAPS: Dict[str, List[str]] = {
    "role_subtype": ["COALESCE(role_subtype_priority, 99)"],
    "role_type": [],
}

_QUERY_TEMPLATE = f"""
WITH all_staff_attribute_periods AS (
    -- is_incarceration_staff if has a caseload assigned
    SELECT
        state_code,
        incarceration_staff_id AS staff_id,
        start_date,
        end_date,
        TRUE AS is_incarceration_staff,
        NULL AS facility,
        NULL AS facility_inferred,
        NULL AS role_type,
        NULL AS role_subtype,
    FROM
        `{{project_id}}.sessions.incarceration_staff_caseload_count_spans_materialized`
    WHERE caseload_count > 0

    UNION ALL

    -- location periods
    SELECT
        state_code,
        staff_id,
        start_date,
        end_date,
        NULL AS is_incarceration_staff,
        JSON_EXTRACT_SCALAR(location_metadata, "$.facility") AS facility,
        NULL AS facility_inferred,
        NULL AS role_type,
        NULL AS role_subtype,
    FROM
        `{{project_id}}.normalized_state.state_staff_location_period` a
    LEFT JOIN
        `{{project_id}}.reference_views.location_metadata_materialized` b
    USING
        (state_code, location_external_id)

    UNION ALL

    -- inferred location periods (based on resident locations)
    SELECT
        a.state_code,
        incarceration_staff_id AS staff_id,
        a.start_date,
        a.end_date_exclusive AS end_date,
        NULL AS is_incarceration_staff,
        NULL AS facility,
        primary_facility AS facility_inferred,
        NULL AS role_type,
        NULL AS role_subtype,
    FROM
        `{{project_id}}.sessions.incarceration_staff_inferred_location_sessions_materialized` a

    UNION ALL

    -- role periods
    SELECT
        state_code,
        staff_id,
        start_date,
        end_date,
        NULL AS is_incarceration_staff,
        NULL AS facility,
        NULL AS facility_inferred,
        role_type,
        role_subtype,
    FROM
        `{{project_id}}.normalized_state.state_staff_role_period`
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
            table_columns_with_priority_columns=_INCARCERATION_STAFF_ATTRIBUTES_NO_OVERLAPS, 
            partition_columns=["state_code", "staff_id", "start_date"],
        )},
        -- For attributes that might have overlap, dedup via the configured priority order and suffix with "_primary"
        {generate_largest_value_query_fragment(
            table_columns_with_priority_columns=_INCARCERATION_STAFF_ATTRIBUTES_WITH_OVERLAPS, 
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
        {list_to_query_string(
            [
                f"ARRAY_AGG(DISTINCT {attr} IGNORE NULLS ORDER BY {attr}) AS {attr}_array"
                for attr in _INCARCERATION_STAFF_ATTRIBUTES_WITH_OVERLAPS
            ]
        )}
    FROM
        sub_sessions_with_attributes
    -- Remove zero-day sessions
    WHERE
        start_date < {nonnull_end_date_clause("end_date")}
    GROUP BY 1, 2, 3, 4
)
SELECT
    a.*,
    {list_to_query_string([f"c.{attr}_array" for attr in _INCARCERATION_STAFF_ATTRIBUTES_WITH_OVERLAPS])},
FROM
    sub_sessions_dedup a
LEFT JOIN
    attribute_arrays c
USING
    (state_code, staff_id, start_date)
WHERE
    is_incarceration_staff
"""

INCARCERATION_STAFF_ATTRIBUTE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "staff_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_STAFF_ATTRIBUTE_SESSIONS_VIEW_BUILDER.build_and_print()
