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

"""Query containing state staff role period information."""

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
    WITH 
    -- create preliminary periods based off all update_datetimes
    preliminary_periods AS (
      SELECT DISTINCT
        employee_id AS employee_id_omni,
        -- Grab position text to determine role type and subtype
        --  (There's also an employee_type_id but it doesn't always correspond perfectly with position, so let's use position for now)
        UPPER(position) AS position,
        -- There's also a work_site_id that's slightly more valued, so TODO(#24103) to incorporate that into this view and location_metadata in the
        -- For default_location_id, let's not grab location if location = '0' which means it's a TEMP location and doesn't give us any information
        CASE default_location_id 
            WHEN '0' THEN NULL
            ELSE default_location_id
            END AS default_location_id,
        update_datetime AS start_date,
        LEAD(update_datetime) OVER(PARTITION BY employee_id ORDER BY update_datetime) AS end_date,
        active_flag
      FROM {{ADH_EMPLOYEE@ALL}}
    ),
    -- aggregate adjacent spans if no attribute information has changed
    final_periods AS (
        {aggregate_adjacent_spans(
            table_name="preliminary_periods",
            attribute=["position", "default_location_id","active_flag"],
            index_columns=["employee_id_omni"])}
    )
    -- finally, only keep periods where the employee was active and where we get either position or location information
    SELECT
        employee_id_omni,
        position,
        default_location_id,
        start_date,
        end_date,
        ROW_NUMBER() OVER(PARTITION BY employee_id_omni ORDER BY start_date, end_date NULLS LAST) AS period_id
    FROM final_periods
    WHERE active_flag = '1' 
    AND (position IS NOT NULL OR default_location_id IS NOT NULL)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="state_staff_role_location_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="employee_id_omni, period_id",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
