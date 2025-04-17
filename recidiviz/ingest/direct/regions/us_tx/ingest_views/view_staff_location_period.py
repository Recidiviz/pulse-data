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
"""Generates location periods for active Staff."""
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_aggregate_cte = aggregate_adjacent_spans(
    table_name="pre_aggregated_spans",
    index_columns=["Staff_ID_Number"],
    attribute="Agency_of_Employment",
)

# TODO(#40581) Denote current critically understaffed locations here or in another downstream view
VIEW_QUERY_TEMPLATE = f"""
WITH 
-- Gets every distinct location assignment for each staff member
staff_agency_changes AS (
    SELECT DISTINCT
        Staff_ID_Number,
        DATE(Last_Modified_Date) AS Last_Modified_Date, 
        Agency_of_Employment,
    FROM 
        {{Staff@ALL}}
    WHERE 
        Deleted_Flag = 'ACTIVE'
),
-- Gets the start and end date for each location assignment
pre_aggregated_spans AS (
SELECT
    Staff_ID_Number,
    Last_Modified_Date AS start_date,
    LEAD(Last_Modified_Date) OVER (PARTITION BY Staff_ID_Number ORDER BY Last_Modified_Date) AS end_date,
    Agency_of_Employment,
FROM 
    staff_agency_changes
),
-- Combines spans at the same agency 
aggregate_adjacent_spans AS ({_aggregate_cte})
SELECT
    Staff_ID_Number,
    start_date,
    end_date,
    Agency_of_Employment
FROM
    aggregate_adjacent_spans
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tx",
    ingest_view_name="state_staff_location_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
