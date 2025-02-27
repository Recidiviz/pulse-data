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
"""Ingest view for supervision staff caseload type information from the monthly P&P directory."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH roster_data AS (
SELECT 
    OFFICER,
    UPPER(JobTitle) AS JobTitle,
    CAST(update_datetime AS DATETIME) AS edge_date, 
    MAX(CAST(update_datetime AS DATETIME)) OVER (PARTITION BY TRUE) AS last_file_update_datetime,
    MAX(CAST(update_datetime AS DATETIME)) OVER (PARTITION BY OFFICER) AS last_appearance_datetime, 
FROM {RECIDIVIZ_REFERENCE_PP_directory@ALL}
),
critical_dates AS (
    SELECT * FROM (
        SELECT
        OFFICER,
        JobTitle,
        edge_date,
        LAG(JobTitle) OVER person_title_window AS last_job_title,
        last_file_update_datetime,
        last_appearance_datetime
    FROM roster_data
    WINDOW person_title_window AS (PARTITION BY OFFICER ORDER BY edge_date)
    ) sub
    -- title changed
    WHERE last_job_title != JobTitle
    -- first appearance
    OR last_job_title IS NULL and JobTitle IS NOT NULL
    OR edge_date = last_file_update_datetime
)
SELECT
    OFFICER,
    JobTitle,
    edge_date AS start_date,
    CASE
        -- All currently-employed staff will appear in the latest roster
        WHEN edge_date = last_file_update_datetime THEN CAST(NULL AS DATETIME)     
    -- If a staff member stops appearing in the roster, close their employment period
        -- on the first date we receive a roster that does not include them
        WHEN lead(edge_date) OVER person_window IS NULL 
        AND edge_date < last_file_update_datetime 
        THEN last_appearance_datetime   
        -- Else there is a more recent entry for a staff member
        ELSE LEAD(edge_date) OVER person_window
    END AS end_date,
    ROW_NUMBER() OVER person_window AS period_seq_num
FROM critical_dates
WINDOW person_window AS (PARTITION BY OFFICER ORDER BY edge_date, JobTitle)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="docstars_staff_caseload_type_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OFFICER,period_seq_num",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
