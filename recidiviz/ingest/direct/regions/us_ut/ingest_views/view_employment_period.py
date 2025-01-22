# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Query that generates employment periods for JII in Utah.

Employment periods can overlap."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
    DISTINCT ofndr_num,
    emplymt_id,
    /* emplyr_id is a code field. We likely want to use it for employer information, 
    but we do not currently have the reference table to decode it. 
    TODO(#36824): Find out if we can use this to find more information about employers.*/ 
    -- emplyr_id, 
    DATE(emplymt_strt_dt) AS start_date,
    DATE(end_dt) AS end_date,
    job_title AS job_title,
    rsn_left_cd AS end_reason_raw_text,
    cmt AS comment,
    supr_full_name AS employer_name,
    /* Only store dates that employment was positively verified. Our "last verified date" 
    is not designed to store failed attempts to verify employment, only successful ones. */
    CASE WHEN 
        vrfy_dt IS NOT NULL AND vrfy_rslt_flg = 'Y' THEN DATE(vrfy_dt) 
        ELSE CAST(NULL AS DATE)
    END AS verified_date,
    CAST(hrs_work_wk AS INT64) AS hours_per_week,
FROM
    {emplymt}
/* < 0.1% of rows do not have a start date. We exclude them here because they cannot 
provide an accurate or complete picture of employment periods. */
WHERE 
    emplymt_strt_dt IS NOT NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="employment_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
