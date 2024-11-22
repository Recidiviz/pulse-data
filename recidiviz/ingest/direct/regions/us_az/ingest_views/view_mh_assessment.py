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
""" Query for Mental Health scores. We do not know what assessment is performed, if any,
to come up with this score, but the results are an eligibility consideration for 
some opportunities.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- This subquery changes all instances of the staff ID "13" to NULL, since "13" is a 
-- Service Account and not a real person. It is only used for system updates and does 
-- not add anything to our understanding of these assessments.
filtered_data AS (
    SELECT  
        MENTAL_HEALTH_ACTION_ID,
        PERSON_ID,
        UPDT_DTM,
        CREATE_DTM,
        MH_STATUS_ID,
        NULLIF(UPDT_USERID, '13') AS UPDT_USERID,
        NULLIF(CREATE_USERID, '13') AS CREATE_USERID
    FROM {AZ_INT_MENTAL_HEALTH_ACTION} mh
)
  SELECT DISTINCT
    mh.MENTAL_HEALTH_ACTION_ID,
    mh.PERSON_ID,
    COALESCE(mh.UPDT_DTM, mh.CREATE_DTM) AS RECORD_DATE,
    l.DESCRIPTION AS mh_score_description,
    l.CODE AS mh_code,
    LEFT(l.CODE, 1) AS mh_code_simplified,
    COALESCE(mh.UPDT_USERID, mh.CREATE_USERID) AS RECORD_USERID
FROM filtered_data mh
INNER JOIN {LOOKUPS} l
    ON (l.LOOKUP_CATEGORY = 'MHSTATUS'
    AND mh.MH_STATUS_ID = l.LOOKUP_ID)
WHERE l.CODE != 'U' -- Undetermined
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="mh_assessment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
