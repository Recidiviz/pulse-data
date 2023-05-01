# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Query containing supervision staff information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH staff_in_roster AS (
    SELECT LastName,FirstName,EMAIL_ADDRESS,Employ_Num
    FROM {RECIDIVIZ_REFERENCE_agent_districts}
)

SELECT 
    LastName,
    FirstName,
    EMAIL_ADDRESS,
    Employ_Num
FROM staff_in_roster
UNION ALL
-- Fill in missing agent ids staff employee numbers for 
-- older staff not in the roster.

SELECT DISTINCT
    LAST_VALUE(PRL_AGNT_LAST_NAME) OVER (PARTITION BY PRL_AGNT_EMPL_NO ORDER BY CREATED_DATE) AS LastName,
    LAST_VALUE(PRL_AGNT_FIRST_NAME) OVER (PARTITION BY PRL_AGNT_EMPL_NO ORDER BY CREATED_DATE) AS FirstName,
    CAST(NULL AS STRING) AS EMAIL_ADDRESS,
    PRL_AGNT_EMPL_NO AS Employ_Num
FROM {dbo_PRS_FACT_PAROLEE_CNTC_SUMRY}
WHERE PRL_AGNT_EMPL_NO NOT IN (
    SELECT Employ_Num FROM staff_in_roster
)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="supervision_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="Employ_Num",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
