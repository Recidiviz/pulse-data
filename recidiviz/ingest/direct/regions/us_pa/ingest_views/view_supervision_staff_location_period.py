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

"""Query containing supervision staff location period information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH agent_base AS (
SELECT 
    ORG_ID, 
    Employ_Num, 
    -- Including all agents who are present in the roster, irrespective of start date
    -- TODO(#21909): Update all start_date fields in this view to match start_date in the role_periods view.
    '1900-01-01' AS start_date, 
    NULL as end_date
FROM {RECIDIVIZ_REFERENCE_agent_districts}
WHERE NOT (UPPER(FirstName) LIKE '%VACANT%' AND UPPER(LastName) LIKE '%POSITION%')
),
supervisor_base AS (
SELECT 
    LPAD(district_id, 2, '0') AS district_id,
    ext_id, 
    '1900-01-01' AS start_date, 
    NULL as end_date
FROM {RECIDIVIZ_REFERENCE_field_supervisor_list} 
WHERE role IN ('District Director','Deputy District Director')
AND ext_id NOT IN (SELECT DISTINCT Employ_Num FROM agent_base)
),
cntc_base AS (
SELECT DISTINCT
    FIRST_VALUE(PRL_AGNT_ORG_NAME) OVER (PARTITION BY PRL_AGNT_EMPL_NO ORDER BY START_DATE) AS district_id,
    PRL_AGNT_EMPL_NO,
    FIRST_VALUE(START_DATE) OVER (PARTITION BY PRL_AGNT_EMPL_NO ORDER BY START_DATE) AS start_date,
    NULL AS end_date
FROM {dbo_PRS_FACT_PAROLEE_CNTC_SUMRY}
WHERE PRL_AGNT_JOB_CLASSIFCTN IN ('Prl Agt 1', 'Prl Agt 2','Prl Supv','Prl Mgr 1')
AND PRL_AGNT_EMPL_NO NOT IN (SELECT DISTINCT Employ_Num FROM agent_base)
AND PRL_AGNT_EMPL_NO NOT IN (SELECT DISTINCT ext_id FROM supervisor_base)
AND PRL_AGNT_ORG_NAME IS NOT NULL
)

SELECT * FROM agent_base 
UNION ALL
SELECT * FROM supervisor_base
UNION ALL
SELECT * 
FROM cntc_base 

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="supervision_staff_location_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="Employ_Num",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
