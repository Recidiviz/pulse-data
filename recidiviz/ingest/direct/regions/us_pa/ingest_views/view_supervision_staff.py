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
    -- Fill in POs and their supervisors from roster
    SELECT LastName,FirstName,EMAIL_ADDRESS,Employ_Num
    FROM {RECIDIVIZ_REFERENCE_agent_districts}
    -- The name "Vacant Position" is used for roles that aren't filled
    WHERE NOT (UPPER(FirstName) LIKE '%VACANT%' AND UPPER(LastName) LIKE '%POSITION%')
),
supervisors_in_roster AS (
-- Fill in upper management from different roster
SELECT DISTINCT 
    lastname AS LastName,
    firstname AS FirstName,
    email AS EMAIL_ADDRESS,
    ext_id AS Employ_Num
FROM {RECIDIVIZ_REFERENCE_field_supervisor_list}
WHERE ext_id NOT IN (
    SELECT DISTINCT Employ_Num FROM staff_in_roster
)
)

SELECT *
FROM staff_in_roster

UNION ALL

SELECT *
FROM supervisors_in_roster

UNION ALL

-- Fill in names / emails for agents in contacts data who are not present in either other roster
SELECT DISTINCT
    LAST_VALUE(PRL_AGNT_LAST_NAME) OVER (PARTITION BY PRL_AGNT_EMPL_NO ORDER BY CREATED_DATE) AS LastName,
    LAST_VALUE(PRL_AGNT_FIRST_NAME) OVER (PARTITION BY PRL_AGNT_EMPL_NO ORDER BY CREATED_DATE) AS FirstName,
    agent_roster.EMAIL_ADDRESS AS EMAIL_ADDRESS,
    PRL_AGNT_EMPL_NO AS Employ_Num
FROM {dbo_PRS_FACT_PAROLEE_CNTC_SUMRY} contacts
LEFT JOIN {RECIDIVIZ_REFERENCE_agent_districts} agent_roster
ON(contacts.PRL_AGNT_EMPL_NO = agent_roster.Employ_Num)
WHERE PRL_AGNT_EMPL_NO NOT IN (
    SELECT Employ_Num FROM staff_in_roster
)
AND PRL_AGNT_EMPL_NO NOT IN (
    SELECT Employ_Num FROM supervisors_in_roster
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
