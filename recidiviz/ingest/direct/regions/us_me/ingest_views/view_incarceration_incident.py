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
"""Query containing MDOC incarceration incident information."""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- We get the main necessary information from 460_incidents and join in 462 to get the client id.
incidents AS ( 
  SELECT 
    ci.Cis_100_Client_Id,
    ci.Clients_Involved_Id,
    Cis_908_Ccs_Location_id, 
    Incident_Date,
    Incident_Id, 
    Incident_Designation_Tx, 
    Location_Tx, 
    Summary_Tx,
    Year_Num
  FROM {CIS_460_INCIDENTS} i
  LEFT JOIN {CIS_462_CLIENTS_INVOLVED} ci
  ON i.Incident_Id = ci.Cis_460_Incident_Id
  AND i.Year_Num = ci.Cis_460_Year_Id
  WHERE Incident_Designation_Tx != 'FN' -- 'FN' appears to be general incidents not tied to any ClientID.
  AND i.Logical_Delete_Ind != 'Y' --
), 
-- We get the violations disposition information from 181, 1811, 1810. We join to 180 to join with incidents.
dispositions AS ( 
  SELECT 
    CIS_460_INCIDENT_ID, 
    CIS_460_YEAR_NUM, 
    CIS_462_CLIENTS_INVOLVED_ID, 
    E_Violation_Disposition_Type_Desc, 
    E_Violation_Disposition_Class_Desc, 
    HEARING_ACTUALLY_HELD_DATE
  FROM {CIS_180_DISCIPLINARY_CASE} dc
  LEFT JOIN {CIS_181_VIOLATION_DISPOSITION} vd
  ON vd.Cis_180_Disciplinary_Case_Id = dc.DISCIPLINARY_CASE_ID
  LEFT JOIN {CIS_1811_VIOLATION_DISPOSITION_TYPE} vdt
  ON vd.Cis_1811_Violation_Type_Cd = vdt.Violation_Disposition_Type_Cd
  LEFT JOIN {CIS_1810_VIOLATION_DISPOSITION_CLASS} vdc
  ON vd.Cis_1810_Violation_Class_Cd = vdc.Violation_Disposition_Class_Cd
  WHERE dc.LOGICAL_DELETE_IND != 'Y'
),
-- We get locations from 980 to join with incidents. 
locations as ( 
  SELECT
    Ccs_location_id,
    Location_Name
  FROM  {CIS_908_CCS_LOCATION}
)
SELECT 
  Cis_100_Client_Id,
  Incident_Id,
  Year_Num,
  Incident_Date,
  Location_name,
  Location_Tx,
  E_Violation_Disposition_Type_Desc,
  E_Violation_Disposition_Class_Desc, 
  Summary_Tx,
  HEARING_ACTUALLY_HELD_DATE
FROM incidents 
LEFT JOIN dispositions
ON incidents.Incident_Id = dispositions.CIS_460_INCIDENT_ID
AND incidents.Year_Num = dispositions.CIS_460_YEAR_NUM
AND incidents.Cis_100_Client_Id = dispositions.CIS_462_CLIENTS_INVOLVED_ID
LEFT JOIN locations 
ON incidents.Cis_908_Ccs_Location_id = locations.Ccs_Location_Id
WHERE Cis_100_Client_Id IS NOT NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_me",
    ingest_view_name="incarceration_incident",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
