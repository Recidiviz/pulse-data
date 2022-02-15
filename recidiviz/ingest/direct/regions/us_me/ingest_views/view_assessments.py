# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query containing MEDOC assessment information.

This includes both LSI assessments from a dedicated LSI_HISTORY table, and a medley of other
non-LSI assessments from a different table which is named as though it contains only sex
offense assessments, though this is not true. This other table includes information about the
outcomes of the assessment in columns that are named for LSI ratings, as a result of the fact
that their internal logic relies on the results of assessments from both tables.
"""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH non_lsi_assessments AS (
    SELECT
        soa.Sex_Offender_Assess_Id as Assessment_Id,
        soa.Cis_100_Client_Id as Client_Id,
        soa.Cis_900_Employee_Id as Conductor_Id,
        soa.Cis_2150_Assess_Type_Cd as Assessment_Type_Cd,
        type.Sex_Off_Assess_Type_Desc as Assessment_Type,
        soa.Assessment_Date,
        soa.Assessment_Score_Num,
        CAST(null as STRING) as Lsi_Rating_Approved_Cd,
        soa.Cis_1161_Lsi_Rating_Over_Cd as Lsi_Rating_Override_Cd,
        soa.Cis_1161_Lsi_Rating_Cd as Lsi_Rating_Cd
    FROM {CIS_215_SEX_OFFENDER_ASSESS} soa
    JOIN {CIS_2150_SEX_OFF_ASSESS_TYPE} type ON soa.Cis_2150_Assess_Type_Cd = type.Sex_Off_Assess_Type_Cd
),
lsi_assessments AS (
    SELECT
        lsi.Lsi_History_Id as Assessment_Id,
        lsi.Cis_100_Client_Id as Client_Id,
        lsi.Cis_900_Employee_Id as Conductor_Id,
        lsi.Cis_1009_Lsi_Type_Cd as Assessment_Type_Cd,
        type.E_Lsi_Type_Desc as Assessment_Type,
        lsi.Lsi_Date as Assessment_Date,
        lsi.Lsi_Score_Num as Assessment_Score_Num,
        lsi.Cis_1161_Comm_Lsi_Apprv_Rating_Cd as Lsi_Rating_Approved_Cd,
        lsi.Cis_1161_Comm_Lsi_Or_Rating_Cd as Lsi_Rating_Override_Cd,
        lsi.Cis_1161_Comm_Lsi_Rating_Cd as Lsi_Rating_Cd,
    FROM {CIS_116_LSI_HISTORY} lsi
    JOIN {CIS_1009_LSI_TYPE} type ON lsi.Cis_1009_Lsi_Type_Cd = type.Lsi_Type_Cd
),
lsi_ratings AS (
    SELECT
        E_Lsi_Rating_Desc,
        Lsi_Rating_Cd,
    FROM {CIS_1161_LSI_RATING}
),
all_assessments AS (
    SELECT * FROM non_lsi_assessments UNION ALL SELECT * FROM lsi_assessments
)
select
    aa.Assessment_Id,
    aa.Client_Id,
    aa.Conductor_Id,
    aa.Assessment_Type,
    aa.Assessment_Date,
    aa.Assessment_Score_Num,
    lra.E_Lsi_Rating_Desc as Lsi_Rating_Approved,
    lro.E_Lsi_Rating_Desc as Lsi_Rating_Override,
    lr.E_Lsi_Rating_Desc as Lsi_Rating,
    emp.First_Name as Conductor_First_Name,
    emp.Last_Name as Conductor_Last_Name,
    emp.Middle_Name as Conductor_Middle_Name,
FROM all_assessments aa
LEFT JOIN lsi_ratings lra on aa.Lsi_Rating_Approved_Cd = lra.Lsi_Rating_Cd
LEFT JOIN lsi_ratings lro on aa.Lsi_Rating_Override_Cd = lro.Lsi_Rating_Cd
LEFT JOIN lsi_ratings lr on aa.Lsi_Rating_Cd = lr.Lsi_Rating_Cd
LEFT JOIN {CIS_900_EMPLOYEE} emp on aa.Conductor_Id = emp.Employee_Id
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_me",
    ingest_view_name="assessments",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="Client_Id, Assessment_Id",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
