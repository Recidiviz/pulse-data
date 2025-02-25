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
"""Query containing information about discharge from incarceration State Task Deadlines """
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH terms as (
    SELECT
        Term_Id,
        Cis_100_Client_Id as Client_Id,
        Curr_Cust_Rel_Date,
        update_datetime
    FROM {CIS_319_TERM@ALL} term
),
sentences as (
    SELECT
        Cis_400_Cis_100_Client_Id as Client_Id,
        Cis_319_Term_Id as Term_Id,
        Court_Order_Id,
    FROM {CIS_401_CRT_ORDER_HDR} co
    -- This is how we identify incarceration sentences. All rows either have all 3 of these values or none.
    WHERE Early_Cust_Rel_Date IS NOT NULL and Curr_Cust_Rel_Date IS NOT NULL and Max_Cust_Rel_Date IS NOT NULL
),

sorted as (SELECT DISTINCT
    sentence.Client_Id as Client_Id,
    sentence.Term_Id as Term_Id,
    sentence.Court_Order_Id as Court_Order_Id,
    term.Curr_Cust_Rel_Date AS Term_Curr_Cust_Rel_Date,
    term.update_datetime as update_datetime,
FROM sentences sentence
LEFT JOIN terms term on sentence.Term_Id = term.Term_Id)

SELECT 
    Client_Id,
    Term_Id,
    Court_Order_Id,
    Term_Curr_Cust_Rel_Date,
    update_datetime
FROM (SELECT 
        Client_Id,
        Term_Id,
        Court_Order_Id,
        Term_Curr_Cust_Rel_Date,
        update_datetime,
        LAG(Term_Curr_Cust_Rel_Date) OVER (PARTITION BY Client_Id, Term_Id, Court_Order_Id ORDER BY update_datetime) AS PREV_Term_Curr_Cust_Rel_Date
from sorted) 
where Term_Curr_Cust_Rel_Date != PREV_Term_Curr_Cust_Rel_Date 
or PREV_Term_Curr_Cust_Rel_Date is NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_me",
    ingest_view_name="incarceration_task_deadline",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
