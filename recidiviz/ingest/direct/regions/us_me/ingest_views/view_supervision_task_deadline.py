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
"""Query containing information about discharge from supervision State Task Deadlines """

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- `court_orders` adds the previous Comm_Rel_Date as an output column, so we can keep
-- only record which indicate a change in the final query.
court_orders as (
    SELECT
        Cis_400_Cis_100_Client_Id as Client_Id,
        Cis_319_Term_Id as Term_Id,
        Court_Order_Id,
        Comm_Rel_Date,
        LAG(Comm_Rel_Date) OVER (PARTITION BY Cis_400_Cis_100_Client_Id, Cis_319_Term_Id, Court_Order_Id ORDER BY update_datetime) AS PREV_Comm_Rel_Date,
        update_datetime
    FROM {CIS_401_CRT_ORDER_HDR@ALL} co
    -- This is how we identify supervision sentences. All rows with this indicate a
    -- sentence to supervision.
    WHERE Comm_Rel_Date is not null
)
select *
from court_orders
where Comm_Rel_Date != PREV_Comm_Rel_Date 
or PREV_Comm_Rel_Date is NULL;
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_me",
    ingest_view_name="supervision_task_deadline",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
