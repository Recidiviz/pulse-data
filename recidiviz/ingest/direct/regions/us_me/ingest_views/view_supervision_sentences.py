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
"""Query containing information about MDOC sentences to supervision.

This includes only sentences that contain information indicating a sentence to a period of supervision. Some MDOC
sentence rows contain data indicating a sentence to both incarceration and supervision as a result of the same
underlying charge. We split that into separate incarceration and supervision sentences, with the latter being captured
in this view.
"""
from recidiviz.ingest.direct.regions.us_me.ingest_views.us_me_view_query_fragments import (
    VIEW_SENTENCE_ADDITIONAL_TABLES,
    VIEW_SENTENCE_COLUMN_SELECTIONS,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    UPDATE_DATETIME_PARAM_NAME,
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
{VIEW_SENTENCE_ADDITIONAL_TABLES}
sentences as (
    SELECT
        Cis_400_Cis_100_Client_Id as Client_Id,
        Cis_319_Term_Id as Term_Id,
        Court_Order_Id,
        Cis_400_Charge_Id as Charge_Id,
        Court_Finding_Date,
        Court_Order_Sent_Date,
        COALESCE(Est_Sent_Start_Date, Prob_Start_Date) AS Sentence_Start_Date,
        Comm_Rel_Date,
        IF(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', Comm_Rel_Date) < @{UPDATE_DATETIME_PARAM_NAME}, Comm_Rel_Date, NULL) as completion_date,
        Prob_Yrs_Num,
        Prob_Mths_Num,
        Prob_Days_Num,
        cos.E_Crt_Order_Status_Desc,
        Cis_4009_Sex_Offense_Cd as Sex_Offense_Cd,
        scs.Sent_Calc_Sys_Desc as Comm_Override_Reason,
        Cis_401_Court_Order_Id as Consecutive_Court_Order_Id,
        Cis_9904_Judge_Prof_Id as Judge_Professional_Id,
    FROM {{CIS_401_CRT_ORDER_HDR}} co
    LEFT JOIN {{CIS_4010_CRT_ORDER_STATUS}} cos ON co.Cis_4010_Crt_Order_Status_Cd = cos.Crt_Order_Status_Cd
    LEFT JOIN {{CIS_4009_SENT_CALC_SYS}} scs on co.Cis_4009_Comm_Override_Rsn_Cd = scs.Sent_Calc_Sys_Cd
    -- This is how we identify supervision sentences. All rows with this indicate a sentence to supervision.
    WHERE Comm_Rel_Date IS NOT NULL
)
{VIEW_SENTENCE_COLUMN_SELECTIONS}
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_me",
    ingest_view_name="supervision_sentences",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="sentence.Client_Id, sentence.Term_Id, sentence.Court_Order_Id",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
