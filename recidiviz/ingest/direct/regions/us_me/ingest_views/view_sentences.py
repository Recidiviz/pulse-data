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
"""Query containing information about MDOC sentences and charges.

Sentences in ME are issued by Court Orders. This view only ingests sentences which:
1. Are associated with charges on adults (IE the sentence is an adult sentence)
2. Have reasonable dates (see bottom of the final query)

If you are interested in this ingest view, you are probably also (and perhaps will be
more) interested in our ingest for StateSentenceGroupLength and StateSentenceSnapshot.

If someone is revoked, they receive a new sentence with the all or some of the remaining
time applied on that new sentence, depending on if it was a full or partial revocation
respectively.

Sentences do not always have time associated with them -- for example, you could be
sentenced to simply pay fines and fees.
"""
from recidiviz.ingest.direct.regions.us_me.ingest_views.us_me_view_query_fragments import (
    is_null_or_reasonable,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCES_VIEW_NAME = "sentences"

VIEW_QUERY_TEMPLATE = f"""
WITH 
-- Gets basic charge information
charges AS (
    SELECT
        charge.Charge_Id,
        charge.Cis_100_Client_Id as Client_Id,
        charge.Offense_Date,
        charge.Referral_Date,
        charge.Cis_4020_Offense_Class_Cd as Offense_Class_Cd,
        charge.Cis_9003_Jurisd_County_Cd as Jurisdiction_County_Cd,
        charge.Comments_Tx,
        charge.Juvenile_Ind,
        charge_status.E_Charge_Outcome_Desc,
        offense_type.E_Offence_Type_Desc,
        offense_type.Mejis_Offns_Class_Tx,
        offense_type.Mejis_Offns_Title_Tx,
        offense_type.Mejis_Offns_Section_Tx,
    FROM {{CIS_400_CHARGE}} charge
    LEFT JOIN {{CIS_4000_CHARGE_STATUS}} charge_status on charge.CIS_4000_Charge_Outcome_Cd = charge_status.Charge_Outcome_Cd
    LEFT JOIN {{CIS_4003_OFFENCE_TYPE}} offense_type on charge.Cis_4003_Offence_Type_Cd = offense_type.Offence_Type_Cd

    -- We can ignore any sentences associated with charges which are associated with a
    -- juvenile, or with unreasonable dates.
    WHERE charge.Juvenile_Ind != 'Y'
        AND {is_null_or_reasonable('Offense_Date')}
        AND {is_null_or_reasonable('Referral_Date')}

        -- Also, only retain charges with an outcome of convicted or null (`null`
        -- because the data is often incomplete -- see TODO(#41300))
        AND COALESCE(UPPER(E_Charge_Outcome_Desc), 'CONVICTED') = 'CONVICTED'
),
-- Gets basic judge information
judges as (
    SELECT
        professional.Professional_Id,
        professional.First_Name AS judge_First_Name,
        professional.Last_Name AS judge_Last_name,
    FROM {{CIS_9904_PROFESSIONAL}} professional
),
-- Get's basic conditions information
conditions as (
    SELECT
        condition_court_order.Cis_401_Court_Order_Id as Court_Order_Id,
        STRING_AGG(condition_type.E_Condition_Type_Desc, '\\n' ORDER BY E_Condition_Type_Desc asc) as conditions,
    FROM {{CIS_403_CONDITION}} condition
    LEFT JOIN {{CIS_409_CRT_ORDER_CNDTION_HDR}} condition_court_order on condition.Cis_408_Condition_Hdr_Id = condition_court_order.Cis_408_Condition_Hdr_Id
    LEFT JOIN {{CIS_4030_CONDITION_TYPE}} condition_type on condition.Cis_4030_Condition_Type_Cd = condition_type.Condition_Type_Cd
    WHERE condition_court_order.Cis_401_Court_Order_Id is not null
    GROUP BY Court_Order_Id
),
-- Gets basic sentence information
sentences as (
    WITH 
    -- Converts sentencing time to an int and replaces nulls with 0
    court_orders_cleaned as (
        SELECT
            *,
            COALESCE(CAST(Stc_Yrs_Num AS INT64), 0) as stc_yrs_num_cleaned,
            COALESCE(CAST(Stc_Mths_Num AS INT64), 0) as stc_mths_num_cleaned,
            COALESCE(CAST(Stc_Days_Num AS INT64), 0) as stc_days_num_cleaned,

            COALESCE(CAST(prob_Yrs_Num AS INT64), 0) as prob_yrs_num_cleaned,
            COALESCE(CAST(prob_Mths_Num AS INT64), 0) as prob_mths_num_cleaned,
            COALESCE(CAST(prob_Days_Num AS INT64), 0) as prob_days_num_cleaned,

            COALESCE(CAST(serve_Yrs_Num AS INT64), 0) as serve_yrs_num_cleaned,
            COALESCE(CAST(serve_Mths_Num AS INT64), 0) as serve_mths_num_cleaned,
            COALESCE(CAST(serve_Days_Num AS INT64), 0) as serve_days_num_cleaned,

            COALESCE(CAST(revocation_Yrs_Num AS INT64), 0) as revocation_yrs_num_cleaned,
            COALESCE(CAST(revocation_Mths_Num AS INT64), 0) as revocation_mths_num_cleaned,
            COALESCE(CAST(revocation_Days_Num AS INT64), 0) as revocation_days_num_cleaned,
        FROM {{CIS_401_CRT_ORDER_HDR}}
        WHERE {is_null_or_reasonable('Court_Order_Sent_Date')}

        -- There are 98 records, all before 2012, which share a Term_Id and are filtered out by
        -- this statement -- this should never happen. 2 people are listed as still being on
        -- probation -- I am trying to ask about this here:
        -- https://colab.research.google.com/drive/13Z8OKa3PqPxHFY_W9uCO_itno-_8DN07#scrollTo=tVXlnG70Sv6J&line=1&uniqifier=1
        QUALIFY COUNT(DISTINCT Cis_400_Cis_100_Client_Id) OVER (PARTITION BY Cis_319_Term_Id) = 1
    )
    SELECT
        Cis_400_Cis_100_Client_Id,
        Cis_319_Term_Id,
        Court_Order_Id,
        Cis_400_Charge_Id,
        Court_Order_Sent_Date,

        Stc_Yrs_num_cleaned || ' yrs ' || Stc_Mths_num_cleaned || ' mths ' || Stc_Days_num_cleaned || ' days' AS stc_time,
        Serve_Yrs_num_cleaned || ' yrs ' || Serve_Mths_num_cleaned || ' mths ' || Serve_Days_num_cleaned || ' days' AS serve_time,
        Prob_Yrs_num_cleaned || ' yrs ' || Prob_Mths_num_cleaned || ' mths ' || Prob_Days_num_cleaned || ' days' AS probation_time,
        Revocation_Yrs_num_cleaned || ' yrs ' || Revocation_Mths_num_cleaned || ' mths ' || Revocation_Days_num_cleaned || ' days' AS revocation_time,

        Cis_4009_Sex_Offense_Cd AS Sex_Offense_Cd,
        Cis_9904_Judge_Prof_Id AS Judge_Professional_Id,
        IF(Apply_Det_Days_Ind = 'Y', CAST(Detention_Days_Num AS INT64), 0) AS Detention_Days_Num_cleaned,

        Cis_401_Court_Order_Id as parent_Court_Order_Id,
    FROM court_orders_cleaned co
    LEFT JOIN {{CIS_4010_CRT_ORDER_STATUS}} cos ON co.Cis_4010_Crt_Order_Status_Cd = cos.Crt_Order_Status_Cd
    LEFT JOIN {{CIS_4009_SENT_CALC_SYS}} scs ON co.Cis_4009_Comm_Override_Rsn_Cd = scs.Sent_Calc_Sys_Cd
),
-- A sentence must have a convicted charge to be valid, and this accounts for charges
-- against juveniles (which we don't want to ingest)
sentences_with_valid_charges as (
    SELECT 
        sentences.*,
        charges.Offense_Date,
        charges.Referral_Date,
        charges.Offense_Class_Cd,
        charges.Jurisdiction_County_Cd,
        charges.Comments_Tx,
        charges.E_Charge_Outcome_Desc,
        charges.E_Offence_Type_Desc,
        charges.Mejis_Offns_Class_Tx,
        charges.Mejis_Offns_Title_Tx,
        charges.Mejis_Offns_Section_Tx, 
    FROM sentences
    INNER JOIN charges ON sentences.Cis_400_Charge_Id = charges.Charge_Id
),
-- If sentence says that it's consecutive with another sentence that we don't see in our
-- list of sentences, we remove that information since ingest complains if a sentence is
-- said to be consecutive with another sentence that doesn't exist.
clean_consecutive_sentences as (
    SELECT 
        valid.* EXCEPT (parent_Court_Order_Id),
        valid_parent.Court_Order_Id as parent_Cis_401_Court_Order_Id_cleaned
    FROM sentences_with_valid_charges valid
    LEFT JOIN sentences_with_valid_charges valid_parent on valid.parent_Court_Order_Id = valid_parent.Court_Order_Id
)
SELECT
    sentences.*,
    condition.conditions,
    judge.judge_First_Name,
    judge.judge_Last_Name
FROM clean_consecutive_sentences sentences
LEFT JOIN conditions condition ON sentences.Court_Order_Id = condition.Court_Order_Id
LEFT JOIN judges judge ON sentences.Judge_Professional_Id = judge.Professional_Id
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_me",
    ingest_view_name=SENTENCES_VIEW_NAME,
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
