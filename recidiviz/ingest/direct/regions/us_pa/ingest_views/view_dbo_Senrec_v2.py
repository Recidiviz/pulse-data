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
"""Query containing incarceration sentence information from the dbo_Senrec table."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH senrec_combined AS (
    SELECT
        sentences.curr_inmate_num,
        sentences.type_number,
        sentences.sent_status_code,
        sentences.type_of_sent,
        sentences.sent_date,
        sentences.sent_start_date,
        sentences.sent_stop_date,
        sentences.sentcing_cnty,
        sentences.class_of_sent,         
        sentences.max_cort_sent_yrs,    
        sentences.max_cort_sent_mths,    
        sentences.max_cort_sent_days,    
        sentences.min_cort_sent_yrs,     
        sentences.min_cort_sent_mths,    
        sentences.min_cort_sent_days,  
        sentences.min_expir_date,
        sentences.max_expir_date,
        -- sig_date is only NULL when type_number is '02', denoting serving two concurrent sentences.
        -- In this case, we'll use the sig_date from the sentence with type_number '01'.
        -- There are a few cases where sig_date is '00000000' and is automatically cast to NULL for type_number '01'.
        -- In that case, there is no other date to use, so we'll just keep the NULL value.
        CASE 
            WHEN sentences.sig_date IS NULL THEN 
                LAG(sentences.sig_date) OVER (PARTITION BY sentences.curr_inmate_num ORDER BY sentences.type_number ASC)
            ELSE 
                sentences.sig_date
        END AS sig_date,
        COALESCE(sentences.judge, extended.Judge_Formatted_Name) as judge,
        COALESCE(sentences.offense_code, extended.Crime_Code) as offense_code,       
        COALESCE(offense_codes.Offense, extended.Crime_Code_Description) as Offense,
        offense_codes.Category, 
        offense_codes.ASCA_Category___Ranked, 
        offense_codes.SubCategory, 
        offense_codes.Grade_Category,
        offense_codes.Grade
    FROM {dbo_Senrec} sentences
    LEFT JOIN {dbo_Senrec_extended} extended
        ON sentences.curr_inmate_num = extended.SEN_CURRENT_INMATE_NUM
        and sentences.type_number = extended.SEN_TYPE_NUMBER
    LEFT JOIN {offense_codes} offense_codes
        ON COALESCE(sentences.offense_code, extended.Crime_Code) = offense_codes.Code
),
all_inc_charges AS (
    SELECT 
        curr_inmate_num, 
        type_number,
        CAST(NULL AS STRING) AS Indictment_Sequence_No,
        offense_code,         
        Offense, 
        Category, 
        ASCA_Category___Ranked, 
        SubCategory, 
        Grade_Category,
        Grade
    FROM senrec_combined

    UNION DISTINCT

    SELECT 
        Lifecycle_Number,
        -- Set type_number as "01" for all charges from this table since we don't have type_number
        -- available in this table.  This means that we'll just attach every charge in this table to
        -- the the sentence in dbo_Senrec with the first type_number for each curr_inmate_num.
        "01" AS type_number,
        Indictment_Sequence_No,
        Code AS offense_code,
        Short_Description AS Offense,
        CAST(NULL AS STRING) AS Category,
        Ranking_Description AS ASCA_Category___Ranked,
        SubCategory,
        CAST(NULL AS STRING) AS Grade_Category,
        NULLIF(Grade, "NULL") AS Grade
    FROM {IncarcerationSentence}
)


SELECT 
    curr_inmate_num,
    type_number,
    sent_status_code,
    type_of_sent,
    sent_date,
    sent_start_date,
    sent_stop_date,
    sentcing_cnty,
    class_of_sent,         
    max_cort_sent_yrs,    
    max_cort_sent_mths,    
    max_cort_sent_days,    
    min_cort_sent_yrs,     
    min_cort_sent_mths,    
    min_cort_sent_days,  
    min_expir_date,
    max_expir_date,
    sig_date,
    judge,
    TO_JSON_STRING(ARRAY_AGG(STRUCT<charge_sequence_number string,
                                    Offense string,
                                    offense_code string,
                                    category string,
                                    asca_category string,
                                    subcategory string,
                                    grade_category string,
                                    grade string>
                                (CAST(charge_sequence_number AS STRING),
                                 Offense,
                                 offense_code,
                                 Category,
                                 ASCA_Category___Ranked,
                                 SubCategory,
                                 Grade_Category,
                                 Grade ) ORDER BY charge_sequence_number)) AS list_of_charges
FROM (
    SELECT *,
        -- create a charge sequence number for charge external id purposes
        ROW_NUMBER() OVER(PARTITION BY curr_inmate_num, type_number ORDER BY Indictment_Sequence_No, all_inc_charges.offense_code, all_inc_charges.Offense, Category, ASCA_Category___Ranked, SubCategory, Grade_Category, Grade) AS charge_sequence_number
    FROM (
        SELECT * EXCEPT(offense_code, Offense, Category, ASCA_Category___Ranked, SubCategory, Grade_Category, Grade)
        FROM senrec_combined
    )
    LEFT JOIN all_inc_charges 
    USING(curr_inmate_num, type_number)
)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="dbo_Senrec_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
