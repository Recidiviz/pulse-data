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
"""Query containing sentence information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    SELECT
        sentences.curr_inmate_num,
        sentences.type_number,
        sentences.sent_status_code,
        sentences.type_of_sent,
        sentences.sent_date,
        sentences.sent_start_date,
        sentences.sent_stop_date,
        sentences.sentcing_cnty,
        sentences.offense_track_num,
        -- Historical offense codes come from offense_code, codes after 9/22/23 come
        -- are joined to dbo_Senrec from dbo_Senrec_Extended.Crime_Code
        COALESCE(sentences.offense_code, sentences.Crime_Code) AS offense_code,
        sentences.class_of_sent,         
        sentences.max_cort_sent_yrs,    
        sentences.max_cort_sent_mths,    
        sentences.max_cort_sent_days,    
        sentences.min_cort_sent_yrs,     
        sentences.min_cort_sent_mths,    
        sentences.min_cort_sent_days,  
        sentences.min_expir_date,
        CASE 
            WHEN sentences.max_expir_date = '00000000'
            THEN NULL
            ELSE sentences.max_expir_date
        END AS max_expir_date,
        CASE 
            WHEN sentences.sig_date IS NULL THEN 
                LAG(sentences.sig_date) OVER (PARTITION BY sentences.curr_inmate_num ORDER BY sentences.type_number ASC)
            WHEN sentences.sig_date = '00000000' THEN 
                NULL
            ELSE 
                sentences.sig_date
        END AS sig_date,
        COALESCE(sentences.judge, sentences.Judge_Formatted_Name) AS judge,
        offense_codes.Offense, 
        offense_codes.Category, 
        offense_codes.ASCA_Category___Ranked, 
        offense_codes.SubCategory, 
        offense_codes.Grade_Category,
        offense_codes.Grade
    FROM 
        {dbo_Senrec} sentences
    LEFT JOIN {offense_codes} offense_codes
        ON COALESCE(sentences.offense_code, sentences.Crime_Code) = offense_codes.Code
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="dbo_Senrec_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
