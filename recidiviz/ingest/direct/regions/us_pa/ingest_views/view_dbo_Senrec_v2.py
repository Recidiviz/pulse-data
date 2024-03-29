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
"""Query containing sentence information.
PADOC provides incarceration period information for two broad population subsets: those incarcerated in
State Correctional Institutions (SCIs) and those incarcerated in Community Corrections Centers (CCCs). SCIs can be
thought of as "state prisons" while CCCs can be thought of as more minimal security facilities that some incarcerated
people can be transferred into near the end of their term to focus on reentry.

The former is determined primarily via `dbo_Movrec`. Rows in `dbo_Movrec` can be used to model "edges" of continuous
periods of incarceration, and multiple such rows can be used to model a continuous period of incarceration. This ingest
view walks over rows for a single person in this table and stitches together "edges" to create continuous periods,
pulling in a small amount of additional sentencing data from `dbo_Senrec`. 

Specifically, we order rows based on the
movement date and then by sequence number when the date is the same (sorting rows with death statuses to the end no
matter what). We throw out rows that mov_rec_del_flag tells us are "bogus", and rows with person ids that are not found
elsewhere. With those ordered and filtered rows, we look at sentence status code, movement code, and movement-to
location fields to gather additional status information, look at this status information to identify if any pair of two
edges indicates a critical movement has occurred (e.g. release to parole, convicted from a parole violation, release to
liberty), and then join periods together where a critical movement has occurred.
"""

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
