# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Query containing the treatment referrals and completion date to hydrate program assignment.
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    WITH program_data as (
      SELECT * \
  FROM (
    SELECT 
      *, 
      -- If there is a row in the history table about this parole_count_id, that means this parole stint has been
      -- terminated and this is the most up to date information about this parole_count_id.
      ROW_NUMBER() OVER (PARTITION BY ParoleNumber, TreatmentID ORDER BY is_history_row DESC) AS entry_priority
    FROM (
      -- These are rows with information on active supervision stints at the time of raw data upload, collected from multiple Release* tables.
      SELECT
        t.ParoleNumber, 
        t.ParoleCountID, 
        t.TreatmentID,
        t.TrtStatusCode,
        t.TrtClassCode,
        t.TrtProgramDescription,
        t.TrtProgramCode,
        t.TrtDo,
        t.TrtCounty,
        t.TrtStartDateYear,
        t.TrtStartDateMonth,
        t.TrtStartDateDay,
        t.TrtEndDateYear,
        t.TrtEndDateMonth,
        t.TrtEndDateDay,
        0 AS is_history_row
    FROM {dbo_Treatment} t
      UNION ALL
      -- These are rows with information on historical program data. The Hist_Program table is where info associated 
      -- with the ParoleCountID goes on the completion of the program, all in one table.
      SELECT
        ht.ParoleNumber, 
        ht.ParoleCountID, 
        ht.TrtHistoryID as TreatmentID,
        ht.TrtHistOutcomeStatusCode as TrtStatusCode,
        NULLIF(SUBSTRING(ht.TrtHistDOCO, 5, 4), '') as TrtClassCode,
        ht.TrtHistTreatmentDescription as TrtProgramDescription,
        ht.TrtHistProgramCode as TrtProgramCode,
        NULLIF(SUBSTRING(ht.TrtHistDOCO, 1, 2), '') as TrtDo,
        NULLIF(SUBSTRING(ht.TrtHistDOCO, 3, 2), '') as TrtCounty,
        ht.TrtHistStartDateYear as TrtStartDateYear,
        ht.TrtHistStartDateMonth as TrtStartDateMonth,
        ht.TrtHistStartDateDay as TrtStartDateDay,
        ht.TrtHistEndDateYear as TrtEndDateYear,
        ht.TrtHistEndDateMonth as TrtEndDateMonth,
        ht.TrtHistEndDateDay as TrtEndDateDay,
        1 AS is_history_row
    FROM {dbo_Hist_Treatment} ht
    ) as programs
  ) as programs_with_priority
  WHERE entry_priority = 1
),

    TrtClassCode_trim AS (
        # The TrtClassCodes were shortened in the raw data file for simplification purposes so we trim the codes to only include codes (not subcodes)
        # in order to join the raw descriptions table. The TrtClassCode is maintained in the program_id field in order to allow for downstream work to 
        # parse based on subcodes as needed. 
    SELECT 
        TrtClassCode,
        CASE WHEN CHAR_LENGTH(TrtClassCode) = 5 THEN LEFT(TrtClassCode,3)
            WHEN CHAR_LENGTH(TrtClassCode) = 4 THEN LEFT(TrtClassCode,2) END AS classification_code
    FROM program_data
    ),
    TrtClassCode_decode AS (
        SELECT 
            DISTINCT
            TrtClassCode_trim.TrtClassCode,
            TrtClassCode_trim.classification_code,
            ref.classification_code,
            ref.classification_description
        FROM TrtClassCode_trim
        LEFT JOIN {treatment_classification_codes} ref USING (classification_code)
    )
    SELECT 
        ParoleNumber, 
        ParoleCountID, 
        TreatmentID,
        TrtStatusCode,
        TrtClassCode,
        TrtClassCode_decode.classification_description,
        TrtProgramDescription,
        TrtProgramCode,
        TrtDo,
        TrtCounty,
        CASE WHEN 
            TrtStartDateYear LIKE '%%*%%' THEN Null ELSE TrtStartDateYear
        END AS TrtStartDateYear,
        CASE WHEN
            TrtStartDateMonth LIKE '%%*%%' THEN Null ELSE TrtStartDateMonth 
        END AS TrtStartDateMonth,
        CASE WHEN 
            TrtStartDateDay LIKE '%%*%%' THEN Null ELSE TrtStartDateDay
        END AS TrtStartDateDay,
        CASE WHEN 
            TrtEndDateYear LIKE '%%*%%' THEN Null ELSE TrtEndDateYear
        END AS TrtEndDateYear,
        CASE WHEN
            TrtEndDateMonth LIKE '%%*%%' THEN Null ELSE TrtEndDateMonth
        END AS TrtEndDateMonth,
        CASE WHEN 
            TrtEndDateDay LIKE '%%*%%' THEN Null ELSE TrtEndDateDay
        END AS TrtEndDateDay
    FROM program_data
    LEFT JOIN TrtClassCode_decode USING (TrtClassCode)
    WHERE TrtProgramCode IN ('REF','REFO','SEXO')
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="program_assignment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
