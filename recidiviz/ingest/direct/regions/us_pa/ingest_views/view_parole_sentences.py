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
"""
Query containing parole term information from the dbo_ReleaseInfo and dbo_Hist_Release tables.
The data in these tables represent parole term information, which we will use as a proxy for
parole sentences.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- This CTE pivots the dbo_ReleaseInfo table so that the three different OffenseCode fields all appear in the same column
--   For context:
--     - RelPBPPOffenseCode = offense code of longest sentence for this parole term
--     - RelPBPPOffenseCode2 = offense code of second longest sentence for this parole term
--     - RelPBPPOffenseCode3 = offense code of third longest sentence for this parole term
dbo_ReleaseInfo_pivoted AS (
  SELECT 
    * EXCEPT(offense),
    -- Convert offense column to just be a number id that we can use in the charges external id
    CASE WHEN offense = 'RelPBPPOffenseCode' THEN '1'
    ELSE REPLACE(offense, 'RelPBPPOffenseCode', '')
    END AS offense_code_num
  FROM {dbo_ReleaseInfo} rel
  UNPIVOT(offense_code FOR offense IN (RelPBPPOffenseCode, RelPBPPOffenseCode2, RelPBPPOffenseCode3))
),
-- Take the previous CTE and then joins on dbo_Release, dbo_Hist_Release, and the offense codes mapping to get info for completed parole sentences and 
-- to convert the offense codes to descriptions.  
-- This CTE also identifies the max_HReleaseID for each set of ParoleNumber-ParoleCountID for deduping later 
-- (because in a few cases, there are multiple rows for a single ParoleNumber-ParoleCountID that appear in the history table)
current_and_historical_info_parsed AS (
    SELECT
        ParoleNumber,
        ParoleCountID,
        
        -- there are only 12 non-parseable HReReldates as of 5/1/2024, so just going to use safe parse here
        SAFE.PARSE_DATE('%Y%m%d', hist.HReReldate) AS HReReldate,
        DATE(CAST(r.RelReleaseDateYear as INT64), CAST(r.RelReleaseDateMonth as INT64), CAST(r.RelReleaseDateDay as INT64)) AS RelReleaseDate,
        
        -- there are many rows set to LIFE, 144 rows set to some other non-parseable string as of 5/1/2024
        -- set LIFE/INDE to the magic end date RelMaxDate uses, otherwise keep RelMaxDate as is
        CASE 
            WHEN HReMaxDate = 'LIFE' OR HReMaxDate LIKE '%INDE%'
                THEN DATE(2999, 12, 31)
            ELSE SAFE.PARSE_DATE('%Y%m%d', HReMaxDate)
            END AS HReMaxDate,
        SAFE.PARSE_DATE('%Y%m%d', RelMaxDate) AS RelMaxDate,
        
        -- there are only 5 non-parseable HReDelDates as of 5/1/2024, so just going to use safe parse here
        SAFE.PARSE_DATE('%Y%m%d', HReDelDate) as HReDelDate,
        
        HReDelCode,
        off.PBPPOffenseDescription as curr_PBPPOffenseDescription,
        off_hist.PBPPOffenseDescription as hist_PBPPOffenseDescription,
        CAST(HReleaseID AS INT64) AS HReleaseID,
        
        -- sometimes there are duplicate dbo_Hist_Release records per ParoleNumber-ParoleCountId and it's presumed to be a data entry error
        -- let's use the most recently entered dbo_Hist_Release record for our purposes
        -- (though largely irrelevant because nearly all the duplicates are way in the past and happen around the year 2000)
        MAX(CAST(HReleaseID AS INT64)) OVER(PARTITION By ParoleNumber, ParoleCountID) as max_HReleaseID,
        
        -- If offense_code_num is null and the offense description from dbo_Hist_Release is not null, that means that we only have information from dbo_Hist_Release
        -- In those cases, let's set offense_code_num to '0'
        -- Otherwise, if offense_code_num is not null or the offense description from dbo_Hist_Release is null, we can use the offense_code_num created from that first CTE if it exists
        CASE 
            WHEN offense_code_num IS NULL and off_hist.PBPPOffenseDescription IS NOT NULL
                THEN '0'
            ELSE offense_code_num
            END AS offense_code_num
    FROM dbo_ReleaseInfo_pivoted rinfo
        LEFT JOIN {dbo_Release} r USING(ParoleNumber, ParoleCountId)
        -- full outer join on the history table because there may be rows in the history table that don't appear in ReleaseInfo and Release
        FULL OUTER JOIN {dbo_Hist_Release} hist USING(ParoleNumber, ParoleCountID)
        LEFT JOIN {RECIDIVIZ_REFERENCE_pbpp_offense_code_descriptions} off
            ON off.PBPPOffenseCode = offense_code
        LEFT JOIN {RECIDIVIZ_REFERENCE_pbpp_offense_code_descriptions} off_hist
            ON off_hist.PBPPOffenseCode = HReOffense
),
-- Dedups based on max_HReleaseID and for dates where there are two different sources, systematically prioritize a single source
--   NOTE: Once a parole stint is completed, it will no longer show up in dbo_Release  and dbo_ReleaseInfo, and the row will appear in dbo_Hist_Release.
--         Since we're using the _latest view, we'll still see any rows we've received before in dbo_Release and dbo_ReleaseInfo even if they've now been deleted from the table.
--         In that case, we  prioritize the dates that currently appear in the history table over the dates that appeared once in the dbo_Release and dbo_ReleaseInfo tables
--         EXCEPT in the case of offense code, because the dbo_Release and dbo_ReleaseInfo tables have more detailed info for that field
current_and_historical_info_deduped AS (
SELECT 
    ParoleNumber,
    ParoleCountID,
    COALESCE(HReReldate, RelReleaseDate) AS RelReleaseDate,
    COALESCE(HReMaxDate, RelMaxDate) AS RelMaxDate,
    HReDelDate,
    HReDelCode,
    COALESCE(curr_PBPPOffenseDescription, hist_PBPPOffenseDescription) AS PBPPOffenseDescription,
    offense_code_num
FROM current_and_historical_info_parsed
WHERE (HReleaseID = max_HReleaseID OR max_HReleaseID IS NULL)
    -- excluding ParoleCountID = -1 because -1 is used for parole numbers that are invalid (there is no information, errors in general with generation, parole numbers that should be voided because its duplicated)
    AND ParoleCountID <> '-1'
)

-- Finally, collapse it so that it is one row per parole sentence instead of one row per parole sentence - offense code
SELECT 
    ParoleNumber,
    ParoleCountID,
    RelReleaseDate,
    RelMaxDate,
    HReDelDate,
    HReDelCode,
    TO_JSON_STRING(ARRAY_AGG(STRUCT<offense_code_num string,PBPPOffenseDescription string>(offense_code_num,PBPPOffenseDescription) ORDER BY offense_code_num)) AS list_of_offense_descriptions
FROM current_and_historical_info_deduped
GROUP BY 1,2,3,4,5,6
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="parole_sentences",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
