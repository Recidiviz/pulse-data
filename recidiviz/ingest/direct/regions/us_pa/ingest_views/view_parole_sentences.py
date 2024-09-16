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
Query containing parole sentence and charge information from the dbo_Release, dbo_ReleaseInfo, dbo_Hist_Release, 
and dbo_Sentence table.  There may be overlapping offense information across these source tables,
but we are ok ingesting duplicate charges in order to ingest all available offense information.  In 
addition, there may be overlapping sentence information between the dbo_ReleaseInfo table and the
dbo_Hist_Release table, and in those cases, we'll prioritize the information from the history table.

We'll be getting each of the following information from each source table:
- dbo_Release (on the sentence group level)
  - sentence effective date
- dbo_ReleaseInfo (on the sentence group level)
  - offense information
  - sentence max date
- dbo_Hist_Release (on the sentence group level)
  - offense information
  - sentence status
  - sentence effective date
  - sentence max date
  - setence completion date
- dbo_Sentence (on the sentence level)
  - sentence imposed date
  - sentence county
  - offense information

Note: 
The dbo_Sentence table is on the sentence level (identified by ParoleNumber, ParoleCountId, 
Sent16DGroupNumber, and SentenceId) while the other three tables are on the sentence group level 
(identified by ParoleNumber and ParoleCountId). Therefore, we identify each sentence using ParoleNumber, 
ParoleCountId, Sent16DGroupNumber, and SentenceId and then associate every charge we see in the dbo_Sentence
table 1-1 with each sentence. For every charge we see in the dbo_ReleaseInfo table, we associate it with 
every sentence in the same sentence group (identified by ParoleNumber and ParoleCountId).

In addition, we use the sentence date from dbo_Sentence as the date_imposed for each sentence, 
though technically sentence date corresponds with the date the original incarceration sentence 
was imposed, not the specific parole sentence. It's still a todo to figure out the right date imposed to use TODO(#30223).


In the future, we'll want to revise this to ingest sentence and charge information from Captor directly.  Currently blocked on data request TODO(#33154)
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
  WITH 

    -- This CTE parses the relevant sentence dates from the dbo_ReleaseInfo table and then 
    -- pivots the table so that the three different OffenseCode fields all appear in the same column.
    -- Since there could be up to 3 offenses for each record in dbo_ReleaseInfo, for each offense code, 
    -- we assign an offense_id_num or 1, 2, or 3 to use in the external_id for the charge we'll ingest.
    -- In dbo_ReleaseInfo, the three offense columns are as follows:
    --     - RelPBPPOffenseCode = offense code of longest sentence for this parole term
    --     - RelPBPPOffenseCode2 = offense code of second longest sentence for this parole term
    --     - RelPBPPOffenseCode3 = offense code of third longest sentence for this parole term
    dbo_Release_Info_cleaned AS (
        SELECT 
            ParoleNumber,
            ParoleCountId,
            -- Since there are four different RelMaxDates for each record, we'll take the greatest date
            -- Note: sometimes RelMaxDateN is null, which is why we always coalesce with RelMaxDate
            NULLIF(
                GREATEST(
                    COALESCE(SAFE.PARSE_DATE('%Y%m%d', RelMaxDate), DATE(1111,1,1)),
                    COALESCE(SAFE.PARSE_DATE('%Y%m%d', RelMaxDate1), DATE(1111,1,1)),
                    COALESCE(SAFE.PARSE_DATE('%Y%m%d', RelMaxDate2), DATE(1111,1,1)),
                    COALESCE(SAFE.PARSE_DATE('%Y%m%d', RelMaxDate3), DATE(1111,1,1))
                ),
                DATE(1111,1,1)
            ) AS RelMaxDate,
            -- Convert offense column to just be a number id that we can use in the charges external id
            CASE WHEN offense = 'RelPBPPOffenseCode' THEN '1'
                ELSE REPLACE(offense, 'RelPBPPOffenseCode', '')
                END AS offense_id_num,
            offense_code
        FROM {dbo_ReleaseInfo} rel
        UNPIVOT(offense_code FOR offense IN (RelPBPPOffenseCode, RelPBPPOffenseCode2, RelPBPPOffenseCode3))
    ),

    -- This CTE takes the dbo_Hist_Release table and parses all relevant sentence date values.
    -- Then, it dedupes it so that for each ParoleNumber - ParoleCountId, we keep only the 
    -- most recent archived release record (based on HReleaseId, which is a PK for the dbo_Hist_Release table).
    -- Again, we assign each offense found in the dbo_Hist_Release an offense_id_num to use in the external_id
    -- for the charge we'll ingest.
    -- For each offense we see in the dbo_Hist_Release table, we'll assign an offense_id_num or 0.  Unlike,
    -- dbo_ReleaseInfo, dbo_Hist_Release only has one offense associated with each record.
    -- Exclude history records with delete code 51 since those are records that were closed in error.
    dbo_Hist_Release_deduped AS (
        SELECT 
            ParoleNumber,
            ParoleCountId,
            SAFE.PARSE_DATE('%Y%m%d', HReReldate) AS HReReldate,
            CASE 
                WHEN HReMaxDate = 'LIFE' OR HReMaxDate LIKE '%INDE%'
                    THEN DATE(2999, 12, 31)
                ELSE
                NULLIF(
                    GREATEST(
                        COALESCE(SAFE.PARSE_DATE('%Y%m%d', HReMaxDate), DATE(1111,1,1)),
                        COALESCE(SAFE.PARSE_DATE('%Y%m%d', HReMaxa), DATE(1111,1,1)), 
                        COALESCE(SAFE.PARSE_DATE('%Y%m%d', HReMaxb), DATE(1111,1,1)), 
                        COALESCE(SAFE.PARSE_DATE('%Y%m%d', HReMaxc), DATE(1111,1,1))
                    ),
                    DATE(1111,1,1)
                ) 
                END AS HReMaxDate,
            SAFE.PARSE_DATE('%Y%m%d', HReDelDate) as HReDelDate,
            HReDelCode,
            "0" as offense_id_num,
            HReOffense as offense_code
        FROM {dbo_Hist_Release}
        WHERE HReDelCode <> '51'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY ParoleNumber, ParoleCountId ORDER BY CAST(HReleaseId AS INT64) DESC) = 1
    ),
  
  -- This CTE dedups the dbo_Sentence table (which seems to hold duplicated sentence records in cases 
  -- where a single sentence is part of multiple sentence groups) to be one row per set of distinct
  -- sentence/charge information.  For sentence external id purposes, we'll keep the highest value Sent16DGroupNumber 
  -- and SentenceId combination for each sentence. For charge external id purposes, we'll assign offense_id_num
  -- for each charge to be CONCAT(Sent16DGroupNumber, "_", SentenceId) since each Sent16DGroupNumber/SentenceId combination
  -- would only be associated with one offense.
  dbo_Sentence_deduped AS (
    SELECT
        ParoleNumber,
        ParoleCountId,
        Sent16DGroupNumber,
        SentenceId,
        SentTerm,
        -- only 3 unparseable dates as of Aug 2024
        SAFE.PARSE_DATE('%Y%m%d', CONCAT(SentYear, SentMonth, SentDay)) as SentDate,
        SentCounty,
        CONCAT(Sent16DGroupNumber, "_", SentenceId) as offense_id_num,
        sentCodeSentOffense as offense_code,
        sentCodeSentOffense as statute,
        # TODO(#33152) aggregate offense descriptions across multiple rows.  See ticket description
        TRIM(CONCAT(COALESCE(SentOffense, ""), " ", COALESCE(SentOffense2, ""), " ", COALESCE(SentOffense3, ""))) as offense_description_orig,
    FROM {dbo_Sentence} 
    QUALIFY 
        ROW_NUMBER() 
        OVER(
             PARTITION BY 
                ParoleNumber, ParoleCountId, SentTerm, SentYear, SentMonth, SentDay, SentOffense, SentOffense2, SentOffense3, SentCounty, sentCodeSentOffense
             ORDER BY 
                CAST(Sent16DGroupNumber AS INT64) DESC, CAST(SentenceId AS INT64) DESC
            ) = 1
  ),

  -- This CTE combines the two different reference tables that crosswalk between offense code and offense description.
  -- Between these two reference files, there is only one offense_code value that appears in both, and in that 
  -- case, we'll prioritize the description from offense_codes.  In addition, offense_codes has additional fields that
  -- RECIDIVIZ_REFERENCE_pbpp_offense_code_descriptions does not, so for the offense codes that only appear in
  -- RECIDIVIZ_REFERENCE_pbpp_offense_code_descriptions, the additional fields will be NULL.
  offense_codes_ref_combined AS (
    SELECT * EXCEPT(offense),
        COALESCE(oc.offense, pbpp.offense) as offense
    FROM {offense_codes} oc
    FULL OUTER JOIN (
        SELECT
            PBPPOffenseCode AS code,
            PBPPOffenseDescription AS offense
        FROM {RECIDIVIZ_REFERENCE_pbpp_offense_code_descriptions}
    ) pbpp 
    USING(code)
  ),

  -- This CTE combines all distinct offense information from all three sources
  -- and then merges on the description and additional offense information found in
  -- the reference files.  For offenses that come from the dbo_Hist_Release table,
  -- only join it on if the offense code doesn't already appear in the dbo_Release_Info
  -- table for that parole count id (in order to prevent dupicate offense info).
  -- At the end, only include rows that contains enough usable information about the
  -- charge (so at least the statute info or description).
  offenses_combined AS (
    SELECT 
        o.*,
        oc.Offense as offense_description_ref, 
        oc.Category, 
        oc.ASCA_Category___Ranked, 
        oc.SubCategory, 
        oc.Grade_Category,
        oc.Grade
    FROM (
        SELECT
            ParoleNumber,
            ParoleCountId,
            offense_id_num,
            offense_code,
            CAST(NULL AS STRING) AS statute,
            CAST(NULL AS STRING) AS offense_description_orig
        FROM dbo_Release_Info_cleaned

        UNION DISTINCT

        SELECT 
            ParoleNumber,
            ParoleCountId,
            dbo_Hist_Release_deduped.offense_id_num,
            dbo_Hist_Release_deduped.offense_code,
            CAST(NULL AS STRING) AS statute,
            CAST(NULL AS STRING) AS offense_description_orig
        FROM dbo_Hist_Release_deduped
        LEFT JOIN dbo_Release_Info_cleaned USING(ParoleNumber, ParoleCountId, offense_code)
        WHERE dbo_Release_Info_cleaned.offense_id_num is null

        UNION DISTINCT

        SELECT
            ParoleNumber,
            ParoleCountId,
            offense_id_num,
            offense_code,
            statute,
            offense_description_orig
        FROM dbo_Sentence_deduped
    ) o 
    LEFT JOIN offense_codes_ref_combined oc
        ON o.offense_code = oc.Code
    WHERE (statute IS NOT NULL or COALESCE(offense_description_orig, oc.Offense) IS NOT NULL)
  ),

  -- This CTE pulls together all distinct sentence information, prioritizing information
  -- from the history table if it's information that appears in both the history and the
  -- current release table.  In addition, we exclude ParoleCountIds 0 and -1 (which are 
  -- incarceration related and invalid respectively), as well as parole counts that were 
  -- deleted because they were opened in error (HReDelCode = 50).
  all_sentence_info AS (
    SELECT DISTINCT
        ParoleNumber,
        ParoleCountId,
        Sent16DGroupNumber,
        SentenceId,
        SentTerm,
        SentDate,
        SentCounty,
        COALESCE(
            HReReldate,            
            DATE(CAST(r.RelReleaseDateYear as INT64), CAST(r.RelReleaseDateMonth as INT64), CAST(r.RelReleaseDateDay as INT64))
            ) AS release_date,
        COALESCE(HReMaxDate, RelMaxDate) as max_date,
        HReDelDate,
        HReDelCode,
    FROM {dbo_Release} r
    FULL OUTER JOIN dbo_Release_Info_cleaned ri
      USING(ParoleNumber, ParoleCountId)
    FULL OUTER JOIN dbo_Hist_Release_deduped h
      USING(ParoleNumber, ParoleCountId)
    FULL OUTER JOIN dbo_Sentence_deduped s
      USING(ParoleNumber, ParoleCountId)
    WHERE 
        ParoleCountId not in ('0', '-1')
        AND (HReDelCode is NULL OR HReDelCode <> '50')
  ),

  -- This CTE joins together the sentence and offense information, and then creates
  -- an array of offenses for each distinct set of sentence information.  For offenses
  -- from the dbo_Sentence table (whose offense_id_num is a concatenation of Sent16DGroupNumber
  -- and SentenceId), only join on the offense to the sentence with the same Sent16DGroupNumber
  -- and SentenceId.  For offenses from the dbo_Hist_Release and dbo_ReleaseInfo tables (whose
  -- offense_id_num are 1, 2, or 3 and therefore don't have an underscore), join the offense onto
  -- every sentence with the same ParoleNumber and ParoleCountId since we are unable to link
  -- those offenses to a specific Sent16DGroupNumber/SentenceId.

  all_info AS (
    SELECT s.*,
        TO_JSON_STRING(ARRAY_AGG(STRUCT<offense_id_num string,
                                        offense_description string,
                                        statute string,
                                        category string,
                                        asca_category string,
                                        subcategory string,
                                        grade_category string,
                                        grade string>
                                 ( o.offense_id_num,
                                   COALESCE(offense_description_ref, o.offense_description_orig),
                                   o.statute,
                                   Category,
                                   ASCA_Category___Ranked,
                                   SubCategory,
                                   Grade_Category,
                                   Grade ) ORDER BY o.offense_id_num)) AS list_of_offense_descriptions
    FROM all_sentence_info s
    LEFT JOIN offenses_combined o
      USING(ParoleNumber, ParoleCountId)
    WHERE 
        -- either the sentence doesn't have matching offense information
        o.ParoleNumber is NULL
        -- or the matching offense information is at the Sent16DGroupNumber and SentenceId level
        OR (CONTAINS_SUBSTR(offense_id_num, "_") AND CONCAT(Sent16DGroupNumber, "_", SentenceId) = offense_id_num)
        -- or the matching offense information is at the ParoleNumber and ParoleCountId level
        OR CONTAINS_SUBSTR(offense_id_num, "_") IS FALSE
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
  )

  SELECT * FROM all_info

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="parole_sentences",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
