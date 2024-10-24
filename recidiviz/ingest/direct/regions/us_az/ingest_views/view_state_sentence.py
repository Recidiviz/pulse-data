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
Ingest view for StateSentence entities.

Sentencing data exists at three levels in AZ:
    - "offenses" are sentence and charge level concepts and their data maps to StateSentence entities
    - "commitments" are sentences and charges that share an imposition. While multiple sentences or
      charges may relate to a single commitment, data is still at the StateSentence/StateCharge level.
    - "episodes" are stints a person can have with the system and is comprised of offenses and commitments
      For example the AZ_DOC_SC_EPISODE file contains projected dates for 
      "all active offenses in all active commitments in this episode"

It follow that a StatePerson relates to StateSentenceGroup entities (episodes), and those
groups are related to StateSentence entities (made up of offenses imposed together in commitments)
that related to StateCharge entities (created from commitment data).


TODO(#29562) We default to STATE_PRISON sentences, but there may be some PROBATION sentences to hydrate.
"""
from recidiviz.ingest.direct.regions.us_az.ingest_views.common_sentencing_views_and_utils import (
    CAPITAL_PUNISHMENT_IDS,
    CONSECUTIVE_SENTENCE_ID,
    LIFE_SENTENCE_IDS,
    OUT_OF_STATE_COUNTY_ID,
    PAROLE_POSSIBLE_ID,
    VALID_PEOPLE_AND_SENTENCES,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#29562) Hydrate sentence_type (the OFFENSE table may just be STATE_PRISON sentences)
# Don't use SENTENCE_BEGIN_DTM! It is not when a sentence is imposed, but a calculated start
# date. It can be in the future for consecutive sentences.
OFFENSE_LEVEL_DATA = f"""
SELECT
    OFFENSE_ID,
    ARS_ID,
    SAFE_CAST(NUM_JAIL_CREDIT_DAYS AS INT64) AS NUM_JAIL_CREDIT_DAYS,
    LIFE_OR_DEATH_ID IN {LIFE_SENTENCE_IDS} AS is_life,
    LIFE_OR_DEATH_ID IN {CAPITAL_PUNISHMENT_IDS} AS is_capital_punishment,
    PAROLE_ELIGIBILITY_ID = '{PAROLE_POSSIBLE_ID}' AS parole_possible,
    CAST(NULLIF(OFFENSE_DTM, 'NULL') AS DATETIME) AS OFFENSE_DTM,
    NULLIF(SUBSECTION_CODE, 'NULL') AS SUBSECTION_CODE, -- for statute
    OFFENSE_NUMBER -- charge count
FROM
    {{AZ_DOC_SC_OFFENSE}} AS sentence
"""

COMMITMENT_LEVEL_DATA = f"""
SELECT
    COMMITMENT_ID,
    SC_EPISODE_ID,
    DATE(SENTENCED_DTM) AS SENTENCED_DTM,
    CASE WHEN 
        county_lookup.LOOKUP_ID = '{OUT_OF_STATE_COUNTY_ID}'
        THEN 'OUT_OF_STATE'
        ELSE county_lookup.DESCRIPTION
    END AS county, 
    -- Y/N, if Y then StateSentencingAuthority.OTHER_STATE
    ICC_COMMITMENT_FLAG,
    NULLIF(JUDGE_ID, 'NULL') AS JUDGE_ID
FROM
    {{AZ_DOC_SC_COMMITMENT}}
LEFT JOIN
    {{LOOKUPS}} AS county_lookup
ON 
    COUNTY_ID = county_lookup.LOOKUP_ID
"""

ARS_DATA_FOR_CHARGES = """
SELECT 
    ARS_ID,
    NCIC_CODE,
    ARS_CODE,
    DESCRIPTION,
    MURDER_DEGREE,
    VIOLENCE_FLAG,
    SEX_OFFENSE_FLAG,
    CASE 
        WHEN TRNSTN_PGM_RLS_INELIG_FLAG = 'Y' THEN 'ELIGIBLE FOR TPR'
        WHEN TRNSTN_PGM_RLS_INELIG_FLAG = 'N' THEN 'INELIGIBLE FOR TPR'
        ELSE CAST(NULL AS STRING)
    END AS TPR_ELIGIBILITY_NOTE
FROM 
    {AZ_DOC_SC_ARS_CODE}
"""

VIEW_QUERY_TEMPLATE = f"""
WITH

-- Subsets to sentences we know have valid imposition
-- and also links to person and controlling charge
valid_sentences AS ({VALID_PEOPLE_AND_SENTENCES}),

-- Has data to hydrate StateSentence
offenses AS ({OFFENSE_LEVEL_DATA}),

-- Has imposition information and links to episode
commitments AS ({COMMITMENT_LEVEL_DATA}),

-- Has charge detail information like NCIC code
ars_data_for_charges AS ({ARS_DATA_FOR_CHARGES}),

-- Has a flag denoting whether a sentence is a flat sentence.
flat_sentence_flags AS (
    SELECT DISTINCT 
        OFFENSE_ID,
        FLAT_SENT_FLAG
    FROM {{AZ_DOC_SC_EXCEPTION}}
),

-- Maps any sentence to its parent sentences if it is to be served
-- consecutively. Note that a consecutive sentence may be in a
-- separate 'commitment'
-- We define it here because we need to join parent 
-- sentences to valid sentences. Otherwise we could point
-- to a sentence ID for a sentence that isn't hydrated
consecutive_sentences AS (
SELECT
    CC_CONS_OFFENSE_ID AS OFFENSE_ID,
    STRING_AGG(OFFENSE_ID, ',' ORDER BY OFFENSE_ID) AS parent_sentence_external_id_array
FROM
    {{AZ_DOC_SC_MAPPING}}
JOIN
    valid_sentences
USING
    (OFFENSE_ID)
WHERE 
    TYPE_ID = '{CONSECUTIVE_SENTENCE_ID}'
GROUP BY
    CC_CONS_OFFENSE_ID
)
SELECT
    PERSON_ID,
    SC_EPISODE_ID,        -- Unique for each StateSentenceGroup
    OFFENSE_ID,           -- Unique for each StateSentence
    SENTENCED_DTM,        -- imposed_date
    NUM_JAIL_CREDIT_DAYS, -- initial_time_served_days
    ICC_COMMITMENT_FLAG,  -- for sentencing_authority
    county,
    is_life,
    is_capital_punishment,
    parole_possible,
    parent_sentence_external_id_array,
    -- Charge only info
    JUDGE_ID,            -- judge external ID
    OFFENSE_NUMBER,      -- charge count
    OFFENSE_DTM,
    NCIC_CODE,
    ARS_CODE,            -- used for statute
    SUBSECTION_CODE,     -- used for statute
    DESCRIPTION,
    MURDER_DEGREE,       -- classification_subtype
    VIOLENCE_FLAG,
    SEX_OFFENSE_FLAG,
    TPR_ELIGIBILITY_NOTE,
    OFFENSE_ID = FINAL_OFFENSE_ID AS is_controlling,
    FLAT_SENT_FLAG
FROM 
    valid_sentences
JOIN
    commitments
USING
    (COMMITMENT_ID, SC_EPISODE_ID)
JOIN 
    offenses
USING
    (OFFENSE_ID)
LEFT JOIN
    consecutive_sentences
USING
    (OFFENSE_ID)
LEFT JOIN
    ars_data_for_charges
USING
    (ARS_ID)
LEFT JOIN 
    flat_sentence_flags
USING  
    (OFFENSE_ID)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="state_sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
