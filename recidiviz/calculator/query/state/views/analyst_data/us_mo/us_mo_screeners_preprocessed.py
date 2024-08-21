# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Preprocessing file pulls on raw data from MO's MOCIS tables that contain information on various screeners
and assessments taken by people within MO's system."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    NORMALIZED_STATE_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_SCREENERS_PREPROCESSED_VIEW_NAME = "us_mo_screeners_preprocessed"

US_MO_SCREENERS_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessing file pulls on raw data from MO's MOCIS tables
that contain information on various screeners and assessments taken by people within MO's system.
- The ICASA and TCUD (MOCIS) and SACA (OPII) are substance use screeners
- The CMHS (MOCIS) and Mental Health (OPII) are mental health screeners
- ORAS provides an overall risk assessment
These are needed for a one-off creation of in-facility program tracks and so are not currently ingested as
part of state_assessment which only has ORAS, but can be."""

US_MO_SCREENERS_PREPROCESSED_QUERY_TEMPLATE = """
-- TODO(#19220): Deprecate view once these screeners are ingested into state_assessment
WITH MH_and_E AS (
    /* This CTE joins data on classification/custody levels (tak015) to classification comments (tak068)
    to get the date of a classification and the associated scores for mental health and education. Not every
    classification entry will have these scores, so the inner join keeps the relevant ones, and at a later stage
    we keep the latest assessment date */
    SELECT
        person_id,
        external_id,
        SAFE.PARSE_DATE('%Y%m%d', tak015.BL_IC) AS assessment_date,
        CAST(tak068.CT_HCI AS INT64) AS assessment_score,
        CASE WHEN tak068.CT_CSD = 'MH' THEN 'mental_health'
             WHEN tak068.CT_CSD = 'E' THEN 'education'
             END AS assessment_type,
        tak068.CT_CSD AS assessment_type_raw_text,
  FROM `{project_id}.{raw_data_up_to_date_views_dataset}.LBAKRDTA_TAK015_latest` tak015
  INNER JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.LBAKRDTA_TAK068_latest` tak068
    ON BL_DOC = CT_DOC
    AND BL_CYC = CT_CYC
    AND BL_CNO = CT_CNO
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
    ON tak015.BL_DOC = pei.external_id
    AND pei.state_code = 'US_MO'
  WHERE tak068.CT_CSD IN ('MH','E')
  -- When there are two assessments on the same day of the same type, we deduplicate using CNO which is part of the primary key.
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, tak068.CT_CSD, SAFE.PARSE_DATE('%Y%m%d', tak015.BL_IC) 
                            ORDER BY tak015.BL_CNO DESC) = 1   
),
SACA_screener AS (
    SELECT
        person_id,
        external_id,
        SAFE.PARSE_DATE('%Y%m%d', HL_BL) AS assessment_date,
        -- Confirmed with MO that this is the "total score" we should use
        CAST(HL_SNS AS INT64) AS assessment_score,
        'SACA' AS assessment_type,
        'SACA' AS assessment_type_raw_text,
    FROM `{project_id}.{raw_data_up_to_date_views_dataset}.LBAKRDTA_TAK204_latest`
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
        ON HL_DOC = pei.external_id
        AND pei.state_code = 'US_MO'
    -- Deduplicate very small number of duplicates on same day by taking higher score
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, SAFE.PARSE_DATE('%Y%m%d', HL_BL)
                            ORDER BY CAST(HL_SNS AS INT64) DESC) = 1
),
MOCIS_screeners AS (
    /* This cte manipulates a completely different set of MOCIS tables for certain assessments and builds off a query
    sent over by MO */
    SELECT pei.person_id,
           doc_id_xref.DOC_ID AS external_id,
           doc_id_xref.OFNDR_CYCLE_REF_ID AS ofndr_cycle_ref,
           CAST(PARSE_DATE('%Y%m%d',doc_id_xref.CYCLE_NO) AS DATE) AS cycle_num_date,
           CAST(PARSE_DATE('%Y-%m-%d',asmt.ASSESSMENT_DATE) AS DATE) AS assessment_date,
           asmt.OFNDR_ASMNT_REF_ID AS assessment_id,
           /* Determining this based on the range of scores - the MO algorithm says the women's test ranges from
           0-8 which is what we see for 950, the men's test ranges from 0-12 which is what we see for 940 */
           CASE WHEN asmt.ASSESSMENT_TOOL_REF_ID = '950'
                THEN CONCAT(asmt.ASSESSMENT_TOOL_REF_ID,'-','WOMEN')
                WHEN asmt.ASSESSMENT_TOOL_REF_ID = '940'
                THEN CONCAT(asmt.ASSESSMENT_TOOL_REF_ID,'-','MEN')
                -- These were given by MO
                WHEN asmt.ASSESSMENT_TOOL_REF_ID = '320'
                THEN CONCAT(asmt.ASSESSMENT_TOOL_REF_ID,'-','MEN')
                WHEN asmt.ASSESSMENT_TOOL_REF_ID = '340'
                THEN CONCAT(asmt.ASSESSMENT_TOOL_REF_ID,'-','WOMEN')
                ELSE asmt.ASSESSMENT_TOOL_REF_ID
                END AS assessment_type_raw_text,
           -- These were given by MO
           CASE WHEN asmt.ASSESSMENT_TOOL_REF_ID IN ('320','340') THEN 'ICASA'
                WHEN asmt.ASSESSMENT_TOOL_REF_ID IN ('940','950') THEN 'CMHS'
                WHEN asmt.ASSESSMENT_TOOL_REF_ID IN ('960') THEN 'TCUD'
                END AS assessment_type,
           /* RAW and SCORING TYPE appear to almost always be the same. RAW and OVERRIDE are nearly always different.
           OVERRIDE appears to be almost always 0 except when OVERRIDE_TOOL_SCORE_IND = 'Y'. MO says there should be no
           override options for any of these assessments, so we confirmed only using the raw score value */
           CAST(CAST(scores.RAW_TOOL_SCORE_VALUE AS FLOAT64) AS INT64) AS assessment_score,
           CAST(CAST(scores.SCORING_TYPE_SCORE_VALUE AS FLOAT64) AS INT64) AS score_type_value,
           CAST(CAST(scores.OVERRIDE_TOOL_SCORE_VALUE AS FLOAT64) AS INT64) AS override_score_value,
           eval.SCORE_INTERPRETATION AS assessment_level_raw_text,
    -- This view joins Offender/Cycles in MOCIS to the DOC_ID used in OPII
    FROM `{project_id}.{raw_data_up_to_date_views_dataset}.OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF_latest` AS doc_id_xref
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
        ON doc_id_xref.DOC_ID = pei.external_id
        AND pei.state_code = 'US_MO'
    -- This table has all the assessments taken by a given person
    INNER JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.OFNDR_PDB_OFNDR_ASMNTS_latest` AS asmt
        ON doc_id_xref.OFNDR_CYCLE_REF_ID = asmt.OFNDR_CYCLE_REF_ID
    -- This table has scores for all assessments
    LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.OFNDR_PDB_OFNDR_ASMNT_SCORES_latest` scores
        ON asmt.OFNDR_ASMNT_REF_ID = scores.OFNDR_ASMNT_REF_ID
    -- This is a reference table that provides interpretations for the scores for a given assessment
    LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.MASTER_PDB_ASSESSMENT_EVALUATIONS_latest` eval
        ON asmt.ASSESSMENT_TOOL_REF_ID = eval.ASSESSMENT_TOOL_REF_ID
        AND CAST(scores.RAW_TOOL_SCORE_VALUE AS FLOAT64) BETWEEN CAST(eval.FROM_SCORE AS FLOAT64)
                                                         AND CAST(eval.TO_SCORE AS FLOAT64)
    WHERE doc_id_xref.DELETE_IND = 'N'
        AND asmt.DELETE_IND = 'N'
        AND scores.DELETE_IND = 'N'
        AND eval.DELETE_IND = 'N'
        AND asmt.ASSESSMENT_TOOL_REF_ID IN ('960','320','340','940','950')
        -- This is the row for "overall score"
        AND scores.ASMNT_SCORING_TYPE_CD = 'OSC'
    -- Deal with a smallish number of duplicates on person, assessment date, assessment type
    QUALIFY ROW_NUMBER() OVER(PARTITION BY doc_id_xref.OFNDR_CYCLE_REF_ID,
                                            asmt.ASSESSMENT_DATE, 
                                            asmt.ASSESSMENT_TOOL_REF_ID
                              ORDER BY asmt.CREATE_TS DESC, 
                                        scores.CREATE_TS DESC) = 1
)
SELECT
    "US_MO" AS state_code,
    person_id,
    external_id,
    assessment_date,
    assessment_id,
    assessment_type,
    assessment_type_raw_text,
    NULL AS assessment_level,
    assessment_level_raw_text,
    assessment_score,
    score_type_value,
    override_score_value,
FROM MOCIS_screeners

UNION ALL

SELECT
    "US_MO" AS state_code,
    person_id,
    external_id,
    assessment_date,
    CAST(assessment_id AS STRING) AS assessment_id,
    'ORAS' AS assessment_type,
    assessment_type_raw_text,
    assessment_level,
    assessment_level_raw_text,
    assessment_score,
    NULL AS score_type_value,
    NULL AS override_score_value,
FROM `{project_id}.{normalized_state_dataset}.state_assessment`    
WHERE state_code = 'US_MO' AND
    -- TODO(#31029): This union can be simplified by removing the next subquery and pulling
    -- both types of assessment from ingested data once #30728 is merged.
    assessment_type_raw_text IN (
        'DIVERSION INSTRUMENT',
        'PRISON SCREENING TOOL',
        'COMMUNITY SUPERVISION SCREENING TOOL - 9 ITEMS',
        'COMMUNITY SUPERVISION SCREENING TOOL - 4 ITEMS',
        'COMMUNITY SUPERVISION TOOL',
        'PRISON INTAKE TOOL',
        'REENTRY TOOL',
        'REENTRY INSTRUMENT',
        'SUPPLEMENTAL REENTRY TOOL'
    )

    
-- There are 2% duplicates on person_id and assessment_date, so we prioritize
-- prison intake and community supervision tool
QUALIFY RANK() OVER(PARTITION BY person_id, assessment_date 
                    ORDER BY CASE WHEN assessment_type_raw_text = 'PRISON INTAKE TOOL' THEN 0
                                 WHEN assessment_type_raw_text = 'COMMUNITY SUPERVISION TOOL' THEN 1
                                 WHEN assessment_type_raw_text LIKE 'PRISON%'  THEN 2
                                 WHEN assessment_type_raw_text = 'REENTRY TOOL'  THEN 3
                                    ELSE 4 END)=1

UNION ALL

SELECT
    "US_MO" AS state_code,
    person_id,
    external_id,
    assessment_date,
    NULL AS assessment_id,
    assessment_type,
    assessment_type_raw_text,
    NULL assessment_level,
    NULL assessment_level_raw_text,
    assessment_score,
    NULL AS score_type_value,
    NULL AS override_score_value,
FROM MH_and_E

UNION ALL

SELECT
    "US_MO" AS state_code,
    person_id,
    external_id,
    assessment_date,
    NULL AS assessment_id,
    assessment_type,
    assessment_type_raw_text,
    NULL assessment_level,
    NULL assessment_level_raw_text,
    assessment_score,
    NULL AS score_type_value,
    NULL AS override_score_value,
FROM SACA_screener

"""

US_MO_SCREENERS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_MO_SCREENERS_PREPROCESSED_VIEW_NAME,
    view_query_template=US_MO_SCREENERS_PREPROCESSED_QUERY_TEMPLATE,
    description=US_MO_SCREENERS_PREPROCESSED_VIEW_DESCRIPTION,
    should_materialize=True,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MO,
        instance=DirectIngestInstance.PRIMARY,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_SCREENERS_PREPROCESSED_VIEW_BUILDER.build_and_print()
