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
"""Query containing classification assessments information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
    -- This CTE retrieves all SACA assessments.
    SACA_screener AS (
        SELECT
            CAST(HL_DOC AS STRING) AS DOC_ID,
            CAST(HL_CYC AS STRING) AS CYC,
            CONCAT(HL_BL, HL_SNS) AS assessment_id,
            SAFE.PARSE_DATE('%Y%m%d', HL_BL) AS assessment_date,
            'SACA' AS assessment_type,
            -- Confirmed with MO that this is the "total score" we should use
            CAST(CAST(HL_SNS AS FLOAT64) AS INT64) AS assessment_score,
            CAST(CAST(NULL AS FLOAT64) AS INT64) AS score_type_value,
            CAST(CAST(NULL AS FLOAT64) AS INT64) AS override_score_value,
        FROM {LBAKRDTA_TAK204}
    ),
    -- This CTE retrieves all assessments including MH and E.
    MH_E_screener AS (
    SELECT 
        CAST(BL_DOC AS STRING) AS DOC_ID,
        CAST(BL_CYC AS STRING) AS CYC,
        CAST(BL_CNO AS STRING) AS assessment_id,
        SAFE.PARSE_DATE('%Y%m%d', tak015.BL_IC) AS assessment_date,
        CONCAT("CLASSIFICATION-", CT_CSD) AS assessment_type,
        CAST(CAST(tak068.CT_HCI AS FLOAT64) AS INT64) AS assessment_score,
        CAST(CAST(NULL AS FLOAT64) AS INT64) AS score_type_value,
        CAST(CAST(NULL AS FLOAT64) AS INT64) AS override_score_value
    FROM {LBAKRDTA_TAK015} tak015
    INNER JOIN {LBAKRDTA_TAK068} tak068
        ON BL_DOC = CT_DOC
        AND BL_CYC = CT_CYC
        AND BL_CNO = CT_CNO
    ),
    -- This CTE retrieves all assessments under MOCIS.
    MOCIS_screener AS (
    /* This cte manipulates a completely different set of MOCIS tables for certain assessments and builds off a query
    sent over by MO */
    SELECT CAST(DOC_ID AS STRING) AS DOC_ID,
           CAST(doc_id_xref.OFNDR_CYCLE_REF_ID AS STRING) AS CYC,
           CAST(asmt.OFNDR_ASMNT_REF_ID AS STRING) AS assessment_id,
           -- CAST(PARSE_DATE('%Y%m%d',doc_id_xref.CYCLE_NO) AS DATE) AS cycle_num_date,
           CAST(PARSE_DATE('%Y-%m-%d',asmt.ASSESSMENT_DATE) AS DATE) AS assessment_date,
           -- asmt.OFNDR_ASMNT_REF_ID AS assessment_id,
           /* Determining this based on the range of scores - the MO algorithm says the women's test ranges from
           0-8 which is what we see for 950, the men's test ranges from 0-12 which is what we see for 940 */
           CASE 
                WHEN asmt.ASSESSMENT_TOOL_REF_ID = '950'
                THEN CONCAT(asmt.ASSESSMENT_TOOL_REF_ID,'-','WOMEN')
                WHEN asmt.ASSESSMENT_TOOL_REF_ID = '940'
                THEN CONCAT(asmt.ASSESSMENT_TOOL_REF_ID,'-','MEN')
                -- These were given by MO
                WHEN asmt.ASSESSMENT_TOOL_REF_ID = '320'
                THEN CONCAT(asmt.ASSESSMENT_TOOL_REF_ID,'-','MEN')
                WHEN asmt.ASSESSMENT_TOOL_REF_ID = '340'
                THEN CONCAT(asmt.ASSESSMENT_TOOL_REF_ID,'-','WOMEN')
                ELSE CAST(asmt.ASSESSMENT_TOOL_REF_ID AS STRING)
                END AS assessment_type,
           /* RAW and SCORING TYPE appear to almost always be the same. RAW and OVERRIDE are nearly always different.
           OVERRIDE appears to be almost always 0 except when OVERRIDE_TOOL_SCORE_IND = 'Y'. MO says there should be no
           override options for any of these assessments, so we confirmed only using the raw score value */
           CAST(CAST(scores.RAW_TOOL_SCORE_VALUE AS FLOAT64) AS INT64) AS assessment_score,
           CAST(CAST(scores.SCORING_TYPE_SCORE_VALUE AS FLOAT64) AS INT64) AS score_type_value,
           CAST(CAST(scores.OVERRIDE_TOOL_SCORE_VALUE AS FLOAT64) AS INT64) AS override_score_value,
           -- eval.SCORE_INTERPRETATION AS assessment_level_raw_text,
    -- This view joins Offender/Cycles in MOCIS to the DOC_ID used in OPII
    FROM {OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF} AS doc_id_xref
    -- This table has all the assessments taken by a given person
    INNER JOIN {OFNDR_PDB_OFNDR_ASMNTS} AS asmt
        ON doc_id_xref.OFNDR_CYCLE_REF_ID = asmt.OFNDR_CYCLE_REF_ID
    -- This table has scores for all assessments
    LEFT JOIN {OFNDR_PDB_OFNDR_ASMNT_SCORES} scores
        ON asmt.OFNDR_ASMNT_REF_ID = scores.OFNDR_ASMNT_REF_ID
    -- This is a reference table that provides interpretations for the scores for a given assessment
    LEFT JOIN {MASTER_PDB_ASSESSMENT_EVALUATIONS} eval
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
                                            asmt.ASSESSMENT_DATE, asmt.ASSESSMENT_TOOL_REF_ID
                              ORDER BY asmt.CREATE_TS DESC, scores.CREATE_TS DESC) = 1
)
SELECT * FROM SACA_screener
UNION ALL
SELECT * FROM MH_E_screener
UNION ALL 
SELECT * FROM MOCIS_screener
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="classification_assessments",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
