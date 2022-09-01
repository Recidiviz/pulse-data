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
"""Preprocessed view of drug screens in Idaho over the last 20 years, unique on person, date, and sample type"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_DRUG_SCREENS_PREPROCESSED_VIEW_NAME = "us_id_drug_screens_preprocessed"

US_ID_DRUG_SCREENS_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessed view of drug screens in Idaho over the last 20 years, unique on person, date, and sample type"""

US_ID_DRUG_SCREENS_PREPROCESSED_QUERY_TEMPLATE = """
    /* {description} */
    WITH drug_screens_raw_cte AS (
        SELECT DISTINCT
            person_id,
            p.state_code,
            SAFE_CAST(SAFE_CAST(t.smpl_rqst_dt AS DATETIME) AS DATE) AS drug_screen_date,
            CASE t.smpl_typ_cd 
                WHEN "A" THEN "ADMISSION"
                WHEN "B" THEN "BREATH"
                WHEN "H" THEN "HAIR"
                WHEN "L" THEN "BLOOD"
                WHEN "S" THEN "SALIVA"
                WHEN "T" THEN "NOT_TAKEN"
                WHEN "U" THEN "URINE"
                WHEN "W" THEN "SWEAT"
                END AS sample_type,
            r.sbstnc_found_flg AS result_raw_text,
            r.med_invalidate_flg,
        FROM 
            `{project_id}.{raw_dataset}.sbstnc_tst_latest` t
        LEFT JOIN 
            `{project_id}.{raw_dataset}.sbstnc_rslt_latest` r
        USING (tst_id)
        LEFT JOIN 
            `{project_id}.{base_dataset}.state_person_external_id` p
        ON 
            t.ofndr_num = p.external_id
            AND p.state_code = "US_ID"
        WHERE 
            SAFE_CAST(SAFE_CAST(t.smpl_rqst_dt AS DATETIME) AS DATE) >= DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 20 YEAR)
    )
    SELECT 
        person_id,
        state_code,
        drug_screen_date,
        sample_type,
        # Assumes that if no result is reported for a test, the outcome was negative.
        COALESCE(
            LOGICAL_OR(result_raw_text = "Y") OVER w,
            FALSE
        ) AS is_positive_result,
        
        # Get the primary raw text result value, prioritizing tests with non-null results and then using alphabetical order of raw text
        FIRST_VALUE(result_raw_text IGNORE NULLS) OVER (
            PARTITION BY person_id, state_code, drug_screen_date, sample_type
            ORDER BY CASE result_raw_text WHEN "Y" THEN 1 WHEN "N" THEN 2 ELSE 3 END, 
            result_raw_text
        ) AS result_raw_text_primary,
        
        # Store an array of all raw text test results for a single drug screen date
        ARRAY_AGG(COALESCE(result_raw_text, 'UNKNOWN')) OVER w AS result_raw_text,
        CAST(NULL AS STRING) AS substance_detected,
        med_invalidate_flg,
        FALSE AS is_inferred,
    FROM drug_screens_raw_cte
    QUALIFY ROW_NUMBER() OVER w = 1
    WINDOW w AS (PARTITION BY person_id, state_code, drug_screen_date, sample_type)
"""

US_ID_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_ID_DRUG_SCREENS_PREPROCESSED_VIEW_NAME,
    base_dataset=STATE_BASE_DATASET,
    description=US_ID_DRUG_SCREENS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_ID_DRUG_SCREENS_PREPROCESSED_QUERY_TEMPLATE,
    should_materialize=False,
    raw_dataset=raw_latest_views_dataset_for_region("us_id"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER.build_and_print()
