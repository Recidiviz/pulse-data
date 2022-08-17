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
"""Preprocessed view of drug screens in Tennessee over the last 20 years, unique on person, date, and sample type"""

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

# Note on TN DRUPs and ZTPDs [8/1/2022]
# -------------------------------------
# When Tennessee conducts drug screens, samples are submitted to a lab. The lab should enter a "DRUP" for any positive
# test and call the officer immediately for any meth-positive result (ZTPD), which must be entered within 2-3 days.
# ZTPDs aren't supposed to be entered without a DRUP, but they sometimes are, and sometimes are entered but well after
# the 3-day window. Moving forward, they expect better compliance with these rules.
# For past data, we assume a "maximum lag" between a DRUP and a ZTPD, and infer DRUPs for ZTPDs without any DRUPs within
# that window. We plotted various lag choices against number of resulting matches to visually gauge the point where a
# longer lag choice wouldn't result in many more matches, but a shorter lag would omit a large number of likely matches.

# Maximum lag allowed between a DRUP contact and a ZTPD contact to associate the two contacts with one drug screen.
US_TN_DRUG_SCREENS_DRUP_ZTPD_MAX_LAG = "21"

US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_NAME = "us_tn_drug_screens_preprocessed"

US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessed view of drug screens in Tennessee over the last 20 years, unique on person, date, and sample type"""

US_TN_DRUG_SCREENS_PREPROCESSED_QUERY_TEMPLATE = """
    /* {description} */
    
    WITH tn_contacts AS
    (
        SELECT 
            DISTINCT p.person_id,
            p.state_code,
            SAFE_CAST(SAFE_CAST(c.ContactNoteDateTime AS datetime) AS DATE) contact_note_date,
            c.ContactNoteType AS contact_note_type,
            c.ContactNoteType LIKE "%DRU%" AS is_dru,
            c.ContactNoteType = "DRUP" AS is_drup,
            c.ContactNoteType = "ZTPD" AS is_ztpd
        FROM `{project_id}.{raw_dataset}.ContactNoteType_latest` c
        LEFT JOIN `{project_id}.{base_dataset}.state_person_external_id` p
        ON
            c.OffenderID = p.external_id
            AND p.state_code = "US_TN"
        WHERE 
            ContactNoteType = "ZTPD"
            OR ContactNoteType LIKE "%DRU%"
    )
    ,
    drup_ztpd_matches AS
    (
        SELECT
            d.person_id,
            d.contact_note_date drup_date,
            z.contact_note_date ztpd_date,
            d.contact_note_type,
            TRUE AS triggered_ztpd,
        FROM tn_contacts d
        JOIN tn_contacts z
        ON 
            d.person_id = z.person_id
            AND DATE_DIFF(z.contact_note_date, d.contact_note_date, DAY) 
                BETWEEN 0 AND {us_tn_drug_screens_drup_ztpd_max_lag}
    
        WHERE 
            d.is_drup
            AND z.is_ztpd
      
        --The following two qualify statements dedup so that the ZTPD to DRUP matching is 1 to 1. 
        
        --In cases where a person has more than one DRUP contact that precede a ZTPD contact by less than {us_tn_drug_screens_drup_ztpd_max_lag} days, 
        --we only want the latest DRUP contact to be associated with the ZTPD. 
        
        --In cases where a person has more than one ZTPD contact that follow a DRUP contact by less than {us_tn_drug_screens_drup_ztpd_max_lag} days, 
        --we only want the earliest ZTPD to be associated with the DRUP. 
        
        QUALIFY 
            ROW_NUMBER() OVER(PARTITION BY person_id, ztpd_date ORDER BY drup_date DESC) = 1 AND
            ROW_NUMBER() OVER(PARTITION BY person_id, drup_date ORDER BY ztpd_date ASC) = 1
    )
    ,
    -- Identify the set of ZTPD contacts that are not captured in `drup_ztpd_matches`, so that these can be added 
    -- back in as DRUPs that triggered a ZTPD
    ztpd_no_matches AS
    (
        SELECT z.person_id, z.state_code, z.contact_note_date, "DRUP" contact_note_type, true triggered_ztpd 
        FROM tn_contacts z
        LEFT JOIN drup_ztpd_matches m
        ON 
            z.contact_note_date = m.ztpd_date
            AND z.person_id = m.person_id
        WHERE m.ztpd_date IS NULL AND z.is_ztpd
    )
    ,
    dru_with_ztpd_from_original_table AS
    (
        SELECT d.person_id, d.state_code, d.contact_note_date, d.contact_note_type, m.ztpd_date IS NOT NULL triggered_ztpd 
        FROM tn_contacts d
        LEFT JOIN drup_ztpd_matches m
        ON 
            d.person_id = m.person_id
            AND d.contact_note_date = m.drup_date
            AND d.is_drup
        WHERE d.is_dru
    )
    ,
    dru_with_ztpd_full_table AS
    (
        SELECT *, false is_inferred, CAST(NULL AS STRING) AS sample_type FROM dru_with_ztpd_from_original_table
        UNION ALL
        SELECT *, true is_inferred, CAST(NULL AS STRING) AS sample_type FROM ztpd_no_matches
    )
    SELECT
        person_id,
        state_code,
        contact_note_date AS drug_screen_date,
        sample_type,
        # Assumes that if no result is reported for a test, the outcome was negative.
        COALESCE(LOGICAL_OR(contact_note_type = "DRUP") OVER w, FALSE) AS is_positive_result,
        
        # Get the primary raw text result value, prioritizing tests with non-null results and then using alphabetical order of raw text
        FIRST_VALUE(contact_note_type IGNORE NULLS) OVER w AS result_raw_text_primary,
        
        # Store an array of all raw text test results for a single drug screen date
        ARRAY_AGG(COALESCE(contact_note_type, 'UNKNOWN')) OVER w AS result_raw_text,
        IF(triggered_ztpd, "METH", CAST(NULL AS STRING)) AS substance_detected,
        is_inferred
    FROM dru_with_ztpd_full_table
    WHERE contact_note_date >= DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 20 YEAR)
    QUALIFY ROW_NUMBER() OVER w = 1
    WINDOW w AS (
        PARTITION BY person_id, state_code, contact_note_date, sample_type
        -- Prioritize positives and all "passed" negative screens above DRULs, which means "awaiting lab results"
        ORDER BY CASE contact_note_type WHEN "DRUP" THEN 1 
                                          WHEN "DRUN" THEN 2 
                                          WHEN "DRUM" THEN 3
                                          WHEN "DRUX" THEN 4
                                          ELSE 5 END, 
    )
"""

US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_NAME,
    base_dataset=STATE_BASE_DATASET,
    description=US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_TN_DRUG_SCREENS_PREPROCESSED_QUERY_TEMPLATE,
    should_materialize=False,
    raw_dataset=raw_latest_views_dataset_for_region("us_tn"),
    us_tn_drug_screens_drup_ztpd_max_lag=US_TN_DRUG_SCREENS_DRUP_ZTPD_MAX_LAG,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER.build_and_print()
