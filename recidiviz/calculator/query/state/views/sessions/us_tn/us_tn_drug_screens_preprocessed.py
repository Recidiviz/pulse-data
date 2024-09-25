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
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Note: A more detailed methodology can be found here:
# https://docs.google.com/document/d/1vfFNslIE2GjhDTAghIw1zIyeU4aRq8rABadxz3ZmBEQ

# Maximum lag allowed between a DRUP contact and a ZTPD contact to associate the two contacts with one drug screen.
US_TN_DRUG_SCREENS_DRUP_ZTPD_MAX_LAG = "21"

# Maximum and minimum lag allowed between a Contacts-based drug test and a DrugTestDrugClass-based drug test
US_TN_DRUG_SCREENS_DRUGTEST_CONTACT_MAX_LAG = "21"
US_TN_DRUG_SCREENS_DRUGTEST_CONTACT_MIN_LAG = "-2"

# Please note that while we found similar inflection points in both the DRUP-ZTPD lag and the DrugTestDrugClass-Contacts
# lag, the similarity in these choices is likely coincidental, though there may be factors in the data entry process
# that result in this similarity.

US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_NAME = "us_tn_drug_screens_preprocessed"

US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_DESCRIPTION = """
Preprocessed view of drug screens in Tennessee over the last 20 years, unique on person,
date, and sample type"""

US_TN_DRUG_SCREENS_PREPROCESSED_QUERY_TEMPLATE = """
   
    WITH tn_contacts AS
    (
        SELECT 
            DISTINCT p.person_id,
            p.state_code,
            SAFE_CAST(SAFE_CAST(c.ContactNoteDateTime AS datetime) AS DATE) contact_note_date,
            c.ContactNoteType AS contact_note_type,
            c.ContactNoteType IN (
                "DRUN", "DRUX", "DRUM", "DRUP"
            ) AS is_dru,
            c.ContactNoteType = "DRUP" AS is_drup,
            c.ContactNoteType = "ZTPD" AS is_ztpd
        FROM `{project_id}.{raw_dataset}.ContactNoteType_latest` c
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` p
        ON
            c.OffenderID = p.external_id
            AND p.state_code = "US_TN"
        WHERE 
            -- Excludes DRUL (awaiting lab results) and XDRU (screen not completed)
            ContactNoteType IN ("DRUN", "DRUX", "DRUM", "DRUP", "ZTPD")
    )
    ,
    drup_ztpd_matches AS
    (
        SELECT
            d.person_id,
            d.contact_note_date drup_date,
            z.contact_note_date ztpd_date,
            d.contact_note_type,
            TRUE AS is_meth_positive,
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
        
        --In cases where a person has more than one DRUP contact that precede a ZTPD 
        --contact by less than {us_tn_drug_screens_drup_ztpd_max_lag} days, 
        --we only want the latest DRUP contact to be associated with the ZTPD. 
        
        --In cases where a person has more than one ZTPD contact that follow a DRUP 
        --contact by less than {us_tn_drug_screens_drup_ztpd_max_lag} days, 
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
        SELECT 
            z.person_id, 
            z.state_code, 
            z.contact_note_date, 
            "DRUP" contact_note_type, 
            true is_meth_positive 
        FROM tn_contacts z
        LEFT JOIN drup_ztpd_matches m
        ON 
            z.contact_note_date = m.ztpd_date
            AND z.person_id = m.person_id
        WHERE 
            m.ztpd_date IS NULL 
            AND z.is_ztpd
    )
    ,
    dru_with_ztpd_from_original_table AS
    (
        SELECT 
            d.person_id, 
            d.state_code, 
            d.contact_note_date, 
            d.contact_note_type, 
            m.ztpd_date IS NOT NULL AS is_meth_positive 
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
        SELECT 
            *, 
            false is_inferred, 
            CAST(NULL AS STRING) AS sample_type 
        FROM dru_with_ztpd_from_original_table
        UNION ALL
        SELECT 
            *, 
            true is_inferred, 
            CAST(NULL AS STRING) AS sample_type 
        FROM ztpd_no_matches
    )
    ,
    contacts_based_drug_screens AS 
    (
        SELECT
            person_id,
            state_code,
            contact_note_date AS drug_screen_date,
            sample_type,
            # Assumes that if no result is reported for a test, the outcome was negative.
            COALESCE(LOGICAL_OR(contact_note_type = "DRUP") OVER w, FALSE) AS is_positive_result,
            
            # Get the primary raw text result value, prioritizing tests with non-null results 
            # and then using alphabetical order of raw text
            FIRST_VALUE(contact_note_type IGNORE NULLS) OVER w AS result_raw_text_primary,
            
            # Store an array of all raw text test results for a single drug screen date
            ARRAY_AGG(COALESCE(contact_note_type, 'UNKNOWN')) OVER w AS result_raw_text,
            is_inferred,
            is_meth_positive
        FROM dru_with_ztpd_full_table
        WHERE contact_note_date >= DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 20 YEAR)
        QUALIFY ROW_NUMBER() OVER w = 1
        WINDOW w AS (
            PARTITION BY person_id, state_code, contact_note_date, sample_type
            -- Prioritize positive screens over other screens, and "valid" negative screens over actual negative screens
            -- so that the valid negative screens are prioritized as negative results when matched to DrugTestDrugClass
            -- (cases of duplicate negative contact codes are rare, < 0.1% of cases with any negative contact)
            ORDER BY CASE contact_note_type WHEN "DRUP" THEN 1 
                                              WHEN "DRUM" THEN 2 
                                              WHEN "DRUX" THEN 3
                                              WHEN "DRUN" THEN 4
                                              ELSE 5 END
        )
    ),
    drugtests AS (
        SELECT 
            p.person_id,
            d.DrugClass IN ('MET', 'MTD') as is_meth, 
            d.FinalResult = 'P' AS is_positive,
            SAFE_CAST(SAFE_CAST(d.TestDate AS datetime) AS DATE) as drug_screen_date
        FROM `{project_id}.{raw_dataset}.DrugTestDrugClass_latest` d
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` p
        ON 
            d.OffenderID = p.external_id
            AND p.state_code = 'US_TN'
        WHERE
            p.person_id IS NOT NULL
            AND d.FinalResult IS NOT NULL
    )
    ,
    drugtests_by_date AS (
        SELECT 
            d.person_id, 
            d.is_positive, 
            d.is_meth AND d.is_positive AS is_meth_positive, 
            d.drug_screen_date 
        FROM drugtests d
        LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` s
        ON 
            d.person_id = s.person_id
            AND d.drug_screen_date BETWEEN s.start_date AND COALESCE(s.end_date,'9999-01-01')
        WHERE 
            drug_screen_date >= DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 20 YEAR)
            
            -- Many DrugTestDrugClass results occur during periods of incarceration, rather than supervision.
            -- We exclude these explicitly to ensure we don't tabulate within-facility tests as drug screens for the
            -- purpose of calculating supervision-level positivity rates. Other non-supervision compartment levels are
            -- rare and potentially expected due to reporting lags.
            AND s.compartment_level_1 != 'INCARCERATION'
        QUALIFY 
            ROW_NUMBER() OVER(PARTITION BY person_id, drug_screen_date ORDER BY is_positive DESC, is_meth DESC) = 1
    )
    ,
    matches AS (
      SELECT 
        c.person_id, 
        c.state_code,
        c.sample_type,
        c.result_raw_text,
        c.result_raw_text_primary,
        c.is_inferred,
        c.drug_screen_date as contacts_date, 
        d.drug_screen_date as drugtest_date,
        c.is_meth_positive as contacts_meth_positive,
        d.is_meth_positive as drugtest_meth_positive,
        c.is_positive_result as contacts_positive,
        d.is_positive as drugtest_positive,
        c.result_raw_text_primary as contacts_contactnote,
        DATE_DIFF(c.drug_screen_date, d.drug_screen_date, DAY) AS lag_days,
      FROM contacts_based_drug_screens c
      INNER JOIN drugtests_by_date d
      ON
        c.person_id = d.person_id
        AND DATE_DIFF(c.drug_screen_date, d.drug_screen_date, DAY) 
                      BETWEEN {us_tn_drug_screens_drugtest_contact_min_lag} AND {us_tn_drug_screens_drugtest_contact_max_lag}
                      
      QUALIFY
        ROW_NUMBER() OVER(PARTITION BY person_id, contacts_date ORDER BY ABS(lag_days), lag_days DESC) = 1 AND
        ROW_NUMBER() OVER(PARTITION BY person_id, drugtest_date ORDER BY ABS(lag_days), lag_days DESC) = 1
    )
    ,
    contacts_no_drugtest AS (
      SELECT
        c.person_id,
        c.state_code,
        c.sample_type,
        c.result_raw_text,
        c.result_raw_text_primary,
        c.is_inferred,
        c.drug_screen_date AS contacts_date,
        m.drugtest_date,
        c.is_meth_positive as contacts_meth_positive,
        m.drugtest_meth_positive,
        c.is_positive_result AS contacts_positive,
        m.drugtest_positive,
        c.result_raw_text_primary AS contacts_contactnote,
        m.lag_days
      FROM contacts_based_drug_screens c
      LEFT JOIN matches m
      ON
        c.person_id = m.person_id
        AND c.drug_screen_date = m.contacts_date

      -- Find all rows that weren't included in the join that generated `matches`
      WHERE m.contacts_contactnote IS NULL
    )
    ,
    drugtest_no_contacts AS (
      SELECT
        d.person_id,
        "US_TN" AS state_code,
        CAST(NULL AS STRING) AS sample_type,
        CAST(NULL AS ARRAY<STRING>) AS result_raw_text,
        CAST(NULL AS STRING) AS result_raw_text_primary,
        FALSE AS is_inferred,
        m.contacts_date,
        d.drug_screen_date AS drugtest_date,
        m.contacts_meth_positive,
        d.is_meth_positive AS drugtest_meth_positive,
        m.contacts_positive,
        d.is_positive AS drugtest_positive,
        m.contacts_contactnote,
        m.lag_days
      FROM drugtests_by_date d
      LEFT JOIN matches m
      ON
        d.person_id = m.person_id
        AND d.drug_screen_date = m.drugtest_date

      -- Find all rows that weren't included in the join that generated `matches`
      WHERE m.contacts_contactnote IS NULL
    )
    ,
    combined AS (
      SELECT 
        *,
        "both" AS source_table
        FROM matches
      UNION ALL
      SELECT 
        *, 
        "drugtest" AS source_table
        FROM drugtest_no_contacts
      UNION ALL
      SELECT 
        *, 
        "contacts" AS source_table
        FROM contacts_no_drugtest
    )
    /*
        Logic for the final is_positive_result flag:
        1) If they tested positive in either data source, then TRUE, except if it was a DRUX/DRUM contact note
        2) If they didnt test positive in either data source AND they tested negative (DRUN, DRUM, DRUX) then FALSE
        3) Else, null. Which means if they didnt test positive but also didnt have the relevant contact note, 
        then we aren't sure/aren't counting it as a negative. This is a more conservative definition of a negative test.
        This is true for approx 2% of tests that have is_positive_result_temp = FALSE but no relevant negative contact note
    */
    SELECT * EXCEPT(is_positive_result_temp),
        CASE WHEN is_positive_result_temp THEN TRUE
             WHEN result_raw_text_primary IN ('DRUN','DRUM','DRUX') 
                AND NOT is_positive_result_temp THEN FALSE
             END AS is_positive_result,
    FROM (
        SELECT 
            person_id,
            state_code,
            
            -- Prioritize the Contacts-based test date over the DrugTestDrugClass-based date
            IFNULL(contacts_date, drugtest_date) AS drug_screen_date,
            sample_type,
            (
                (
                    IFNULL(contacts_positive, FALSE) 
                    OR 
                    IFNULL(drugtest_positive, FALSE)
                )
                AND
                (
                    -- Where the Contact Note Type is either DRUX ("DRUG SCREEN NEGATIVE DUE TO VALID PRESCRIPTION"),
                    -- or DRUM ("DRUG SCREEN UNAVAILABLE DUE TO MEDICAL CONDITION"), 
                    -- positive lab results do not indicate a "positive" screen in the sense of a rules violation.
                    -- If this screen comes from DrugTestDrugClass alone, then we don't have information on these exceptions
                    -- and assume a positive lab test indicates a positive result.
                    (source_table = "drugtest") 
                    OR 
                    (result_raw_text_primary NOT IN ("DRUX", "DRUM"))
                )
            ) AS is_positive_result_temp,
            result_raw_text_primary,
            result_raw_text,
            IF (
                (
                    (
                        IFNULL(contacts_meth_positive, FALSE) 
                        OR 
                        IFNULL(drugtest_meth_positive, FALSE)
                    ) 
                    AND
                    (
                        -- Where the Contact Note Type is either DRUX ("DRUG SCREEN NEGATIVE DUE TO VALID PRESCRIPTION"),
                        -- or DRUM ("DRUG SCREEN UNAVAILABLE DUE TO MEDICAL CONDITION"), 
                        -- positive lab results do not indicate a "positive" screen in the sense of a rules violation.
                        -- If this screen comes from DrugTestDrugClass alone, then we don't have information on these exceptions
                        -- and assume a positive lab test indicates a positive result.
                        (source_table = "drugtest") 
                        OR 
                        (result_raw_text_primary NOT IN ("DRUX", "DRUM"))
                    )
                ), 
                "METH", 
                NULL
            ) AS substance_detected,
            is_inferred,
            contacts_date,
            drugtest_date,
            contacts_meth_positive,
            drugtest_meth_positive,
            contacts_positive,
            drugtest_positive,
            contacts_contactnote,
            lag_days,
            source_table,
            CAST(NULL AS STRING) AS med_invalidate_flg
        FROM combined
    )
"""

US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_NAME,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    description=US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_TN_DRUG_SCREENS_PREPROCESSED_QUERY_TEMPLATE,
    should_materialize=False,
    raw_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    sessions_dataset=SESSIONS_DATASET,
    us_tn_drug_screens_drup_ztpd_max_lag=US_TN_DRUG_SCREENS_DRUP_ZTPD_MAX_LAG,
    us_tn_drug_screens_drugtest_contact_max_lag=US_TN_DRUG_SCREENS_DRUGTEST_CONTACT_MAX_LAG,
    us_tn_drug_screens_drugtest_contact_min_lag=US_TN_DRUG_SCREENS_DRUGTEST_CONTACT_MIN_LAG,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER.build_and_print()
