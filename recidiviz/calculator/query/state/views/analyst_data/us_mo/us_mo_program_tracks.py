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
"""Pulls together information on screeners, sentencing dates, and other information on currently incarcerated
individuals in MO to assign them to Program Tracks"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.utils.us_mo_query_fragments import (
    classes_cte,
    current_bed_stay_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_PROGRAM_TRACKS_VIEW_NAME = "us_mo_program_tracks"

US_MO_PROGRAM_TRACKS_VIEW_DESCRIPTION = """Pulls together information on screeners, sentencing dates, and
other information on currently incarcerated individuals in MO to assign them to Program Tracks"""

US_MO_PROGRAM_TRACKS_QUERY_TEMPLATE = f"""
WITH {classes_cte()}
,
mosop_details AS (
  SELECT
    *
  FROM classes 
  WHERE CLASS_TITLE LIKE "%MOSOP%"
),
mosop_completion_flags AS (
  /* MOSOP completion is identified based on the individual's most recent cycle involving
  MOSOP classes. "Completion" is only met when every MOSOP class in that cycle has a 
  EXIT_TYPE_CD of "SFL", which encompasses the three exit types that count as completion:
    - Partial Completion (Required Phase Completed)
    - Completion
    - Full Completion (Required and Re-Entry Phase Completed)
  This approach was confirmed to be correcct during MDOC Office Hours (3/24/23).
  */
  SELECT 
    DOC_ID,
    (
      LOGICAL_OR(p1_completed_flag) AND 
      LOGICAL_OR(p2_completed_flag) 
      AND NOT LOGICAL_OR(is_referral)
    ) AS all_complete,
    LOGICAL_OR(uns_flag) AS has_uns,  
    LOGICAL_OR(nof_flag) AS has_nof,
    LOGICAL_OR(any_exit_flag) AS has_any_exit,
    MAX(failure_date) AS most_recent_failure,
    SUM(CASE WHEN uns_flag THEN 1 ELSE 0 END) AS uns_ct,
    LOGICAL_OR(p1_completed_flag) AS completed_p1
  FROM (
    SELECT 
      mosop_most_recent_cyc.DOC_ID,
      CASE WHEN mosop_general.EXIT_TYPE_CD = 'UNS' AND ACTUAL_EXIT_DT != '7799-12-31' THEN SAFE_CAST(ACTUAL_EXIT_DT AS DATE FORMAT "YYYY-MM-DD") ELSE NULL END AS failure_date, 
      CASE WHEN ACTUAL_EXIT_DT!='7799-12-31' THEN TRUE ELSE FALSE END AS any_exit_flag,
      CASE WHEN mosop_general.EXIT_TYPE_CD = 'UNS' THEN TRUE ELSE FALSE END AS uns_flag,
      CASE WHEN mosop_general.EXIT_TYPE_CD = 'NOF' THEN TRUE ELSE FALSE END AS nof_flag,
      CASE WHEN mosop_general.EXIT_TYPE_CD = 'SFL' THEN TRUE ELSE FALSE END AS sfl_flag,
      CASE WHEN mosop_general.EXIT_TYPE_CD = 'SFL' AND CLASS_TITLE = 'MOSOP PHASE I' THEN TRUE ELSE FALSE END AS p1_completed_flag,
      CASE WHEN CLASS_TITLE = 'MOSOP PHASE II' THEN TRUE ELSE FALSE END AS p2_enrollment,
      CASE WHEN mosop_general.EXIT_TYPE_CD = 'SFL' AND CLASS_TITLE = 'MOSOP PHASE II' THEN TRUE ELSE FALSE END AS p2_completed_flag,
      is_referral
    FROM (
      SELECT 
        DOC_ID,
        MAX(CAST(CYCLE_NO AS DATE FORMAT 'YYYYMMDD')) AS most_recent_cyc 
      FROM mosop_details 
      GROUP BY DOC_ID
    ) mosop_most_recent_cyc
    LEFT JOIN mosop_details mosop_general 
    ON 
      mosop_most_recent_cyc.DOC_ID = mosop_general.DOC_ID AND 
      CAST(CYCLE_NO AS DATE FORMAT 'YYYYMMDD') = most_recent_cyc
  )
  GROUP BY DOC_ID
),
mosop_completed_or_ongoing AS (
  SELECT * 
  FROM (
    SELECT 
      DOC_ID,
      LOGICAL_OR(all_complete) AS completed_flag,
      LOGICAL_OR(has_uns) AS has_uns,  
      LOGICAL_OR(has_nof) AS has_nof,
      LOGICAL_OR(has_any_exit) AS has_any_exit,
      LOGICAL_OR(CASE WHEN ACTUAL_EXIT_DT = '7799-12-31' AND ACTUAL_START_DT != '7799-12-31' THEN TRUE ELSE FALSE END) AS ongoing_flag,
      ANY_VALUE(most_recent_failure) AS most_recent_failure,
      ANY_VALUE(uns_ct) AS uns_ct,
      LOGICAL_OR(completed_p1) AS completed_p1
    FROM (
      SELECT * 
      FROM mosop_details 
      LEFT JOIN mosop_completion_flags 
      USING (DOC_ID)
    ) 
  GROUP BY DOC_ID
  )
  INNER JOIN 
  `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
  ON 
    DOC_ID = pei.external_id
    AND pei.state_code = 'US_MO'
),
latest_assessment AS (
  /*
  This CTE does the re-scoring of various assessment scores into the "algorithm" provided by MO,
  and then selects the latest assessment per assessment type
  */
  SELECT * EXCEPT(external_id),
        CASE WHEN assessment_type = 'ORAS' THEN (
          CASE WHEN assessment_level = 'LOW' THEN 0
               -- This captures low/medium, medium, high, very high, and medium high
               -- It's possibly low/medium should be 0, but its only ~50 of currently incarcerated folks so leaving as is
               ELSE 6
               END
          )
          WHEN assessment_type = 'ICASA' THEN (
            CASE WHEN assessment_score BETWEEN 1 AND 2 THEN 0
                 WHEN assessment_score BETWEEN 3 AND 5 THEN 2
                 END
          )
          WHEN assessment_type = 'TCUD' THEN (
            CASE WHEN assessment_score BETWEEN 0 AND 3 THEN 0
                 WHEN assessment_score BETWEEN 4 AND 11 THEN 2
                 END
          )
          WHEN assessment_type = 'CMHS' THEN (
            CASE WHEN assessment_type_raw_text = '940-MEN' THEN (
                CASE WHEN assessment_score BETWEEN 0 AND 5 THEN 0
                     WHEN assessment_score BETWEEN 6 AND 12 THEN 3
                     END
                )
                WHEN assessment_type_raw_text = '950-WOMEN' THEN  (
                CASE WHEN assessment_score BETWEEN 0 AND 4 THEN 0
                     WHEN assessment_score BETWEEN 5 AND 8 THEN 3
                     END
                )
                 END
          )
          WHEN assessment_type = 'mental_health' THEN (
            CASE WHEN assessment_score BETWEEN 1 AND 2 THEN 0
                 WHEN assessment_score BETWEEN 3 AND 5 THEN 3
                 END
          )
          WHEN assessment_type = 'SACA' THEN (
            CASE WHEN assessment_score BETWEEN 1 AND 2 THEN 0
                 WHEN assessment_score BETWEEN 3 AND 5 THEN 2
            END
          )
          ELSE assessment_score
          END AS asmt_score_track
  FROM `{{project_id}}.{{analyst_dataset}}.us_mo_screeners_preprocessed_materialized`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, assessment_type
                            ORDER BY assessment_date DESC) = 1
),
-- TODO(#19223): Use ingested data when available
flag_120 AS (
    /*
    This CTE flags people who require court mandated treatment using the 120-day treatment flag - if it is 
    (still) Y and if the associated sentence is marked incomplete ("N")
    */
    SELECT external_id,
       person_id,
       LOGICAL_OR(COALESCE(BT_OTD,'N') = 'Y' AND BS_SCF = 'N') AS flag_120_required,
       MAX(SAFE.PARSE_DATE('%Y%m%d',NULLIF(BT_OH,'0'))) AS max_120_date
  FROM `{{project_id}}.{{us_mo_raw_data_up_to_date_dataset}}.LBAKRDTA_TAK023_latest`
  LEFT JOIN `{{project_id}}.{{us_mo_raw_data_up_to_date_dataset}}.LBAKRDTA_TAK022_latest`
    ON BT_DOC = BS_DOC
    AND BT_CYC = BS_CYC
    AND BT_SEO = BS_SEO
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id`
    ON BT_DOC = external_id
    AND state_code = 'US_MO'
  GROUP BY 1,2
),
board_mandated AS (
  SELECT person_id, 
         ARRAY_AGG(CI_SPC) AS special_conditions,
  FROM `{{project_id}}.{{analyst_dataset}}.us_mo_sentencing_dates_preprocessed_materialized`
  LEFT JOIN `{{project_id}}.{{us_mo_raw_data_up_to_date_dataset}}.LBAKRDTA_TAK046_latest`
      ON doc_id = CI_DOC
      AND cycle_num = CI_CYC
      AND seq_num = CI_BSN
  WHERE CI_SPC IN ('ITC','LTD','TRT','BAS') AND board_determined_release_date IS NOT NULL
  GROUP BY 1
),
latest_sentencing_dates AS (
  -- Previously unique at the person-cycle level, this takes max across a given person
  SELECT * EXCEPT(cycle_num) FROM (
  SELECT person_id,
         cycle_num,
         MAX(minimum_eligibility_date) AS minimum_eligibility_date,
         MAX(minimum_mandatory_release_date) AS minimum_mandatory_release_date,
         MAX(board_determined_release_date) AS board_determined_release_date,
         MAX(conditional_release) AS conditional_release,
         MAX(max_discharge) AS max_discharge,
         MAX(rch_date) AS rch_date,
         -- If the person has a life sentence in their current cycle 
         -- (CV_AP = '99999999'), then we flag that here.
         LOGICAL_OR(life_flag) AS life_flag
  FROM `{{project_id}}.{{analyst_dataset}}.us_mo_sentencing_dates_preprocessed_materialized`
  GROUP BY 1,2
  ) QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY cycle_num DESC) = 1
),
current_inc_pop AS (
  SELECT cs.*, pei.external_id
  FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` cs
  LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    USING(person_id, state_code)
  WHERE state_code = 'US_MO'
    AND compartment_level_1 = 'INCARCERATION'
    AND end_date IS NULL
),
pop_and_asmts AS (
  SELECT a.*, 
          b.assessment_type, 
          b.asmt_score_track
  FROM current_inc_pop a
  LEFT JOIN latest_assessment b
    ON a.person_id = b.person_id
    -- Optionally do this, for now not doing it. Reduces assessments for another ~1.5k people
    -- AND b.assessment_date >= a.start_date
),
pivoted_asmts AS (
  /* The previous cte is "long" on assessments - this cte pivots to have one column per relevant assessment */
  SELECT person_id,
       external_id,
       gender,
       compartment_level_2,
       facility,
       start_date,
       ORAS,
       COALESCE(ICASA,TCUD,SACA) AS substance_use_screener,
       COALESCE(mental_health, CMHS) AS mh_screener,
       education,
       ORAS + COALESCE(ICASA,TCUD,SACA) + COALESCE(mental_health, CMHS) AS program_track_score,
  FROM
    (SELECT person_id, 
            external_id,
            gender,
            start_date,
            compartment_level_2,
            compartment_location_end AS facility,
            assessment_type,
            asmt_score_track,
      FROM pop_and_asmts
      )
    PIVOT(ANY_VALUE(asmt_score_track) 
          FOR assessment_type 
            IN ('ORAS','ICASA','TCUD','CMHS','mental_health','education','SACA')
          )
),
{current_bed_stay_cte()},
unprioritized_program_tracks AS (
SELECT p.*,
       housing.* EXCEPT(person_id, state_code, facility),
       CONCAT(JSON_VALUE(full_name, '$.given_names'), ' ', JSON_VALUE(full_name, '$.surname')) AS person_name,
       IF(
            latest_sentencing_dates.minimum_eligibility_date >= start_date,
            latest_sentencing_dates.minimum_eligibility_date,
            NULL
        ) AS minimum_eligibility_date,
       IF(
            latest_sentencing_dates.minimum_mandatory_release_date >= start_date,
            latest_sentencing_dates.minimum_mandatory_release_date,
            NULL
        ) AS minimum_mandatory_release_date,
       IF(
            latest_sentencing_dates.board_determined_release_date >= start_date, 
            latest_sentencing_dates.board_determined_release_date,
            NULL
        ) AS board_determined_release_date,
        IF(
            latest_sentencing_dates.conditional_release >= start_date,
            latest_sentencing_dates.conditional_release,
            NULL
        ) AS conditional_release,
        IF(
            latest_sentencing_dates.max_discharge >= start_date,
            latest_sentencing_dates.max_discharge,
            NULL
        ) AS max_discharge,
        IF(
            latest_sentencing_dates.rch_date >= start_date,
            latest_sentencing_dates.rch_date,
            NULL
        ) AS rch_date,
        latest_sentencing_dates.life_flag,
       mosop.mosop_indicator,
       flag_120_required AS court_mandated_treatment,
       special_conditions AS board_inst_special_conditions,
      -- Using program track score to assign groups and tracks per "algorithm" given by MO
      CASE program_track_score
        WHEN 0 THEN 'Group1'
        WHEN 3 THEN 'Group2'
        WHEN 2 THEN 'Group3'
        WHEN 5 THEN 'Group4'
        WHEN 6 THEN 'Group5'
        WHEN 9 THEN 'Group6'
        WHEN 8 THEN 'Group7'
        WHEN 11 THEN 'Group8'
        ELSE 'UNKNOWN' END AS program_group_num,
      CASE program_track_score
        WHEN 0 THEN 'Track1'
        WHEN 3 THEN 'Track1'
        WHEN 2 THEN 'Track2'
        WHEN 5 THEN 'Track3'
        WHEN 6 THEN 'Track4'
        WHEN 9 THEN 'Track4'
        WHEN 8 THEN 'Track5'
        WHEN 11 THEN 'Track6'
        ELSE 'UNKNOWN' END AS program_track_num,
FROM
    pivoted_asmts p
LEFT JOIN
    latest_sentencing_dates
USING
    (person_id)
LEFT JOIN
    flag_120
ON
    p.person_id = flag_120.person_id
-- Brings in MOSOP (Sex Offender Required Treatment) info from raw data. Only about 12 of the currently incarcerated
-- population don't show up in this table
-- TODO(#19223): Use ingested data when available
LEFT JOIN (
  SELECT 
    person_id,
    LOGICAL_OR(COALESCE(mosop_indicator,FALSE) AND status = 'SERVING') AS mosop_indicator
  FROM (
    SELECT pei.person_id,
      /* R is dept required, not required by state and F is sentences before MOSOP completion was required
          O is override such that it is no longer required
          This table in general has a ton of null values for DQ_SOP (70%) which is *likely* because of records
          that pre-dated the MOSOP statute. This is suggested by the fact that for the current incarcerated population,
          the mosop_indicator is only null for approximately 3% of the people. Since it's not clear what the default
          assumption should be, we're leaving it as null
      */
      CASE WHEN DQ_SOP IN ('Y','R','F') THEN TRUE
            WHEN DQ_SOP IN ('N','O') THEN FALSE
            END AS mosop_indicator,
      DQ_DOC,DQ_CYC,
      sp.external_id,
      sp.status
    FROM 
      `{{project_id}}.{{us_mo_raw_data_up_to_date_dataset}}.LBAKRDTA_TAK040_latest` t
    INNER JOIN
      `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON
          t.DQ_DOC = pei.external_id
      AND
          pei.state_code = 'US_MO'
    LEFT JOIN (
      SELECT 
        external_id,
        REGEXP_EXTRACT(external_id,r'^[[:digit:]]+') AS DOC_ID,
        REGEXP_EXTRACT(external_id,r'-([[:digit:]]+)-') AS CYCLE_NO,
        status
      FROM 
        `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` 
      WHERE state_code = 'US_MO'
    ) sp
    ON DQ_DOC = sp.DOC_ID AND DQ_CYC = sp.CYCLE_NO
  ) GROUP BY person_id
) mosop
  ON p.person_id = mosop.person_id
LEFT JOIN current_bed_stay housing
  ON
    p.person_id = housing.person_id
LEFT JOIN
    `{{project_id}}.{{normalized_state_dataset}}.state_person` sp
  ON
    p.person_id = sp.person_id
LEFT JOIN
    board_mandated 
  ON
    p.person_id = board_mandated.person_id
/* There are about 30 people in each of these, and it seems like a bug / not an actual facility. AIFED means someone is 
"serving a Missouri sentence(s) concurrently with an out-of-state or federal facility.
Both Missouri and the confining agency have jurisdiction over the offender." based on https://doc.mo.gov/glossary */
WHERE p.facility NOT IN ('AIFED', 'EXTERNAL_UNKNOWN')
),
pre_rch_override AS (
SELECT 
  pt.*,
  COALESCE(
    pt.board_determined_release_date,
    GREATEST(IFNULL(pt.minimum_eligibility_date,pt.minimum_mandatory_release_date), IFNULL(pt.minimum_mandatory_release_date,pt.minimum_eligibility_date)),
    pt.conditional_release,
    pt.max_discharge
  ) AS prioritized_date,
  COALESCE(mosop.ongoing_flag, FALSE) AS ongoing_flag,
  COALESCE(mosop.completed_flag, FALSE) AS completed_flag,
  NOT COALESCE(mosop.has_any_exit, FALSE) AS has_no_exits,
  COALESCE(mosop.has_uns, FALSE) AS has_uns,
  COALESCE(mosop.has_nof, FALSE) AS has_nof,
  mosop.most_recent_failure,
  COALESCE(mosop.uns_ct, 0) AS uns_ct,
  COALESCE(mosop.completed_p1, FALSE) AS completed_p1,
  CURRENT_DATE('-05:00') >= GREATEST(
      IFNULL(pt.minimum_eligibility_date,pt.minimum_mandatory_release_date), 
      IFNULL(pt.minimum_mandatory_release_date,pt.minimum_eligibility_date)
  ) AS past_me_mpt,
  NULLIF(
    LEAST(
      COALESCE(pt.conditional_release,DATE(9999,1,1)),
      COALESCE(pt.max_discharge,DATE(9999,1,1))
    ),
    DATE(9999,1,1)
  ) AS cr_md
FROM unprioritized_program_tracks pt
LEFT JOIN mosop_completed_or_ongoing mosop
USING (person_id)
)

SELECT 
  * EXCEPT(prioritized_date),

  /*
  The CASE below adjusts the prioritized date using these steps.
  - Follow the standard prioritization ranking from above (BDRD, then ME/MPT, then CR, 
  then MAX). 
  - Then, remove the prioritized date entirely for people serving life sentences.
  - Then, for people past their ME/MPT dates who have RCH dates, override the prioritized 
    date with either:
    (a) the RCH date, if it's 10 years (or less) before the CR/MAX date
    (b) The CR/MAX date, if it's more than 10 years past the RCH date
  */

  CASE 
    WHEN life_flag THEN NULL
    WHEN past_me_mpt AND 
      cr_md IS NOT NULL AND 
      rch_date IS NOT NULL AND 
      DATE_DIFF(cr_md,rch_date,MONTH) <= 120 
    THEN rch_date
    WHEN past_me_mpt AND 
      cr_md IS NOT NULL AND 
      rch_date IS NOT NULL AND 
      DATE_DIFF(cr_md,rch_date,YEAR) > 120 
    THEN cr_md
    ELSE prioritized_date
    -- TODO(#29245): This currently will fall back to the standard prioritized date if 
    -- someone's CR/MAX dates are null, regardless of their RCH date. We need to check if 
    -- this is the desired behavior.
  END AS prioritized_date
FROM pre_rch_override
"""

US_MO_PROGRAM_TRACKS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    view_id=US_MO_PROGRAM_TRACKS_VIEW_NAME,
    view_query_template=US_MO_PROGRAM_TRACKS_QUERY_TEMPLATE,
    description=US_MO_PROGRAM_TRACKS_VIEW_DESCRIPTION,
    should_materialize=True,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    us_mo_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MO,
        instance=DirectIngestInstance.PRIMARY,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_PROGRAM_TRACKS_VIEW_BUILDER.build_and_print()
