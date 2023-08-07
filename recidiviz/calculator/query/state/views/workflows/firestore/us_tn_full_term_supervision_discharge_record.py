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
"""Query for relevant metadata needed to support supervision discharge opportunity in Tennessee
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.views.workflows.us_tn.shared_ctes import (
    us_tn_get_current_offense_information,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_NAME = (
    "us_tn_full_term_supervision_discharge_record"
)

US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_DESCRIPTION = """
    Query for relevant metadata needed to support supervision discharge opportunity in Tennessee
    """
US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_QUERY_TEMPLATE = f"""
WITH latest_sentences AS (
  {us_tn_get_current_offense_information()}
),
latest_system_session AS ( # get latest system session date to bring in relevant codes only for this time on supervision
  SELECT person_id,
         start_date AS latest_system_session_start_date
  FROM `{{project_id}}.{{sessions_dataset}}.system_sessions_materialized`
  WHERE state_code = 'US_TN'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY system_session_id DESC) = 1
),
relevant_codes AS (
  /*
    ContactNoteComment is unique on OffenderID and ContactNoteDateTime while ContactNoteType is unique on OffenderID,
    ContactNoteDateTime, and ContactType. Which means POs enter one comment but a collection of ContactTypes at any 
    given time, and there can be multiple per day. 
    
    The trade-off between joining on Contact Date vs. ContactDateTime is the following:
    - For drug tests, we sometimes see a DRUL (drug test sent to lab) entered on Date X and Time Y, with the matching 
    comment describing what the person tested positive for, awaiting confirmation from the lab. Later on Date X Time Z, 
    there is a DRUP contact to confirm the positive test but not matching comment. For this reason, joining on Contact
    Date provides more complete information
    - A PO might enter completely unrelated contact notes on Date X at Time Y and Time Z - in this case, joining on date
    might just add some noise since we only care about the comment from Time Y but will get both
    
    Below, we join on contact date and aggregate all relevant contact types and comments for a given date which may be
    noisier than ideal but also hopefully more complete  
  */ 
  SELECT person_id,
         contact_date,
         ARRAY_AGG(DISTINCT contact.ContactNoteType IGNORE NULLS) AS contact_type,
         ARRAY_AGG(DISTINCT comment.Comment IGNORE NULLS) AS contact_comment
  FROM
    (
        SELECT * EXCEPT(ContactNoteDateTime),
                CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS contact_date,
        FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.ContactNoteType_latest`
    ) contact
  LEFT JOIN (
        SELECT * EXCEPT(ContactNoteDateTime),
                CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS contact_date,
        FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.ContactNoteComment_latest`
    ) comment
    USING (OffenderID, contact_date)
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON contact.OffenderID = pei.external_id
      AND pei.state_code = 'US_TN'
  LEFT JOIN latest_system_session
    USING(person_id)
  WHERE contact_date >= latest_system_session_start_date
     AND (
           ContactNoteType LIKE '%PSE%'
           OR ContactNoteType IN ('VRPT','VWAR','COHC','ARRP')
           OR ContactNoteType LIKE "%ABS%"
           OR ContactNoteType IN ('DRUN','DRUP','DRUM','DRUX')
           OR ContactNoteType LIKE "%FSW%"
           OR ContactNoteType LIKE "%EMP%"
           OR ContactNoteType IN ("SPEC","SPET")
           OR ContactNoteType LIKE 'VRR%'
           OR ContactNoteType IN ("FAC1","FAC2")
           OR ContactNoteType LIKE 'FEE%'
           OR ContactNoteType = 'TEPE'
           OR ContactNoteType IN ('NCAC','NCAF')
     )
  GROUP BY 1,2
),
sex_offense_pse_code AS ( #latest PSE code
  SELECT
    person_id,
    STRUCT(
      contact_date,
      contact_type,
      contact_comment
    ) AS latest_pse
  FROM relevant_codes,
  UNNEST(contact_type) AS contact_type
  WHERE contact_type LIKE '%PSE%'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY contact_date DESC) = 1
),
new_offense_codes AS ( #latest PSE code
  SELECT
    person_id,
    -- Create array because we want to keep all, not just latest
    ARRAY_AGG(
        STRUCT(
          contact_date,
          contact_type,
          contact_comment
        )
        ) AS new_offenses
  FROM relevant_codes,
  UNNEST(contact_type) AS contact_type
  WHERE contact_type IN ('NCAC','NCAF')
  GROUP BY 1
),
emp_code AS ( #latest EMP code
  SELECT
    person_id,
    STRUCT(
      contact_date,
      contact_type,
      contact_comment
    ) AS latest_emp
  FROM relevant_codes,
  UNNEST(contact_type) AS contact_type
  WHERE contact_type LIKE '%EMP%'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY contact_date DESC) = 1
),
spe_code AS ( #latest SPE code
  SELECT
    person_id,
    STRUCT(
      contact_date,
      contact_type,
      contact_comment
    ) AS latest_spe
  FROM relevant_codes,
  UNNEST(contact_type) AS contact_type
  WHERE contact_type IN ("SPEC","SPET")
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY contact_date DESC) = 1
),
vrr_code AS ( #latest SPE code
  SELECT
    person_id,
    STRUCT(
      contact_date,
      contact_type
    ) AS latest_vrr
  FROM relevant_codes,
  UNNEST(contact_type) AS contact_type
  WHERE contact_type LIKE 'VRR%'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY contact_date DESC) = 1
),
fee_code AS ( #latest SPE code
  SELECT
    person_id,
    STRUCT(
      contact_date,
      contact_type,
      contact_comment
    ) AS latest_fee
  FROM relevant_codes,
  UNNEST(contact_type) AS contact_type
  WHERE contact_type LIKE 'FEE%'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY contact_date DESC) = 1
),
tepe_code AS ( # latest TEPE code
    SELECT
    person_id,
    contact_date AS latest_tepe,
  FROM relevant_codes,
  UNNEST(contact_type) AS contact_type
  WHERE contact_type = 'TEPE'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY contact_date DESC) = 1
),
alc_history_codes AS (
    SELECT  person_id,
            -- Create array because we want to keep all, not just latest
            ARRAY_AGG(
                STRUCT(
                  contact_date,
                  contact_type,
                  contact_comment
                )
            ) AS alc_history
    FROM relevant_codes,
    UNNEST(contact_type) AS contact_type
    WHERE contact_type IN ('DRUP') OR contact_type LIKE "%FSW%"
    GROUP BY 1
),
sidebar_contact_notes_union AS ( #cte to union all contact notes to be displayed in sidebar
    SELECT person_id,
          contact_type AS note_title,
          contact_date AS event_date,
          contact_comment AS note_body,
          "REVOCATION HEARINGS" AS criteria,
    FROM relevant_codes,
    UNNEST(contact_type) AS contact_type
    WHERE contact_type IN ('VRPT','VWAR','COHC','AARP')
          OR contact_type LIKE "%ABS%"

    UNION ALL

    SELECT  person_id,
            contact_type AS note_title,
            contact_date AS event_date,
            contact_comment AS note_body,
            "ALCOHOL DRUG HISTORY" AS criteria,
    FROM relevant_codes,
    UNNEST(contact_type) AS contact_type
    WHERE contact_type IN ('DRUP') OR contact_type LIKE "%FSW%"
    
    UNION ALL

    SELECT  person_id,
            contact_type AS note_title,
            contact_date AS event_date,
            contact_comment AS note_body,
            "ALCOHOL DRUG HISTORY - NEGATIVE SCREENS" AS criteria,
    FROM relevant_codes,
    UNNEST(contact_type) AS contact_type
    WHERE contact_type IN ('DRUN','DRUM','DRUX')

    UNION ALL

    SELECT  person_id,
            contact_type AS note_title,
            contact_date AS event_date,
            contact_comment AS note_body,
            "INTAKE NOTES" AS criteria,
    FROM (
      SELECT person_id, contact_type, contact_date, contact_comment
      FROM relevant_codes,
      UNNEST(contact_type) AS contact_type
      WHERE contact_type IN ("FAC1","FAC2")
      -- Keep latest FAC1 and latest FAC2
      QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, contact_type ORDER BY contact_date DESC) = 1
    )
),
sidebar_contact_notes_array AS (
    SELECT
        person_id,
        TO_JSON(
            ARRAY_AGG(
                IF(note_title IS NOT NULL,
                    STRUCT(note_title, note_body, event_date, criteria),
                    NULL
                )
            IGNORE NULLS
            ORDER BY criteria, event_date
            )
        ) AS case_notes
    FROM sidebar_contact_notes_union
    GROUP BY 1
),
sex_offenses AS (
  SELECT person_id,
          ARRAY_AGG(offenses) AS sex_offenses
  FROM latest_sentences,
  UNNEST(current_offenses) as offenses
  LEFT JOIN (
    SELECT DISTINCT
          OffenseDescription,
          FIRST_VALUE(AssaultiveOffenseFlag)
              OVER(PARTITION BY OffenseDescription
                   ORDER BY CASE WHEN AssaultiveOffenseFlag = 'Y' THEN 0 ELSE 1 END) AS AssaultiveOffenseFlag,
          FIRST_VALUE(SexOffenderFlag)
              OVER(PARTITION BY OffenseDescription
                   ORDER BY CASE WHEN SexOffenderFlag = 'Y' THEN 0 ELSE 1 END) AS SexOffenderFlag
            FROM  `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.OffenderStatute_latest`
  ) statute
  ON offenses = statute.OffenseDescription
  WHERE SexOffenderFlag = 'Y' OR
      (
        SexOffenderFlag IS NULL
        AND (
          offenses LIKE '%SEX%' OR offenses LIKE '%RAPE%'
        )
      )
  GROUP BY 1
),
tes_cte AS (
    SELECT *,
      CAST(JSON_EXTRACT_SCALAR(single_reason.reason.eligible_date) AS DATE) AS expiration_date,
    FROM `{{project_id}}.{{task_eligibility_dataset}}.complete_full_term_discharge_from_supervision_materialized` tes,
    UNNEST(JSON_QUERY_ARRAY(reasons)) AS single_reason
    WHERE tes.state_code = 'US_TN'
        AND CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND {nonnull_end_date_exclusive_clause('tes.end_date')}
        AND tes.is_eligible
        AND STRING(single_reason.criteria_name) = 'SUPERVISION_PAST_FULL_TERM_COMPLETION_DATE_OR_UPCOMING_1_DAY'
)
  SELECT
         tes.state_code,
         pei.external_id,
         tes.reasons,
         arr.case_notes,
         pse.latest_pse AS form_information_latest_pse,
         sex_offenses.sex_offenses AS form_information_sex_offenses,
         new_off.new_offenses AS form_information_new_offenses,
         alc.alc_history AS form_information_alcohol_history,
         emp.latest_emp AS form_information_latest_emp,
         spe.latest_spe AS form_information_latest_spe,
         vrr.latest_vrr AS form_information_latest_vrr,
         fee.latest_fee AS form_information_latest_fee,
         latest_sentences.current_offenses AS form_information_offenses,
         latest_sentences.docket_numbers AS form_information_docket_numbers,
         latest_sentences.conviction_counties AS form_information_conviction_counties,
         stg.STGID AS form_information_gang_affiliation_id,
         latest_tepe,
  FROM tes_cte tes
  LEFT JOIN sidebar_contact_notes_array arr
    USING(person_id)
  LEFT JOIN sex_offense_pse_code pse
    USING(person_id)
  LEFT JOIN new_offense_codes new_off
    USING(person_id)
  LEFT JOIN alc_history_codes alc
    USING(person_id)
  LEFT JOIN sex_offenses
    USING(person_id)
  LEFT JOIN emp_code emp
    USING(person_id)
  LEFT JOIN spe_code spe
    USING(person_id)
  LEFT JOIN vrr_code vrr
    USING(person_id)
  LEFT JOIN fee_code fee
    USING(person_id)
  LEFT JOIN latest_sentences
    USING(person_id)
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    ON tes.person_id = pei.person_id
    AND pei.state_code = 'US_TN'
  LEFT JOIN tepe_code
    ON tes.person_id = tepe_code.person_id
    AND latest_tepe >= DATE_SUB(tes.expiration_date, INTERVAL 30 day)
  LEFT JOIN `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.STGOffender_latest` stg
    ON pei.external_id = stg.OffenderID
    /* TODO(#18327) - Excluding anyone from being surfaced who has a recent TEPE. Add these folks back in and surface
    recent TEPE note in client profile when UX changes can handle this behavior */
  WHERE latest_tepe IS NULL
"""

US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_NAME,
    view_query_template=US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_QUERY_TEMPLATE,
    description=US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_TN
    ),
    should_materialize=True,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.build_and_print()
