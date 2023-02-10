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
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
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
  SELECT person_id,
         ARRAY_AGG(docket_number IGNORE NULLS) AS docket_numbers,
         ARRAY_AGG(offense IGNORE NULLS) AS offenses,
         ARRAY_AGG(DISTINCT
                CASE WHEN Decode is not null then CONCAT(conviction_county, ' - ', Decode)
                    ELSE conviction_county END
                ) AS conviction_counties,
  FROM (
      SELECT
          s.person_id,
          s.state_code,
          s.start_date,
          sentences.county_code AS conviction_county,
          JSON_EXTRACT_SCALAR(sentences.sentence_metadata, '$.CASE_NUMBER') AS docket_number,
          sentences.description AS offense
      FROM (
        SELECT *
        FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized`
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY start_date DESC) = 1
      ) s,
      UNNEST(sentences_preprocessed_id_array) as sentences_preprocessed_id
      LEFT JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sentences
        USING(person_id, state_code, sentences_preprocessed_id)
      WHERE s.state_code = 'US_TN'
  )
  LEFT JOIN (
            SELECT *
            FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.CodesDescription_latest`
            WHERE CodesTableID = 'TDPD130'
        ) codes
  ON conviction_county = codes.Code
  GROUP BY 1
),
/* TODO(#11164): Because we're not allowing arbitrarily many sentence levels, some later sentences with updated
    expiration dates are getting omitted from sentence spans. This is a quick fix, but once #11164 is updated it can
    be removed */
latest_sentences_all AS (
    SELECT person_id,
           MAX(completion_date) AS max_expiration_date
    FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sentences
    WHERE state_code = 'US_TN'
    GROUP BY 1
),
latest_system_session AS ( # get latest system session date to bring in relevant codes only for this time on supervision
  SELECT person_id,
         start_date AS latest_system_session_start_date
  FROM `{{project_id}}.{{sessions_dataset}}.system_sessions_materialized`
  WHERE state_code = 'US_TN'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY system_session_id DESC) = 1
),
relevant_codes AS (
  SELECT person_id,
         CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS contact_date,
         ContactNoteType AS contact_type,
         Comment AS contact_comment
  FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.ContactNoteType_latest` contact
  LEFT JOIN `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.ContactNoteComment_latest`
    USING (OffenderID, ContactNoteDateTime)
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON contact.OffenderID = pei.external_id
      AND pei.state_code = 'US_TN'
  LEFT JOIN latest_system_session
    USING(person_id)
  WHERE CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) >= latest_system_session_start_date
     AND (
           ContactNoteType LIKE '%PSE%'
           OR ContactNoteType IN ('VRPT','VWAR','COHC','AARP')
           OR ContactNoteType LIKE "%ABS%"
           OR ContactNoteType = 'DRUP'
           OR ContactNoteType LIKE "%FSW%"
           OR ContactNoteType LIKE "%EMP%"
           OR ContactNoteType IN ("SPEC","SPET")
           OR ContactNoteType LIKE 'VRR%'
           OR ContactNoteType IN ("FAC1","FAC2")
           OR ContactNoteType LIKE 'FEE%'
           OR ContactNoteType = 'TEPE'
     )
),
sex_offense_pse_code AS ( #latest PSE code
  SELECT
    person_id,
    STRUCT(
      contact_date,
      contact_type,
      contact_comment
    ) AS latest_pse
  FROM relevant_codes
  WHERE contact_type LIKE '%PSE%'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY contact_date DESC) = 1
),
emp_code AS ( #latest EMP code
  SELECT
    person_id,
    STRUCT(
      contact_date,
      contact_type,
      contact_comment
    ) AS latest_emp
  FROM relevant_codes
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
  FROM relevant_codes
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
  FROM relevant_codes
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
  FROM relevant_codes
  WHERE contact_type LIKE 'FEE%'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY contact_date DESC) = 1
),
tepe_code AS ( # latest TEPE code
    SELECT
    person_id,
    contact_date AS latest_tepe,
  FROM relevant_codes
  WHERE contact_type = 'TEPE'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY contact_date DESC) = 1
),
sidebar_contact_notes_union AS ( #cte to union all contact notes to be displayed in sidebar
    SELECT person_id,
          contact_type AS note_title,
          contact_date AS event_date,
          contact_comment AS note_body,
          "REVOCATION HEARINGS" AS criteria,
    FROM relevant_codes
    WHERE contact_type IN ('VRPT','VWAR','COHC','AARP')
          OR contact_type LIKE "%ABS%"

    UNION ALL

    SELECT  person_id,
            contact_type AS note_title,
            contact_date AS event_date,
            contact_comment AS note_body,
            "ALCOHOL DRUG HISTORY" AS criteria,
    FROM relevant_codes
    WHERE contact_type = 'DRUP' OR contact_type LIKE "%FSW%"

    UNION ALL

    SELECT  person_id,
            contact_type AS note_title,
            contact_date AS event_date,
            contact_comment AS note_body,
            "INTAKE NOTES" AS criteria,
    FROM (
      SELECT *
      FROM relevant_codes
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
            )
        ) AS case_notes
    FROM sidebar_contact_notes_union
    GROUP BY 1
),
sex_offenses AS (
  SELECT person_id,
          ARRAY_AGG(offenses2) AS sex_offenses
  FROM latest_sentences,
  UNNEST(offenses) as offenses2
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
  ON offenses2 = statute.OffenseDescription
  WHERE SexOffenderFlag = 'Y' OR
      (
        SexOffenderFlag IS NULL
        AND (
          offenses2 LIKE '%SEX%' OR offenses2 LIKE '%RAPE%'
        )
      )
  GROUP BY 1
),
latest_people AS (
    SELECT DISTINCT OffenderID AS external_id
    FROM `{{project_id}}.{{us_tn_raw_data_dataset}}.OffenderMovement`
    WHERE update_datetime = (
        SELECT MAX(update_datetime)
        FROM `{{project_id}}.{{us_tn_raw_data_dataset}}.OffenderMovement`
    )
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
         emp.latest_emp AS form_information_latest_emp,
         spe.latest_spe AS form_information_latest_spe,
         vrr.latest_vrr AS form_information_latest_vrr,
         fee.latest_fee AS form_information_latest_fee,
         latest_sentences.offenses AS form_information_offenses,
         latest_sentences.docket_numbers AS form_information_docket_numbers,
         latest_sentences.conviction_counties AS form_information_conviction_counties,
         stg.STGID AS form_information_gang_affiliation_id,
         latest_tepe,
  FROM tes_cte tes
  LEFT JOIN sidebar_contact_notes_array arr
    USING(person_id)
  LEFT JOIN sex_offense_pse_code pse
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
  LEFT JOIN latest_sentences_all
    USING(person_id)
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    ON tes.person_id = pei.person_id
    AND pei.state_code = 'US_TN'
  LEFT JOIN tepe_code
    ON tes.person_id = tepe_code.person_id
    AND latest_tepe >= DATE_SUB(tes.expiration_date, INTERVAL 30 day)
  LEFT JOIN `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.STGOffender_latest` stg
    ON pei.external_id = stg.OffenderID
  INNER JOIN latest_people
    ON pei.external_id = latest_people.external_id
    /* TODO(#18327) - Excluding anyone from being surfaced who has a recent TEPE. Add these folks back in and surface
    recent TEPE note in client profile when UX changes can handle this behavior */
  WHERE latest_tepe IS NULL
    /* TODO(#11164): Because we're not allowing arbitrarily many sentence levels, some later sentences with updated
    expiration dates are getting omitted from sentence spans. This is a quick fix, but once #11164 is updated it can
    be removed */
    AND latest_sentences_all.max_expiration_date <= DATE_ADD(CURRENT_DATE('US/Pacific'), INTERVAL 1 DAY)

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
    us_tn_raw_data_dataset=raw_tables_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.build_and_print()
