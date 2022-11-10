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
"""Query for information needed to autopopulate the transfer chrono as well as
for relevant case notes needed to determine eligibility for transfer to limited unit
supervision in Idaho
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.pipeline.supplemental.dataset_config import (
    SUPPLEMENTAL_DATA_DATASET,
)
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_NAME = (
    "us_id_complete_transfer_to_limited_supervision_form_record"
)

US_ID_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_DESCRIPTION = """
    View of the limited supervision unit record for transfer chrono completion and 
    for determining eligibility for transfer to limited unit supervision in Idaho
    for individuals that may be eligible 
    """
US_ID_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_QUERY_TEMPLATE = f"""
 WITH sentence_charge_description AS (
      SELECT
        sent.state_code,
        sent.person_id,
        sent.date_imposed,
        sent.projected_completion_date_max,
        sent.description, 
      FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized`,
        UNNEST (sentences_preprocessed_id_array) sentences_preprocessed_id
      INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
        USING (state_code, person_id, sentences_preprocessed_id)
      WHERE state_code = "US_ID"
      AND CURRENT_DATE BETWEEN start_date AND COALESCE(end_date, "9999-12-31")
    ),
    person_info AS (
      SELECT
        spi.person_id,
        spi.state_code,
        spi.current_address,
        spi.current_email_address,
        spi.current_phone_number
      FROM `{{project_id}}.{{normalized_state_dataset}}.state_person` spi
      WHERE state_code = 'US_ID'
    ),
    latest_assessment_score AS (
      SELECT
        person_id,
        state_code,
        assessment_date,
        assessment_score,
      FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` score
      WHERE assessment_type = 'LSIR'
        AND score_end_date IS NULL
        AND state_code = 'US_ID'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY assessment_date DESC)=1
    ),
    latest_employment_info AS (
      SELECT
        pei.person_id,
        'US_ID' AS state_code,
        emp.name,
        CONCAT(emp.line1,', ', emp.city, ', ', emp.zipcode) AS employer_address,
        SAFE_CAST(startdate AS DATETIME) AS start_date,
        SAFE_CAST(verifydate AS DATETIME) AS verify_date,
      FROM `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.cis_employment_latest` e
      INNER JOIN `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.cis_offender_latest` o
        ON e.personemploymentid = o.id
      INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON o.offendernumber = pei.external_id
        AND pei.id_type = "US_ID_DOC"
        AND pei.state_code = 'US_ID'
      LEFT JOIN `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.cis_employer_latest` emp
        ON e.employerid = emp.id
      QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY SAFE_CAST(startdate AS DATETIME) DESC)=1
    ),
    latest_employment_date AS (
      SELECT
        state_code,
        person_id,
        contact_date,
      FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_contact`
      WHERE verified_employment
        AND state_code = 'US_ID'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY contact_date DESC)=1
    ),
    latest_drug_screen AS (
      SELECT
        person_id,
        state_code,
        drug_screen_date,
        is_positive_result,
      FROM `{{project_id}}.{{sessions_dataset}}.drug_screens_preprocessed_materialized` 
        WHERE sample_type = "URINE"
        AND state_code = 'US_ID'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY drug_screen_date DESC)=1
    ),
    latest_ncic_ilets_check AS(
      SELECT 
        a.state_code,
        a.person_id,
        a.create_dt AS review_date,
        cn.agnt_note_title AS note_title, 
        cn.agnt_note_txt AS note_body, 
      FROM `{{project_id}}.{{supplemental_dataset}}.us_id_case_note_matched_entities` a
      LEFT JOIN `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.agnt_case_updt_latest` cn
        USING(agnt_case_updt_id)
      WHERE ncic_ilets_nco_check
      QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY review_date DESC)=1
    ),
    latest_treatment_discharge AS(
      SELECT 
        a.state_code,
        a.person_id,
        a.create_dt AS discharge_date,
        cn.agnt_note_title AS note_title,
        cn.agnt_note_txt AS note_body,
      FROM `{{project_id}}.{{supplemental_dataset}}.us_id_case_note_matched_entities` a
      LEFT JOIN `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.agnt_case_updt_latest` cn
        USING(agnt_case_updt_id)
      --only select treatment completion records
      WHERE any_treatment AND treatment_complete
      QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY discharge_date DESC)=1
    ),
    #TODO(#14923) replace once state person is hydrated with phone and email
    latest_phone_number AS (
      SELECT
        pei.person_id,
        c.phonenumber,
      FROM `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.cis_personphonenumber_latest` a
      LEFT JOIN `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.cis_codephonenumbertype_latest` b
        ON a.codephonenumbertypeid = b.id
      LEFT JOIN `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.cis_phonenumber_latest` c
        ON a.phonenumberid = c.id
      LEFT JOIN `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.cis_offenderphonenumber_latest` d
        ON a.id = d.id
      LEFT JOIN `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.cis_offender_latest` e
        ON a.personid = e.id
      INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON e.offendernumber = pei.external_id
      WHERE b.active = 'T'
        AND a.primaryphone = 'T'
      QUALIFY ROW_NUMBER() OVER(PARTITION BY pei.person_id ORDER BY d.startdate DESC)=1
    ),
    latest_email AS (
      SELECT
        pei.person_id,
        a.email,
      FROM {{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.cis_personemailaddress_latest a
      LEFT JOIN {{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.cis_offender_latest b
        ON a.personid = b.id
      INNER JOIN {{project_id}}.{{normalized_state_dataset}}.state_person_external_id pei
        ON b.offendernumber = pei.external_id
      WHERE a.iscurrent = "T"
      QUALIFY ROW_NUMBER() OVER(PARTITION BY pei.person_id ORDER BY a.insdate DESC)=1
    ),
   notes AS(
    --violations
      SELECT 
        v.external_id,
        v.state_code,
        v.person_id,
        COALESCE(vr.response_date,v.violation_date) AS event_date,
        COALESCE(vt.violation_type, cn.agnt_note_title) AS note_title,
        IF(cn.agnt_note_txt IS NULL, "--",cn.agnt_note_txt) AS note_body,
        "Violations" AS criteria,
      FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation` v
      LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_type_entry` vt
        USING (supervision_violation_id, person_id, state_code)
      LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_response` vr
        USING (supervision_violation_id, person_id, state_code)
      LEFT JOIN `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.agnt_case_updt_latest` cn
        ON cn.agnt_case_updt_id = SPLIT((v.external_id), '-')[SAFE_OFFSET(1)]
      WHERE (violation_type NOT IN ('TECHNICAL') OR violation_type IS NULL)
        AND DATE_ADD(COALESCE(vr.response_date,v.violation_date), INTERVAL 90 DAY) >= CURRENT_DATE('US/Pacific')
    --ncic/ilets and new crime violations
      UNION ALL
      SELECT 
        a.agnt_case_updt_id AS external_id,
        a.state_code,
        a.person_id,
        a.create_dt AS event_date,
        cn.agnt_note_title AS note_title, 
        cn.agnt_note_txt AS note_body, 
        CASE 
            WHEN ncic_ilets_nco_check THEN "No new criminal activity check"
            WHEN new_crime THEN "Violations"
            ELSE NULL
        END AS criteria
      FROM `{{project_id}}.{{supplemental_dataset}}.us_id_case_note_matched_entities` a
      LEFT JOIN `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.agnt_case_updt_latest` cn
        USING(agnt_case_updt_id)
      WHERE (ncic_ilets_nco_check OR new_crime)
      --only select ncic checks and new crime within the past 90 days
        AND DATE_ADD(a.create_dt, INTERVAL 90 DAY) >= CURRENT_DATE('US/Pacific')
    --Community service,Special conditions, Interlock, , Treatment, Specialty court, Transfer chrono, LSU, UA waiver
      UNION ALL
      SELECT 
        a.agnt_case_updt_id AS external_id,
        a.state_code,
        a.person_id,
        a.create_dt AS event_date,
        cn.agnt_note_title AS note_title, 
        cn.agnt_note_txt AS note_body,
        CASE 
            WHEN (community_service AND NOT not_cs AND NOT agents_warning) THEN "Community service"
            WHEN case_plan THEN "Special Conditions"
            WHEN interlock THEN "Interlock"
            WHEN any_treatment THEN "Treatment"
            WHEN (specialty_court AND court AND NOT psi) THEN "Specialty court"
            WHEN transfer_chrono THEN "Transfer chrono"
            WHEN lsu THEN "Previous LSU notes"
            WHEN (ua AND waiver AND NOT pending AND NOT revocation) THEN "UA waiver"
            ELSE NULL
        END AS criteria
      FROM `{{project_id}}.{{supplemental_dataset}}.us_id_case_note_matched_entities` a
      LEFT JOIN `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.agnt_case_updt_latest` cn
        USING(agnt_case_updt_id)
      WHERE (community_service AND NOT not_cs AND NOT agents_warning)
            OR case_plan
            OR interlock
            OR any_treatment
            OR (specialty_court AND court AND NOT psi)
            OR (transfer_chrono AND DATE_ADD(a.create_dt, INTERVAL 1 YEAR) >= CURRENT_DATE('US/Pacific'))
            OR (lsu AND DATE_ADD(a.create_dt, INTERVAL 6 MONTH) >= CURRENT_DATE('US/Pacific'))
            OR (ua AND waiver AND NOT pending AND NOT revocation)
    --DUI notes
      UNION ALL
      SELECT 
        a.agnt_case_updt_id AS external_id,
        a.state_code,
        a.person_id,
        a.create_dt AS event_date,
        cn.agnt_note_title AS note_title, 
        cn.agnt_note_txt AS note_body,
        "DUI" AS criteria
      FROM `{{project_id}}.{{supplemental_dataset}}.us_id_case_note_matched_entities` a
      LEFT JOIN `{{project_id}}.{{us_id_raw_data_up_to_date_dataset}}.agnt_case_updt_latest` cn
        USING(agnt_case_updt_id)
      WHERE dui 
        AND NOT not_m_dui
         --only include DUI notes within the past 12 months 
        AND DATE_ADD(a.create_dt, INTERVAL 12 MONTH) >= CURRENT_DATE('US/Pacific')
    ),
    latest_notes AS(
      SELECT
        n.state_code,
        n.person_id,
        TO_JSON(ARRAY_AGG(IF(n.note_title IS NOT NULL, STRUCT(n.note_title, n.note_body, n.event_date, n.criteria),NULL) IGNORE NULLS)) AS case_notes,
      FROM notes n
      --only select notes during the current supervision session
      INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized` ses
        ON n.person_id = ses.person_id
        AND CURRENT_DATE('US/Pacific') BETWEEN ses.start_date AND {nonnull_end_date_clause('ses.end_date')}
        AND n.event_date BETWEEN ses.start_date AND {nonnull_end_date_clause('ses.end_date')}
      GROUP BY 1,2
    ),
    latest_alcohol_drug_lsir AS (
      SELECT 
        person_id,
        state_code,
        assessment_date,
        alcohol_drug_total
      FROM `{{project_id}}.{{sessions_dataset}}.us_id_raw_lsir_assessments` 
      QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY assessment_date DESC)=1
    ),
    form AS (
      SELECT
          pei.external_id,
          tes.state_code,
          tes.start_date AS eligible_start_date,
          ses.start_date AS supervision_start_date,
          DATE_DIFF(proj.projected_completion_date_max, CURRENT_DATE('US/Pacific'), DAY) AS days_remaining_on_supervision,
          --aggregate all relevant charge descriptions 
          ARRAY_AGG(DISTINCT charge.description IGNORE NULLS) AS form_information_charge_descriptions,
          pi.current_address AS form_information_current_address,
          ph.phonenumber AS form_information_current_phone_number,
          #TODO(#14923) replace with state person once hydrated 
          --pi.current_phone_number AS form_information_current_phone_number,
          e.email AS form_information_email_address,
          score.assessment_date AS form_information_assessment_date,
          score.assessment_score AS form_information_assessment_score,
          ei.name AS form_information_employer_name,
          ei.employer_address AS form_information_employer_address,
          ei.start_date AS form_information_employment_start_date,
          ed.contact_date AS form_information_employment_date_verified,
          --only include negative drug screen info if within the past 90 days 
          IF(DATE_ADD(ds.drug_screen_date, INTERVAL 90 DAY) >= CURRENT_DATE('US/Pacific'),
                ds.drug_screen_date, NULL) AS form_information_latest_negative_drug_screen_date,
          ncic.review_date AS form_information_ncic_review_date,
          ncic.note_title AS form_information_ncic_note_title,
          ncic.note_body AS form_information_ncic_note_body,
          tx.discharge_date AS form_information_tx_discharge_date,
          tx.note_title AS form_information_tx_note_title,
          tx.note_body AS form_information_tx_note_body,
          ARRAY_AGG(tes.reasons)[ORDINAL(1)] AS reasons,
          ARRAY_AGG(n.case_notes IGNORE NULLS ORDER BY ses.start_date)[ORDINAL(1)] AS case_notes,
          ds.drug_screen_date AS metadata_latest_negative_drug_screen_date, 
          ad.assessment_date AS metadata_lsir_alchohol_drug_date,
          ad.alcohol_drug_total AS metadata_lsir_alcohol_drug_score
      FROM `{{project_id}}.{{task_eligibility_dataset}}.complete_transfer_to_limited_supervision_form_materialized` tes
      INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized` ses
        ON tes.state_code = ses.state_code
        AND tes.person_id = ses.person_id 
        AND tes.start_date BETWEEN ses.start_date AND {nonnull_end_date_clause('ses.end_date')}
        AND tes.state_code = 'US_ID'
      INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON tes.state_code = pei.state_code 
        AND tes.person_id = pei.person_id
        AND pei.id_type = "US_ID_DOC"
      LEFT JOIN `{{project_id}}.{{sessions_dataset}}.supervision_projected_completion_date_spans_materialized` proj
        ON proj.state_code = ses.state_code
        AND proj.person_id = ses.person_id
        --use the projected completion date from the current span
        AND CURRENT_DATE('US/Pacific') BETWEEN proj.start_date AND {nonnull_end_date_exclusive_clause('proj.end_date')}
      LEFT JOIN sentence_charge_description charge
        ON tes.state_code = charge.state_code
        AND tes.person_id = charge.person_id
      LEFT JOIN latest_phone_number ph
        ON tes.person_id = ph.person_id
      LEFT JOIN latest_email e
        ON tes.person_id = e.person_id
      LEFT JOIN person_info pi
        ON tes.state_code = pi.state_code
        AND tes.person_id = pi.person_id
      LEFT JOIN latest_assessment_score score
        ON tes.state_code = score.state_code
        AND tes.person_id = score.person_id
        AND score.assessment_date >= ses.start_date
      LEFT JOIN latest_employment_info ei
        ON tes.state_code = ei.state_code
        AND tes.person_id = ei.person_id
        AND ei.start_date >= ses.start_date
      LEFT JOIN latest_employment_date ed
        ON tes.state_code = ed.state_code
        AND tes.person_id = ed.person_id
      LEFT JOIN latest_drug_screen ds
        ON tes.state_code = ds.state_code
        AND tes.person_id = ds.person_id
        --only join negative results 
        AND NOT ds.is_positive_result
      LEFT JOIN latest_alcohol_drug_lsir ad
        ON tes.state_code = ad.state_code
        AND tes.person_id = ad.person_id
        AND ad.assessment_date >= ses.start_date
        --only join alcohol_drug_totals of 0
        AND ad.alcohol_drug_total = 0
      LEFT JOIN latest_ncic_ilets_check ncic
        ON tes.state_code = ncic.state_code
        AND tes.person_id = ncic.person_id
        AND DATE_ADD(review_date, INTERVAL 90 DAY) >= CURRENT_DATE('US/Pacific')
      LEFT JOIN latest_treatment_discharge tx
        ON tes.state_code = tx.state_code
        AND tes.person_id = tx.person_id
        --select only treatment discharges that occured within the last supervision session
        AND tx.discharge_date BETWEEN ses.start_date AND {nonnull_end_date_clause('ses.end_date')}
      LEFT JOIN latest_notes n
        ON tes.state_code = n.state_code
        AND tes.person_id = n.person_id
      WHERE CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND {nonnull_end_date_exclusive_clause('tes.end_date')}
        AND tes.is_eligible
        AND tes.state_code = 'US_ID'
      GROUP BY 1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,25,26,27
     )
     SELECT * from form 
     ORDER BY external_id
"""

US_ID_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_ID_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_NAME,
    view_query_template=US_ID_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_QUERY_TEMPLATE,
    description=US_ID_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    supplemental_dataset=SUPPLEMENTAL_DATA_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_ID
    ),
    should_materialize=True,
    us_id_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ID, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.build_and_print()
