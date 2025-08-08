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
"""
Helper SQL queries for Michigan
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    array_agg_case_notes_by_external_id,
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    extract_object_from_json,
)


def scc_form(
    description: str,
    view_id: str,
    task_name: str,
) -> SimpleBigQueryViewBuilder:
    """
    Creates a big query view builder that generates the opportunity record query needed for all
    security classification committee review opportunities.

    Args:
        task_name (str): Task name from task_eligibility_spans
        description (str): form descriptions
        view_id (str): view_builder name

    Returns:
        SimpleBigQueryViewBuilder
    """

    if (
        task_name
        == "complete_security_classification_committee_review_form_materialized"
    ):
        reasons_blob = "latest_scc_review_date"
        criteria_name = "US_MI_PAST_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_DATE"
    elif (
        task_name
        == "complete_warden_in_person_security_classification_committee_review_form_materialized"
    ):
        reasons_blob = "latest_warden_in_person_scc_review_date"
        criteria_name = "US_MI_PAST_WARDEN_IN_PERSON_REVIEW_FOR_SCC_DATE"
    elif (
        task_name
        == "complete_add_in_person_security_classification_committee_review_form_materialized"
    ):
        reasons_blob = "latest_add_in_person_scc_review_date"
        criteria_name = "US_MI_PAST_ADD_IN_PERSON_REVIEW_FOR_SCC_DATE"
    else:
        raise ValueError("Incorrect input for task_name")

    query_template = f"""
    WITH eligible_and_almost_eligible AS (
{join_current_task_eligibility_spans_with_external_id(
    state_code=StateCode.US_MI,
    tes_task_query_view=task_name,
    id_type="'US_MI_DOC'",
    additional_columns='tes.start_date',
    eligible_and_almost_eligible_only=True,
)}
),
release_dates AS (
/* Queries raw data for the min and max release dates */ 
    SELECT 
        LPAD(offender_number, 7, "0") AS offender_number,
        MAX(pmi_sgt_min_date) AS pmi_sgt_min_date, 
        MAX(pmx_sgt_max_date) AS pmx_sgt_max_date,
    FROM `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.ADH_OFFENDER_SENTENCE_latest`
    INNER JOIN `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.ADH_OFFENDER_latest` 
        USING(offender_id)
    WHERE sentence_type_id = '430' -- sentence is a prison sentence
        AND closing_date is NULL --sentence is open 
    GROUP BY 1
),
bondable_codes AS (
/* Queries all bondable codes for misconduct reports in the last 6 months */
    SELECT
        person_id,
        incident_date,
        SPLIT(JSON_EXTRACT_SCALAR(incident_metadata, '$.BONDABLE_OFFENSES'), ',') AS bondable_offenses
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident`
    WHERE 
        state_code = "US_MI"
        AND DATE_ADD(incident_date, INTERVAL 6 MONTH) >= CURRENT_DATE('US/Eastern')
        AND JSON_EXTRACT_SCALAR(incident_metadata, '$.BONDABLE_OFFENSES') != ''
    ),
nonbondable_codes AS (
/* Queries all nonbondable codes for misconduct reports in the last year */
    SELECT
        person_id,
        incident_date,
        SPLIT(JSON_EXTRACT_SCALAR(incident_metadata, '$.NONBONDABLE_OFFENSES'), ',') AS nonbondable_offenses
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident`
    WHERE 
        state_code = "US_MI"
        AND DATE_ADD(incident_date, INTERVAL 1 YEAR) >= CURRENT_DATE('US/Eastern')
        AND JSON_EXTRACT_SCALAR(incident_metadata, '$.NONBONDABLE_OFFENSES') != ''
),
form_misconduct_codes AS (
/* Queries bondable and nonbondable codes to include on the scc form. 
For bondable codes, the resident can remain in their current cell. 
For nonbondable code, the resident must be moved to segregation until ticket is heard. */
    SELECT 
        COALESCE(b.person_id, n.person_id) AS person_id,
        CONCAT('(', 
            STRING_AGG(DISTINCT CONCAT(bondable_offense, ', ', STRING(b.incident_date)), '), (' ORDER BY CONCAT(bondable_offense, ', ', STRING(b.incident_date))),
        ')') AS bondable_offenses_within_6_months,
        CONCAT('(',
            STRING_AGG(DISTINCT CONCAT(nonbondable_offense, ', ', STRING(n.incident_date)), '), (' ORDER BY CONCAT(nonbondable_offense, ', ', STRING(n.incident_date))),
        ')') AS nonbondable_offenses_within_1_year
    FROM (
        SELECT 
            person_id,
            incident_date,
            bondable_offense
        FROM bondable_codes,
            UNNEST(bondable_offenses) AS bondable_offense
    ) b
    FULL OUTER JOIN (
        SELECT 
            person_id,
            incident_date,
            nonbondable_offense
        FROM nonbondable_codes,
            UNNEST(nonbondable_offenses) AS nonbondable_offense
    ) n
        ON n.person_id = b.person_id
    GROUP BY COALESCE(b.person_id, n.person_id)
),
recent_misconduct_codes AS (
/* Queries bondable and nonbondable codes within the current housing_unit_session for metadata. */
    SELECT 
        h.person_id,
        CONCAT('(',
            STRING_AGG(DISTINCT CONCAT(bondable_offense, ', ', STRING(b.incident_date)), '), (' ORDER BY CONCAT(bondable_offense, ', ', STRING(b.incident_date))),
        ')') AS bondable_offenses_within_6_months,
        CONCAT('(',
            STRING_AGG(DISTINCT CONCAT(nonbondable_offense, ', ', STRING(n.incident_date)), '), (' ORDER BY CONCAT(nonbondable_offense, ', ', STRING(n.incident_date))),
        ')') AS nonbondable_offenses_within_1_year,
    FROM `{{project_id}}.{{sessions_dataset}}.housing_unit_type_collapsed_solitary_sessions_materialized` h
    LEFT JOIN (
        SELECT 
            person_id,
            incident_date,
            bondable_offense
        FROM bondable_codes,
            UNNEST(bondable_offenses) AS bondable_offense
        ) b
        ON b.person_id = h.person_id 
        AND b.incident_date >= h.start_date 
    LEFT JOIN (
        SELECT 
            person_id,
            incident_date,
            nonbondable_offense
        FROM nonbondable_codes,
            UNNEST(nonbondable_offenses) AS nonbondable_offense
        ) n
        ON n.person_id = h.person_id
        AND n.incident_date >= h.start_date 
    WHERE 
        --only select current housing_unit_sessions
        CURRENT_DATE('US/Eastern') BETWEEN h.start_date AND {nonnull_end_date_exclusive_clause('h.end_date_exclusive')}
    GROUP BY 1
),
previous_ad_seg_stays AS (
/* Queries all previous ad seg stays from the past three years, and the associates the closest incarceration incident.
30 days in ad seg is used a proxy to determine "true" ad seg stays, vs. time spent in an ad seg cell */
SELECT 
  person_id,
  ARRAY_AGG(
    CONCAT('(', STRING(start_date), ',', offenses, ')') IGNORE NULLS ORDER BY start_date, offenses
  ) AS ad_seg_stays_and_reasons_within_3_yrs
FROM (
  SELECT 
    h.person_id,
    h.start_date,
    --concatenate nonbondable and bondable offenses 
    COALESCE(IF(JSON_EXTRACT_SCALAR(i.incident_metadata, '$.NONBONDABLE_OFFENSES') != "", 
        CONCAT(JSON_EXTRACT_SCALAR(i.incident_metadata, '$.NONBONDABLE_OFFENSES'), ",", 
        JSON_EXTRACT_SCALAR(i.incident_metadata, '$.BONDABLE_OFFENSES')),
        JSON_EXTRACT_SCALAR(i.incident_metadata, '$.BONDABLE_OFFENSES')), "") AS offenses
  FROM 
    `{{project_id}}.{{sessions_dataset}}.housing_unit_type_sessions_materialized` h
  LEFT JOIN 
    `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident` i
  ON 
    h.state_code = i.state_code
    AND h.person_id = i.person_id 
    --only join incidents that happen within 2 months of the segregation stay 
    AND DATE_DIFF(start_date, incident_date, DAY) < 60
    AND i.incident_date <= h.start_date
  WHERE housing_unit_type = 'ADMINISTRATIVE_SOLITARY_CONFINEMENT'
  --only join previous (closed) ad seg stays 
    AND h.end_date_exclusive IS NOT NULL
    --only select stays greater than one month 
    AND DATE_DIFF(h.end_date_exclusive, h.start_date, DAY) >= 30
    --only select stays within the last 3 years
    AND DATE_ADD(h.start_date, INTERVAL 3 YEAR) >= CURRENT_DATE('US/Eastern')
    --choose the most recent incident report within the last 2 months and prioritize the incident with the bondable code
  QUALIFY ROW_NUMBER() OVER (PARTITION BY h.person_id, h.start_date ORDER BY JSON_EXTRACT_SCALAR(i.incident_metadata, '$.NONBONDABLE_OFFENSES') = "", incident_date DESC) = 1
)
GROUP BY person_id
),
reasons_for_eligibility AS (
/* Queries the reasons for eligibility from the currently eligible or upcoming eligible span */
    SELECT 
        person_id,
        {extract_object_from_json(
            json_column="criteria_reason",
            object_column=f"reason.{reasons_blob}",
            object_type="DATE",
        )} AS latest_scc_review_date,
        {extract_object_from_json(
            json_column="criteria_reason",
            object_column="reason.next_scc_date",
            object_type="DATE",
        )} AS next_scc_date,
    FROM eligible_and_almost_eligible,
    UNNEST(JSON_QUERY_ARRAY(reasons)) AS criteria_reason
    -- Pull out the reasons blob from the one criteria with review date metadata
    WHERE {extract_object_from_json(
            json_column="criteria_reason",
            object_column="criteria_name",
            object_type="STRING",
        )} = "{criteria_name}"
),
SMI AS (
  select OffenderCode, MMD
  from `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.COMS_Mental_Health_Problems_latest`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderCode ORDER BY CAST(StatusDate AS DATETIME) DESC) = 1
),
OPT AS (
  select OffenderCode, OPTLevelOfCare
  from `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.COMS_Level_Of_Care_latest`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderCode ORDER BY CAST(AdmissionDate AS DATETIME) DESC) = 1
),
stg_information AS (
    SELECT 
        offender_number,
        STG_Level
    FROM `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.COMS_Security_Threat_Group_Involvement_latest`
    -- there are a small number of times where a single person appears wih two distinct STG levels in the table
    -- in those cases, let's pick the STG level that was most recently entered
    QUALIFY ROW_NUMBER() OVER(PARTITION BY offender_number ORDER BY CAST(Start_Date AS DATETIME) DESC, STG_LEVEL DESC) = 1
),
assessment_information AS (
    SELECT 
        offender_number,
        Management_Level_Assessment_Result,
        Confinement_Level_Assessment_Result,
        Actual_Placement_Level_Assessment_Result,
    FROM `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.COMS_Security_Classification_latest`
),
case_notes_cte AS (
    SELECT
        pei.external_id,
        Activity AS criteria,
        NULL AS note_title,
        cn.Note AS note_body,
        DATE(cn.Note_Date) AS event_date
    FROM `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.COMS_Case_Notes_latest` cn 
    INNER JOIN `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.COMS_Supervision_Schedule_Activities_latest` 
        USING(Offender_Number, Case_Note_Id)
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON cn.Offender_Number = pei.external_id 
        AND id_type = 'US_MI_DOC'
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.compartment_level_1_super_sessions_materialized` s
        ON s.person_id = pei.person_id 
        AND CURRENT_DATE BETWEEN s.start_date AND {nonnull_end_date_exclusive_clause('s.end_date_exclusive')}
    WHERE Activity in ('SCC - Security Classification Committee','SCC – ADD – 12 Month Review','SCC – Warden – 6 Month Review','In Person Contact')
    AND DATE(cn.Note_Date) BETWEEN s.start_date AND {nonnull_end_date_exclusive_clause('s.end_date_exclusive')}
),
array_case_notes_cte AS (
{array_agg_case_notes_by_external_id()}
)
SELECT 
    tes.person_id,
    tes.state_code,
    tes.external_id AS external_id,
    tes.reasons,
    tes.is_eligible,
    tes.is_almost_eligible,
    tes.ineligible_criteria,
    IF(tes.is_eligible,DATE_DIFF(CURRENT_DATE("US/Eastern"), e.next_scc_date, DAY)>=7, False) AS is_overdue,
    --form information
    tes.external_id AS form_information_prisoner_number,
    INITCAP(JSON_VALUE(PARSE_JSON(sp.full_name), '$.given_names'))
            || " " 
            || INITCAP(JSON_VALUE(PARSE_JSON(sp.full_name), '$.surname')) AS form_information_prisoner_name,
    DATE(pmx_sgt_max_date) AS form_information_max_release_date,
    DATE(pmi_sgt_min_date) AS form_information_min_release_date,
    si.facility AS form_information_facility,
    si.housing_unit AS form_information_lock,
    (SMI.MMD = "Yes") AS form_information_OPT,
    CASE WHEN stg.STG_Level = 'I' THEN "1"
         WHEN stg.STG_Level = 'II' THEN "2"
         ELSE stg.STG_Level 
      END AS form_information_STG,
    h.housing_unit_type AS form_information_segregation_type,
    h.start_date AS form_information_segregation_classification_date,
    m.bondable_offenses_within_6_months AS form_information_bondable_offenses_within_6_months,
    m.nonbondable_offenses_within_1_year AS form_information_nonbondable_offenses_within_1_year,
    p.ad_seg_stays_and_reasons_within_3_yrs AS form_information_ad_seg_stays_and_reasons_within_3_yrs,
    --metadata 
    e.latest_scc_review_date AS metadata_latest_scc_review_date,
    DATE(pmx_sgt_max_date) AS metadata_max_release_date,
    DATE(pmi_sgt_min_date) AS metadata_min_release_date,
    DATE_DIFF(DATE(pmi_sgt_min_date), CURRENT_DATE('US/Eastern'), MONTH) <24 AS metadata_less_than_24_months_from_erd,
    DATE_DIFF(CURRENT_DATE('US/Eastern'), h.start_date, DAY) AS metadata_days_in_solitary_session,
    DATE_DIFF(CURRENT_DATE('US/Eastern'), hc.start_date, DAY) AS metadata_days_in_collapsed_solitary_session,
    h.start_date AS metadata_solitary_session_start_date,
    CASE WHEN h.housing_unit_type = 'ADMINISTRATIVE_SOLITARY_CONFINEMENT' THEN 'Administrative Segregation'
            WHEN h.housing_unit_type = 'TEMPORARY_SOLITARY_CONFINEMENT' THEN 'Temporary Segregation'
            WHEN h.housing_unit_type = 'DISCIPLINARY_SOLITARY_CONFINEMENT' THEN 'Detention'
            WHEN h.housing_unit_type = 'MENTAL_HEALTH_SOLITARY_CONFINEMENT' THEN 'Observation'
            ELSE 'Other Segregation'
    END AS metadata_solitary_session_type,
    (OPT.OPTLevelOfCare = "Y") AS metadata_OPT,
    rm.bondable_offenses_within_6_months AS metadata_recent_bondable_offenses,
    rm.nonbondable_offenses_within_1_year AS metadata_recent_nonbondable_offenses,
    p.ad_seg_stays_and_reasons_within_3_yrs AS metadata_ad_seg_stays_and_reasons_within_3_yrs,
    #TODO(#28298) add in missing info when we receive data
    NULL AS metadata_needed_programming,
    NULL AS metadata_completed_programming,
    assessment.Management_Level_Assessment_Result AS metadata_management_security_level,
    assessment.Confinement_Level_Assessment_Result AS metadata_confinement_security_level,
    assessment.Actual_Placement_Level_Assessment_Result AS metadata_actual_security_level,
    a.case_notes 
FROM eligible_and_almost_eligible tes
LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person` sp
    ON tes.state_code = sp.state_code
    AND tes.person_id = sp.person_id
LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_period` si
    ON tes.state_code = si.state_code
    AND tes.person_id = si.person_id
    AND CURRENT_DATE('US/Eastern') BETWEEN si.admission_date AND {nonnull_end_date_clause('si.release_date')}
LEFT JOIN release_dates sgt
    ON sgt.offender_number = tes.external_id 
LEFT JOIN form_misconduct_codes m
    ON tes.person_id = m.person_id
LEFT JOIN recent_misconduct_codes rm
    ON tes.person_id = rm.person_id
LEFT JOIN previous_ad_seg_stays p
    ON tes.person_id = p.person_id
LEFT JOIN `{{project_id}}.{{sessions_dataset}}.housing_unit_type_sessions_materialized` h
    ON h.person_id = tes.person_id 
    AND CURRENT_DATE('US/Eastern') BETWEEN h.start_date AND {nonnull_end_date_exclusive_clause('h.end_date_exclusive')}
LEFT JOIN `{{project_id}}.{{sessions_dataset}}.housing_unit_type_non_protective_custody_solitary_sessions_materialized` hc
    ON hc.person_id = tes.person_id 
    AND CURRENT_DATE('US/Eastern') BETWEEN hc.start_date AND {nonnull_end_date_exclusive_clause('hc.end_date_exclusive')}
LEFT JOIN reasons_for_eligibility e
    ON tes.person_id = e.person_id
LEFT JOIN assessment_information assessment
    ON tes.external_id = assessment.offender_number
LEFT JOIN OPT opt
    ON tes.external_id = OPT.OffenderCode
LEFT JOIN SMI smi
    ON tes.external_id = smi.OffenderCode
LEFT JOIN stg_information stg
    ON tes.external_id = stg.offender_number
LEFT JOIN array_case_notes_cte a 
    ON a.external_id = tes.external_id

    """
    return SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
        view_id=view_id,
        view_query_template=query_template,
        description=description,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        sessions_dataset=SESSIONS_DATASET,
        us_mi_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_MI, instance=DirectIngestInstance.PRIMARY
        ),
        should_materialize=True,
    )
