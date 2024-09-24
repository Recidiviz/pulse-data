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
Helper SQL queries for Tennessee
"""

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause

DISCIPLINARY_HISTORY_MONTH_LOOKBACK = "60"

# TODO(#20870) - Deprecate this in favor of better long term solution to excluding these levels
# These are DRC (Day Reporting Centers) and PSU (Sex Offense Unit) supervision levels that we map to Medium/High, but
# that are usually excluded from various supervision side opportunities like compliant reporting, downgrades based on
# risk assessments, etc
EXCLUDED_MEDIUM_RAW_TEXT = ["6P1", "6P2", "6P3", "6P4", "3D3"]
EXCLUDED_HIGH_RAW_TEXT = ["1D1", "2D2"]


def detainers_cte() -> str:
    """Helper method that returns a CTE getting detainer information in TN"""

    return f"""
        -- As discussed with TTs in TN, a detainer is "relevant" until it has been lifted, so we use that as
        -- our end date
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            detainer_felony_flag,
            detainer_misdemeanor_flag,
            CASE WHEN detainer_felony_flag = 'X' THEN 5
                 WHEN detainer_misdemeanor_flag = 'X' THEN 3
                 END AS detainer_score,
            jurisdiction,
            description,
            charge_pending,
        FROM (
            SELECT
                OffenderID,
                DATE(DetainerReceivedDate) AS start_date,
                DATE(DetainerLiftDate) AS end_date,
                -- According to TN counselors, if a detainer is missing a felony/misdemeanor flag but is from a federal
                -- agency, it's always a felony
                CASE WHEN DetainerFelonyFlag IS NULL
                        AND DetainerMisdemeanorFlag IS NULL
                        AND Jurisdiction IN ("FED","INS") THEN 'X'
                    ELSE DetainerFelonyFlag END AS detainer_felony_flag,
                DetainerMisdemeanorFlag AS detainer_misdemeanor_flag,
                Jurisdiction AS jurisdiction,
                OffenseDescription AS description,
                ChargePendingFlag AS charge_pending,
            FROM
                `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.Detainer_latest`
            ) dis
        INNER JOIN
            `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON
            dis.OffenderID = pei.external_id
        AND
            pei.state_code = 'US_TN'
        WHERE
            {nonnull_end_date_exclusive_clause('end_date')} > {nonnull_end_date_exclusive_clause('start_date')}
        
        """


def keep_contact_codes(
    codes_cte: str,
    comments_cte: str,
    where_clause_codes_cte: str,
    output_name: str = "output",
    keep_last: bool = False,
) -> str:
    """
    Helper function to join on contact codes with comments, filter to specific codes, and either keep all codes of a
    specific type or the latest one. Useful for TEPE form and side-bar

    codes_cte: String that contains a CTE with contact notes that can be filtered on ContactNoteType
    comments_cte: String that contains a CTE with contact comments
    where_clause_codes_cte: String used to filter to specific contact types
    output_name: Optional parameter for Struct containing contact date, type, comment
    keep_last: Optional parameter defaulting to false. If true, only the latest contact date for a given person is kept,
              otherwise all are
    """

    qualify = ""
    if keep_last:
        qualify = "QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY contact_date DESC) = 1"

    return f"""
        SELECT
            person_id,
            STRUCT(
              contact_date,
              contact_type,
              contact_comment
            ) AS {output_name},
            contact_date,
            contact_type,
            contact_comment,
        FROM (
            SELECT
                person_id,
                contact_date,
                STRING_AGG(DISTINCT contact_type, ", ") AS contact_type,
            FROM {codes_cte}
            {where_clause_codes_cte}
            GROUP BY 1,2
        )
        LEFT JOIN (
            SELECT
                person_id,
                contact_date,
                STRING_AGG(DISTINCT contact_comment, ", ") AS contact_comment,
            FROM {comments_cte}
            GROUP BY 1,2        
        )
        USING(person_id, contact_date)
        {qualify}
    """


def us_tn_classification_forms(
    tes_view: str,
    where_clause: str,
    disciplinary_history_month_lookback: str = DISCIPLINARY_HISTORY_MONTH_LOOKBACK,
) -> str:
    return f"""
    WITH latest_classification AS (
        SELECT person_id,
              external_id,
              DATE(ClassificationDate) AS form_information_latest_classification_date,
              OverrideReason AS form_information_latest_override_reason,
              RecommendedCustody AS recommended_custody_level,
              DATE_ADD(DATE(ClassificationDate), INTERVAL 12 MONTH) AS form_reclassification_due_date,
        FROM
            `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.Classification_latest` c
        INNER JOIN 
            `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON
            c.OffenderID = pei.external_id
        AND
            pei.state_code = 'US_TN'
        WHERE
            ClassificationDecision = 'A'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY DATE(ClassificationDecisionDate) DESC) = 1
    ),
    latest_CAF AS (
        SELECT
            person_id,
            assessment_date AS form_information_last_CAF_date,
            assessment_score AS form_information_last_CAF_total,
            CAST(JSON_EXTRACT_SCALAR(assessment_metadata,'$.SCHEDULEASCORE') AS INT64) AS last_CAF_schedule_A,
            CAST(JSON_EXTRACT_SCALAR(assessment_metadata,'$.SCHEDULEBSCORE') AS INT64) AS last_CAF_schedule_B
        FROM
            `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` asmt
        WHERE
            assessment_type = 'CAF'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id  ORDER BY assessment_date DESC, assessment_score DESC) = 1
    ),
    -- This CTE keeps all disciplinaries that we may want to count
    disciplinaries AS (
        SELECT 
            person_id,
            state_code,
            incident_date AS event_date,
            COALESCE(infraction_type_raw_text, incident_type_raw_text) AS incident_type,
            CASE WHEN injury_level != "" 
                 THEN CONCAT(COALESCE(infraction_type_raw_text, incident_type_raw_text), "- Injury:", injury_level)
                 ELSE COALESCE(infraction_type_raw_text, incident_type_raw_text)
                 END AS note_title,
            injury_level,
            incident_class,
            CONCAT('Class ', 
                    incident_class,
                    ' Incident Code:',
                    COALESCE(infraction_type_raw_text, incident_type_raw_text),
                    '  Incident Details:',
                    incident_details) AS note_body,
            disposition,
            assault_score,
            incident_details,
        FROM 
            `{{project_id}}.{{analyst_dataset}}.incarceration_incidents_preprocessed_materialized` dis
        WHERE
            state_code = "US_TN"
    ),    
    -- Union case notes
    case_notes_cte AS (
        -- This only keeps disciplinaries with an assault, for Q1 and Q2 info
        SELECT
            person_id,
            "ASSAULTIVE DISCIPLINARIES" AS criteria,
            event_date,
            note_title,
            note_body, 
            FROM
            disciplinaries
        WHERE
            assault_score IS NOT NULL
            AND event_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL {disciplinary_history_month_lookback} MONTH)
    
        UNION ALL
        
        SELECT 
            person_id,
            "PRIOR RECORD OFFENSES" AS criteria,
            disposition_date AS event_date,
            description AS note_title,
            CAST(NULL AS STRING) AS note_body,
        FROM `{{project_id}}.{{analyst_dataset}}.us_tn_prior_record_preprocessed_materialized`
        
        UNION ALL
        
        SELECT 
            person_id,
            "TN, ISC, DIVERSION SENTENCES" AS criteria,
            date_imposed AS event_date,
            description AS note_title,
            CONCAT("Expires: ", CAST(projected_completion_date_max AS STRING)) AS note_body,
        FROM `{{project_id}}.sessions.sentences_preprocessed_materialized`
        WHERE state_code = "US_TN"
    ),
    case_notes_array AS (
        SELECT
            person_id,
            -- Group all notes into an array within a JSON
            TO_JSON(
                ARRAY_AGG(
                    STRUCT(note_title, note_body, event_date, criteria)
                    )
            ) AS case_notes,
        FROM
            case_notes_cte
        GROUP BY 1
    ),
    -- This CTE only keeps guilty disciplinaries, for Q6 and Q7 info
    guilty_disciplinaries AS (
        SELECT
            person_id,
            event_date,
            incident_class,
            note_body,
        FROM
            disciplinaries
        -- For q6 and q7, we only want to consider guilty disciplinaries with non-missing disciplinary class
        WHERE disposition = 'GU'
             AND note_body != ""
             AND incident_details NOT LIKE "%VERBAL WARNING%"
    ), 
    recommended_scores AS (
      -- Schedule B is only scored if Schedule A is 9 or less, so this CTE "re scores" schedule B scores if needed
      SELECT * 
        EXCEPT(form_information_calculated_total_score),
             CASE WHEN form_information_calculated_schedule_a_score > 9 
                  THEN form_information_calculated_schedule_a_score 
                  ELSE form_information_calculated_total_score
                  END AS form_information_calculated_total_score,
      FROM (
          SELECT person_id,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q1_score') AS INT64) AS form_information_q1_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q2_score') AS INT64) AS form_information_q2_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q3_score') AS INT64) AS form_information_q3_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q4_score') AS INT64) AS form_information_q4_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q5_score') AS INT64) AS form_information_q5_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q6_score') AS INT64) AS form_information_q6_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q7_score') AS INT64) AS form_information_q7_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q8_score') AS INT64) AS form_information_q8_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q9_score') AS INT64) AS form_information_q9_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.calculated_schedule_a_score') AS INT64) AS form_information_calculated_schedule_a_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.calculated_schedule_b_score') AS INT64) AS form_information_calculated_schedule_b_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.calculated_total_score') AS INT64) AS form_information_calculated_total_score,
          FROM
            `{{project_id}}.{{analyst_dataset}}.recommended_custody_level_spans_materialized`
          WHERE
            CURRENT_DATE('US/Pacific') BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date_exclusive')}
            AND state_code = 'US_TN'
      )
    ),
    q6_score_info AS (
        SELECT person_id, 
                TO_JSON(
                    ARRAY_AGG(
                        STRUCT(note_body, event_date)
                    IGNORE NULLS
                    ORDER BY event_date DESC)
                ) AS form_information_q6_notes
        FROM (
            SELECT *
            FROM guilty_disciplinaries
            WHERE event_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 6 MONTH)
            -- This surfaces disciplinaries on the 2 most recent dates in the last 6 months.
            -- If there are multiple disciplinaries the 2 most recent dates, we will surface those
            QUALIFY RANK() OVER(PARTITION BY person_id ORDER BY event_date DESC) <= 2
        )
        GROUP BY 1
    ),
    q7_score_info AS (
        SELECT
            person_id,
            TO_JSON(STRUCT(event_date, note_body)) AS form_information_q7_notes
        FROM guilty_disciplinaries
        WHERE event_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 18 MONTH)
        -- Keep most serious incident. If there are duplicates, keep latest incident within that class
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id 
                                  ORDER BY CASE WHEN incident_class = 'A' THEN 1
                                                  WHEN incident_class = 'B' THEN 2
                                                  WHEN incident_class = 'C' THEN 3
                                                  END ASC,
                                                  event_date DESC
                                                  ) = 1
    ),
    level_care AS (
        SELECT
            person_id,
            SubServiceType AS form_information_level_of_care
        FROM
            `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.MentalHealthServices_latest` m
        INNER JOIN
            `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON
            m.OffenderID = pei.external_id
        AND
            pei.state_code = 'US_TN'
        WHERE
            MainServiceType = 'LVCA'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY DATE(HealthServiceDateTime) DESC) = 1
    ),
    vantage AS (
        SELECT  
            a.OffenderID, 
            a.RiskLevel,
            DATE(a.CompletedDate) AS CompletedDate,
            r.Pathway,
            p.PathwayName,
            r.Recommendation,
            r.TreatmentGoal,
            DATE(r.RecommendationDate) AS RecommendationDate,
            DATE(r.RecommendedEndDate) AS RecommendedEndDate,
            pr.VantagePointTitle,
            c.latest_refusal_date,
            c.latest_refusal_contact
        FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.VantagePointAssessments_latest` a
        LEFT JOIN `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.VantagePointRecommendations_latest` r
          ON a.OffenderID = r.OffenderID
          AND a.AssessmentID = r.AssessmentID
        LEFT JOIN `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.VantagePointPathways_latest` p
          USING(Recommendation, Pathway)
        LEFT JOIN `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.VantagePointProgram_latest` pr
          ON pr.VantageProgramID = r.VantagePointProgamID
        LEFT JOIN (
            SELECT
                OffenderID,
                CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS latest_refusal_date,
                ContactNoteType AS latest_refusal_contact
            FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.ContactNoteType_latest`
            WHERE ContactNoteType IN ("IRAR", "XRIS", "CCMC")
            QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) DESC) = 1
            ) c
          ON a.OffenderID = c.OffenderID
          AND latest_refusal_date >= DATE(a.CompletedDate)
        -- As per TTs, only information associated with latest assessment (e.g. recommendation associated with latest assessment)
        -- should be considered relevant
        QUALIFY ROW_NUMBER() OVER(PARTITION BY a.OffenderID ORDER BY DATE(a.CompletedDate) DESC) = 1
    ),
    active_vantage_recommendations AS (
        SELECT
            OffenderID,
            ARRAY_AGG(
                STRUCT(
                    Recommendation,
                    Pathway,
                    PathwayName,
                    TreatmentGoal,
                    VantagePointTitle
                )
            ) AS active_recommendations,
        FROM vantage
        -- Keep only recommendations that are still active, since there might be recommendations associated with the 
        -- latest assessment that have ended
        WHERE CURRENT_DATE BETWEEN DATE(RecommendationDate) AND COALESCE(DATE(RecommendedEndDate), '9999-01-01')
        GROUP BY 1
    ),
    detainers_cte AS (
        SELECT person_id,
               TO_JSON(
                ARRAY_AGG(
                    STRUCT(start_date AS detainer_received_date, 
                           detainer_felony_flag AS detainer_felony_flag, 
                           detainer_misdemeanor_flag AS detainer_misdemeanor_flag,
                           jurisdiction AS jurisdiction,
                           description AS description,
                           charge_pending AS charge_pending
                           )
                )
            ) AS form_information_q8_notes
        FROM ({detainers_cte()})
        WHERE end_date IS NULL
        GROUP BY 1
    ),
    incompatibles AS (
        /*
         This table should list each pair of incompatible folks (i.e. Person A as Offender ID
         and Person B as IncompatibleOffenderID, and another row for Person B as Offender ID 
         and Person A as  IncompatibleOffenderID. But this doesn't always happen so we're unioning
         the table twice (first using OffenderID and then using IncompatibleOffenderID to catch all)
        */
        SELECT
            DISTINCT
            OffenderID,
            CASE WHEN IncompatibleType = 'S' THEN 'STAFF'
                  -- If the location for anyone is a county jail, for example, we don't need the specific jail info
                  ELSE IF(location_type='STATE_PRISON',facility_id,'OUT OF SYSTEM')
                  END AS incompatible_offender_id,
            IncompatibleType AS incompatible_type,
        FROM `{{project_id}}.us_tn_raw_data_up_to_date_views.IncompatiblePair_latest` i
        -- Left join because we don't expect this to work if IncompatibleOffenderID is a Staff ID
        LEFT JOIN 
            `{{project_id}}.normalized_state.state_person_external_id` pei
        ON
            i.IncompatibleOffenderID = pei.external_id
        AND
            pei.state_code = 'US_TN'
        LEFT JOIN (
            SELECT
                person_id,
                state_code,
                facility AS facility_id,
            FROM `{{project_id}}.sessions.compartment_sub_sessions_materialized`
            WHERE
                CURRENT_DATE('US/Pacific') BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date_exclusive')}
        ) cs
            USING(person_id, state_code)
        LEFT JOIN `{{project_id}}.reference_views.location_metadata_materialized` lm
            ON cs.facility_id = lm.location_external_id
            AND cs.state_code = lm.state_code
        WHERE IncompatibleRemovedDate IS NULL
            AND (facility_id IS NOT NULL OR IncompatibleType = 'S')

        UNION DISTINCT 
    
        SELECT
            DISTINCT
            IncompatibleOffenderID AS OffenderID,
            CASE WHEN IncompatibleType = 'S' THEN 'STAFF'
                  -- If the location for anyone is a county jail, for example, we don't need the specific jail info
                  ELSE IF(location_type='STATE_PRISON',facility_id,'OUT OF SYSTEM')
                  END AS incompatible_offender_id,
            IncompatibleType AS incompatible_type,
        FROM `{{project_id}}.us_tn_raw_data_up_to_date_views.IncompatiblePair_latest` i
        -- Left join because we don't expect this to work if IncompatibleOffenderID is a Staff ID
        LEFT JOIN 
            `{{project_id}}.normalized_state.state_person_external_id` pei
        ON
            i.OffenderID = pei.external_id
        AND
            pei.state_code = 'US_TN'
        LEFT JOIN (
            SELECT
                person_id,
                state_code,
                facility AS facility_id,
            FROM `{{project_id}}.sessions.compartment_sub_sessions_materialized`
            WHERE
                CURRENT_DATE('US/Pacific') BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date_exclusive')}
        ) cs
            USING(person_id, state_code)
        LEFT JOIN `{{project_id}}.reference_views.location_metadata_materialized` lm
            ON cs.facility_id = lm.location_external_id
            AND cs.state_code = lm.state_code
        WHERE IncompatibleRemovedDate IS NULL
            AND (facility_id IS NOT NULL OR IncompatibleType = 'S')
   ), 
   prea AS (
        SELECT OffenderID, 
                STRUCT(
                    screening_date AS latest_prea_screening_date,
                    COALESCE(victim_finding_level, 'missing') != COALESCE(previous_victim_finding,'missing') AS victim_finding_level_changed,
                    COALESCE(aggressor_finding_level, 'missing') != COALESCE(previous_aggressor_finding,'missing') AS aggressor_finding_level_changed,
                    victim_finding_level,
                    aggressor_finding_level
                    ) AS form_information_latest_prea_screening_results,
        FROM (
            SELECT
                OffenderID,
                CAST(ScreeningDate AS DATE) AS screening_date,
                VictimFindingLevel AS victim_finding_level,
                AggressorFindingLevel AS aggressor_finding_level,
                LAG(VictimFindingLevel) OVER(PARTITION BY OffenderID ORDER BY CAST(ScreeningDate AS DATE) ASC) AS previous_victim_finding,
                LAG(AggressorFindingLevel) OVER(PARTITION BY OffenderID ORDER BY CAST(ScreeningDate AS DATE) ASC) AS previous_aggressor_finding,
            FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.PREAScreeningResults_latest`
        )
        QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY screening_date DESC) = 1
   )
    SELECT tes.state_code,
           tes.reasons,
           pei.external_id,
           level_care.form_information_level_of_care,
           latest_classification.form_information_latest_classification_date,
           latest_classification.form_reclassification_due_date,
           latest_classification.form_information_latest_override_reason,
           latest_CAF.form_information_last_CAF_date,
           latest_CAF.form_information_last_CAF_total,
           case_notes_array.case_notes,
           q6_score_info.form_information_q6_notes,
           q7_score_info.form_information_q7_notes,
           detainers_cte.form_information_q8_notes,
           recommended_scores.* EXCEPT(person_id),
           stg.STGID AS form_information_gang_affiliation_id,
           CAST(CAST(sent.SentenceEffectiveDate AS datetime) AS DATE) AS form_information_sentence_effective_date,
           CAST(CAST(sent.ExpirationDate AS datetime) AS DATE) AS form_information_sentence_expiration_date,
           CAST(CAST(sent.FullExpirationDate AS datetime) AS DATE) AS form_information_sentence_full_expiration_date,
           CAST(CAST(sent.ReleaseEligibilityDate AS datetime) AS DATE) AS form_information_sentence_release_eligibility_date,
           CAST(CAST(sent.SafetyValveDate AS datetime) AS DATE) AS form_information_sentence_safety_valve_date,
           CASE 
            WHEN seg.SegregationType = 'PCB' THEN 'PC'
            WHEN seg.OffenderID IS NOT NULL THEN 'SEG' 
            ELSE 'GEN' 
            END AS form_information_status_at_hearing_seg,
           CASE WHEN tes.task_name = 'ANNUAL_RECLASSIFICATION_REVIEW' THEN 'ANNUAL' ELSE 'SPECIAL' END AS form_information_classification_type,
           -- if there has been a refusal more recently than the latest assessment completion date, then we want to show
           -- the contact note / date rather than the risk level
           IF(latest_vantage.latest_refusal_date IS NULL,
                  latest_vantage.RiskLevel,
                  CONCAT(latest_vantage.latest_refusal_contact, ', ', latest_vantage.latest_refusal_date)) AS form_information_latest_vantage_risk_level,
           latest_vantage.CompletedDate AS form_information_latest_vantage_completed_date,
           active_vantage_recommendations.active_recommendations AS form_information_active_recommendations,
           incompatible.incompatible_array AS form_information_incompatible_array,
           CASE WHEN ARRAY_LENGTH(incompatible.incompatible_array)>0 THEN TRUE ELSE FALSE END AS form_information_has_incompatibles,
           health.HealthRelatedClassification AS form_information_health_classification,
           prea.form_information_latest_prea_screening_results,
    FROM
        `{{project_id}}.{{task_eligibility_dataset}}.{tes_view}_materialized` tes
    INNER JOIN
        `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    USING(person_id)
    LEFT JOIN
        latest_classification
    USING(person_id)
    LEFT JOIN
        latest_CAF
    USING(person_id)
    LEFT JOIN
        recommended_scores
    USING(person_id)
    LEFT JOIN
        case_notes_array
    USING(person_id)
    LEFT JOIN
        q7_score_info
    USING(person_id)
    LEFT JOIN
        detainers_cte
    USING(person_id)
    LEFT JOIN
        q6_score_info
    USING(person_id)
    LEFT JOIN
        level_care
    USING(person_id)
    LEFT JOIN `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.STGOffender_latest` stg
        ON pei.external_id = stg.OffenderID
    LEFT JOIN `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.OffenderSentenceSummary_latest` sent
        ON pei.external_id = sent.OffenderID
    LEFT JOIN (
        SELECT *
        FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.Segregation_latest`
        WHERE ActualEndDateTime IS NULL
        QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY DATE(StartDateTime) DESC) = 1     
    ) seg
        ON pei.external_id = seg.OffenderID
    LEFT JOIN (
        SELECT OffenderID, ARRAY_AGG(STRUCT(incompatible_offender_id,
                                            incompatible_type)) AS incompatible_array
        FROM
            incompatibles
        GROUP BY 1
    ) incompatible
        ON pei.external_id = incompatible.OffenderID
    LEFT JOIN (
        SELECT OffenderID, HealthRelatedClassification 
        FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.HealthExam_latest`
        QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY CAST(ExamNumber AS INT64) DESC) = 1  
    ) health
        ON pei.external_id = health.OffenderID
    LEFT JOIN vantage latest_vantage
        ON pei.external_id = latest_vantage.OffenderID
    LEFT JOIN active_vantage_recommendations
        ON pei.external_id = active_vantage_recommendations.OffenderID
    LEFT JOIN prea
        ON pei.external_id = prea.OffenderID
    {where_clause}
    """
