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
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    get_sentences_current_span,
)

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


def at_least_X_time_since_latest_assessment(
    assessment_type: str,
    date_interval: int = 12,
    date_part: str = "MONTH",
) -> str:
    """
    Args:
        assessment_type (str): Type of assessment
        date_interval (int): Number of weeks, months, etc between assessments
        date_part (str): Supports any of the BigQuery date_part values:
            "DAY", "WEEK","MONTH","QUARTER","YEAR". Defaults to "MONTH".
    Returns:
        f-string: Spans of time where the criteria is NOT met, because X time has not passed between assessments
    """

    return f"""
    WITH assessment_sessions_cte AS
    (
        SELECT
            state_code,
            person_id,
            /* The logic behind the start and end date is the following - during the classification process we see a 
            few different relevant dates: CAFDate, ClassificationDate, and ClassificationDecisionDate. CAFDate is the
            date of the CAF form, and what we ingest as assessment_date. ClassificationDate is the date the
            classification was submitted, and ClassificationDecisionDate is when a decision was made. 
            
            We want the span when someone is ineligible to begin on classification_decision_date; this ensures that
            someone will continue to show up as eligible even after a form has been submitted for them, until the final
            hearing is done and a decision is made.
            
            We want the span to end 12 months after the ClassificationDate, as per what Counselors have told us they use
            to evaluate when a person needs an annual reclass.
            
            Since a person is considered "eligible" during the whole month where their assessment is due, we use a
            DATE_TRUNC() for the end date so that someone becomes eligible in the month of their assessment due date.
            
            This also improves tracking historical spans in situations when an annual assessment may be filled out
            prior to the due date, since we consider someone eligible the starting the first of the month when their
            assessment is due 
            */
            classification_decision_date AS start_date,
            DATE_SUB(DATE_TRUNC(DATE_ADD(assessment_date, INTERVAL {date_interval} {date_part}), MONTH), INTERVAL 1 WEEK) AS end_date,
            FALSE AS meets_criteria,
            assessment_date,
        FROM
            (
                SELECT asmt.* EXCEPT(assessment_date),
                        classification_decision_date,
                        classification_decision,
                        COALESCE(
                            DATE(NULLIF(JSON_EXTRACT_SCALAR(assessment_metadata,"$.CLASSIFICATIONDATE"),"")),
                            assessment_date
                        ) AS assessment_date,                        
                FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` asmt
                LEFT JOIN (
                    SELECT person_id, 
                            DATE(CAFDate) AS caf_date, 
                            -- TODO(#24737): Pull from metadata once ClassificationDecision is ingested
                            ClassificationDecision AS classification_decision,
                            -- TODO(#23526): Pull classification decision date from metadata once ingested and entity deletion issues resolved
                            DATE(ClassificationDecisionDate) AS classification_decision_date,
                    FROM
                        `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.Classification_latest` classification
                    INNER JOIN
                        `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
                    ON
                        classification.OffenderID = pei.external_id
                    AND
                        pei.state_code = 'US_TN'
                    QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderId, CAFDate ORDER BY ClassificationSequenceNumber DESC) = 1
                ) caf_raw
                    ON asmt.person_id = caf_raw.person_id
                    AND asmt.assessment_date = caf_raw.caf_date
            )
        WHERE
            assessment_type = "{assessment_type}"
            -- Removes a small number of spans where ClassificationDecisionDate appears to be 1 year after the ClassficationDate
            AND DATE_SUB(DATE_TRUNC(DATE_ADD(assessment_date, INTERVAL {date_interval} {date_part}), MONTH), INTERVAL 1 WEEK) > classification_decision_date
            AND COALESCE(classification_decision,'A') = 'A'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, assessment_type, assessment_date ORDER BY assessment_score DESC) = 1
    )
    ,
    {create_sub_sessions_with_attributes('assessment_sessions_cte')}
    ,
    dedup_cte AS
    (
        SELECT
            *,
        FROM sub_sessions_with_attributes
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date 
            ORDER BY assessment_date DESC) = 1
    )
    ,
    sessionized_cte AS 
    /*
    Sessionize so that we have continuous periods of time for which a person is eligible. A
    new session exists either when a person becomes eligible, or if a person has an additional assessment in the 
    specified date interval, which changes the "assessment_date" value.
    */
    (
    {aggregate_adjacent_spans(table_name='dedup_cte',
                       attribute=['assessment_date','meets_criteria'],
                       end_date_field_name='end_date')}
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(assessment_date AS most_recent_assessment_date,
                        DATE_ADD(assessment_date, INTERVAL 12 MONTH) AS assessment_due_date
                        )
                ) AS reason
    FROM sessionized_cte
    """


def us_tn_classification_forms(
    tes_view: str,
    where_clause: str,
    disciplinary_history_month_lookback: str = DISCIPLINARY_HISTORY_MONTH_LOOKBACK,
) -> str:
    return f"""
    WITH current_offenses AS (
        SELECT
            person_id,
            ARRAY_AGG(offense IGNORE NULLS) AS form_information_current_offenses,
         FROM 
            ({get_sentences_current_span(in_projected_completion_array=True)})
        GROUP BY 1
    ),
    latest_classification AS (
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
            incident_type_raw_text,
            CASE WHEN injury_level != "" 
                 THEN CONCAT(incident_type_raw_text, "- Injury:", injury_level)
                 ELSE incident_type_raw_text
                 END AS note_title,
            injury_level,
            incident_class,
            CONCAT('Class ', 
                    incident_class,
                    ' Incident Code:',
                    incident_type_raw_text,
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
    -- This CTE only keeps disciplinaries with an assault, for Q1 and Q2 info
    assaultive_disciplinary_history AS (
        SELECT
            person_id,
            TO_JSON(
                ARRAY_AGG(
                    STRUCT(note_title, note_body, event_date, "ASSAULTIVE DISCIPLINARIES" AS criteria)
                ORDER BY event_date DESC)
            ) AS case_notes
        FROM
            disciplinaries
        WHERE
            assault_score IS NOT NULL
            AND event_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL {disciplinary_history_month_lookback} MONTH)
        GROUP BY 1                
    ),
    -- This CTE only keeps guilty disciplinaries, for Q6 and Q7 info
    guilty_disciplinaries AS (
        SELECT
            DISTINCT
            person_id,
            event_date,
            incident_class,
            MAX(event_date) OVER(PARTITION BY person_id) AS latest_disciplinary,
            note_body,
        FROM
            disciplinaries
        -- For q7, we only want to consider guilty disciplinaries with non-missing disciplinary class
        WHERE disposition = 'GU'
             AND note_body != ""
             AND incident_details NOT LIKE "%VERBAL WARNING%"
    ),
    guilty_disciplinary_history AS (
        SELECT
            person_id,
            TO_JSON(
                ARRAY_AGG(
                    CASE WHEN event_date = latest_disciplinary
                    THEN STRUCT(note_body, event_date) 
                    END
                IGNORE NULLS) 
            ) AS latest_disciplinary,
            TO_JSON(
                ARRAY_AGG(
                    CASE WHEN event_date > DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 6 MONTH) 
                    THEN STRUCT(note_body, event_date)
                    END
                IGNORE NULLS
                ORDER BY event_date DESC)
            ) AS case_notes_guilty_disciplinaries_last_6_month,
        FROM
            guilty_disciplinaries
        GROUP BY 1
    ), 
    most_serious_disciplinaries AS (
        SELECT
            person_id,
            TO_JSON(
                ARRAY_AGG(
                    STRUCT(event_date, note_body)
                    ORDER BY event_date DESC
                )
            
            ) AS form_information_q7_notes
        FROM 
            ( 
            SELECT *
            FROM guilty_disciplinaries
            WHERE event_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 18 MONTH)
            QUALIFY RANK() OVER(PARTITION BY person_id ORDER BY CASE WHEN incident_class = 'A' THEN 1
                                                                      WHEN incident_class = 'B' THEN 2
                                                                      WHEN incident_class = 'C' THEN 3
                                                                      END ASC) = 1
            )
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
        SELECT *,
            CASE WHEN form_information_q6_score IN (-4, -2, -1) THEN latest_disciplinary
                 WHEN form_information_q6_score IN (1,4) THEN case_notes_guilty_disciplinaries_last_6_month
                 END AS form_information_q6_notes
        FROM
            recommended_scores
        LEFT JOIN
            guilty_disciplinary_history
        USING(person_id)
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
            ROW_NUMBER() OVER(PARTITION BY a.OffenderID ORDER BY DATE(a.CompletedDate) DESC) AS latest_row,
            r.Pathway,
            p.PathwayName,
            r.Recommendation,
            r.TreatmentGoal,
            DATE(r.RecommendationDate) AS RecommendationDate,
            DATE(r.RecommendedEndDate) AS RecommendedEndDate,
            pr.VantagePointTitle
        FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.VantagePointAssessments_latest` a
        LEFT JOIN `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.VantagePointRecommendations_latest` r
          ON a.OffenderID = r.OffenderID
          AND a.AssessmentID = r.AssessmentID
          AND CURRENT_DATE BETWEEN DATE(RecommendationDate) AND COALESCE(DATE(RecommendedEndDate), '9999-01-01')
        LEFT JOIN `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.VantagePointPathways_latest` p
          USING(Recommendation, Pathway)
        LEFT JOIN `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.VantagePointProgram_latest` pr
          ON pr.VantageProgramID = r.VantagePointProgamID
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
        WHERE Recommendation IS NOT NULL
        GROUP BY 1
    
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
           current_offenses.form_information_current_offenses,
           assaultive_disciplinary_history.case_notes,
           q6_score_info.form_information_q6_notes,
           most_serious_disciplinaries.form_information_q7_notes,
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
           latest_vantage.RiskLevel AS form_information_latest_vantage_risk_level,
           latest_vantage.CompletedDate AS form_information_latest_vantage_completed_date,
           active_vantage_recommendations.active_recommendations AS form_information_active_recommendations,
           incompatible.incompatible_array AS form_information_incompatible_array,
           CASE WHEN ARRAY_LENGTH(incompatible.incompatible_array)>0 THEN TRUE ELSE FALSE END AS form_information_has_incompatibles,
           health.HealthRelatedClassification AS form_information_health_classification,
    FROM
        `{{project_id}}.{{task_eligibility_dataset}}.{tes_view}_materialized` tes
    INNER JOIN
        `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    USING(person_id)
    LEFT JOIN
        current_offenses
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
        assaultive_disciplinary_history
    USING(person_id)
    LEFT JOIN
        guilty_disciplinary_history
    USING(person_id)
    LEFT JOIN
        most_serious_disciplinaries
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
        SELECT OffenderID, ARRAY_AGG(STRUCT(incompatible_ids AS incompatible_offender_id,
                                            IncompatibleType AS incompatible_type)) AS incompatible_array
        FROM (
            SELECT DISTINCT
                    OffenderID,
                    CASE WHEN IncompatibleType = 'S' THEN 'STAFF'
                         ELSE IncompatibleOffenderID
                         END AS incompatible_ids,
                    IncompatibleType,
            FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.IncompatiblePair_latest`
            WHERE IncompatibleRemovedDate IS NULL
            )
        GROUP BY 1
    ) incompatible
        ON pei.external_id = incompatible.OffenderID
    LEFT JOIN (
        SELECT OffenderID, HealthRelatedClassification 
        FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.HealthExam_latest`
        QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY CAST(ExamNumber AS INT) DESC) = 1  
    ) health
        ON pei.external_id = health.OffenderID
    LEFT JOIN vantage latest_vantage
        ON pei.external_id = latest_vantage.OffenderID
        AND latest_vantage.latest_row = 1
    LEFT JOIN active_vantage_recommendations
        ON pei.external_id = active_vantage_recommendations.OffenderID
    {where_clause}
    """
