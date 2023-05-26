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
"""Query for relevant metadata needed to support custody level downgrade opportunity in Tennessee
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
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
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    get_current_offenses,
)
from recidiviz.task_eligibility.utils.us_tn_query_fragments import detainers_cte
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_NAME = "us_tn_custody_level_downgrade_record"

US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_DESCRIPTION = """
    Query for relevant metadata needed to support custody level downgrade opportunity in Tennessee
    """

DISCIPLINARY_HISTORY_MONTH_LOOKBACK = "60"

US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_QUERY_TEMPLATE = f"""
    WITH current_offenses AS (
        SELECT
            person_id,
            ARRAY_AGG(offense IGNORE NULLS) AS offenses,
         FROM 
            ({get_current_offenses()})
        GROUP BY 1
    ),
    latest_classification AS (
        SELECT person_id,
              external_id,
              DATE(ClassificationDecisionDate) AS latest_classification_decision_date,
              OverrideReason AS latest_override_reason,
              RecommendedCustody AS recommended_custody_level
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
            assessment_date AS last_CAF_date,
            assessment_score AS last_CAF_total,
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
            disciplinary_date AS event_date,
            incident_type_raw_text,
            CASE WHEN injury_level != "" 
                 THEN CONCAT(incident_type_raw_text, "- Injury:", injury_level)
                 ELSE incident_type_raw_text
                 END AS note_title,
            injury_level,
            disciplinary_class AS note_body,
            disposition,
            assault_score,
        FROM
            `{{project_id}}.{{analyst_dataset}}.us_tn_disciplinaries_preprocessed` dis
        WHERE
            # In TN, we only want to count incidents where disciplinary class is not null or they have a pending disposition
            (disciplinary_class != "" OR disposition_date IS NULL)      
    ),
    -- This CTE only keeps disciplinaries with an assault, for Q1 and Q2 info
    assaultive_disciplinary_history AS (
        SELECT
            person_id,
            TO_JSON(
                ARRAY_AGG(
                    STRUCT(note_title, note_body, event_date, "ASSAULTIVE DISCIPLINARIES" AS criteria)
                ORDER BY event_date DESC)
            ) AS case_notes_assaultive_disciplinaries
        FROM
            disciplinaries
        WHERE
            assault_score IS NOT NULL
            AND event_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL {{disciplinary_history_month_lookback}} MONTH)
        GROUP BY 1                
    ),
    -- This CTE only keeps guilty disciplinaries, for Q6 and Q7 info
    guilty_disciplinaries AS (
        SELECT
            DISTINCT
            person_id,
            event_date,
            MAX(event_date) OVER(PARTITION BY person_id) AS latest_disciplinary,
            note_body,
        FROM
            disciplinaries
        -- For q7, we only want to consider guilty disciplinaries with non-missing disciplinary class
        WHERE disposition = 'GU'
             AND note_body != ""
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
                )
            ) AS form_q7_notes
        FROM 
            ( 
            SELECT *
            FROM guilty_disciplinaries
            WHERE event_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 18 MONTH)
            QUALIFY RANK() OVER(PARTITION BY person_id ORDER BY CASE WHEN note_body = 'A' THEN 1
                                                                      WHEN note_body = 'B' THEN 2
                                                                      WHEN note_body = 'C' THEN 3
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
                           detainer_misdemeanor_flag AS detainer_misdemeanor_flag
                           )
                )
            ) AS form_q8_notes
        FROM ({detainers_cte()})
        WHERE end_date IS NULL
        GROUP BY 1
    ),
    recommended_scores AS (
      -- Schedule B is only scored if Schedule A is 9 or less, so this CTE "re scores" schedule B scores if needed
      SELECT * 
        EXCEPT(form_calculated_total_score),
             CASE WHEN form_calculated_schedule_a_score > 9 
                  THEN form_calculated_schedule_a_score 
                  ELSE form_calculated_total_score
                  END AS form_calculated_total_score,
      FROM (
          SELECT person_id,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q1_score') AS INT64) AS form_q1_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q2_score') AS INT64) AS form_q2_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q3_score') AS INT64) AS form_q3_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q4_score') AS INT64) AS form_q4_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q5_score') AS INT64) AS form_q5_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q6_score') AS INT64) AS form_q6_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q7_score') AS INT64) AS form_q7_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q8_score') AS INT64) AS form_q8_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.q9_score') AS INT64) AS form_q9_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.calculated_schedule_a_score') AS INT64) AS form_calculated_schedule_a_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.calculated_schedule_b_score') AS INT64) AS form_calculated_schedule_b_score,
                 CAST(JSON_EXTRACT_SCALAR(score_metadata,'$.calculated_total_score') AS INT64) AS form_calculated_total_score,
          FROM
            `{{project_id}}.{{analyst_dataset}}.recommended_custody_level_spans_materialized`
          WHERE
            CURRENT_DATE('US/Pacific') BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date')}
            AND state_code = 'US_TN'
      )
    ),
    q6_score_info AS (
        SELECT *,
            CASE WHEN form_q6_score IN (-4, -2, -1) THEN latest_disciplinary
                 WHEN form_q6_score IN (1,4) THEN case_notes_guilty_disciplinaries_last_6_month
                 END AS form_q6_notes
        FROM
            recommended_scores
        LEFT JOIN
            guilty_disciplinary_history
        USING(person_id)
    
    ),
    level_care AS (
        SELECT
            person_id,
            SubServiceType AS level_of_care
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
    )
    SELECT tes.state_code,
           tes.reasons,
           pei.external_id,
           level_care.level_of_care,
           latest_classification.latest_classification_decision_date,
           latest_classification.latest_override_reason,
           latest_CAF.last_CAF_date,
           latest_CAF.last_CAF_total,
           current_offenses.offenses AS current_offenses,
           assaultive_disciplinary_history.case_notes_assaultive_disciplinaries,
           q6_score_info.form_q6_notes,
           most_serious_disciplinaries.form_q7_notes,
           detainers_cte.form_q8_notes,
           recommended_scores.* EXCEPT(person_id)
    FROM
        `{{project_id}}.{{task_eligibility_dataset}}.custody_level_downgrade_materialized` tes
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
    WHERE
        tes.state_code = 'US_TN'
        AND CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND {nonnull_end_date_exclusive_clause('tes.end_date')}
        AND tes.is_eligible
"""

US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_NAME,
    view_query_template=US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_QUERY_TEMPLATE,
    description=US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_TN
    ),
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    disciplinary_history_month_lookback=DISCIPLINARY_HISTORY_MONTH_LOOKBACK,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.build_and_print()
