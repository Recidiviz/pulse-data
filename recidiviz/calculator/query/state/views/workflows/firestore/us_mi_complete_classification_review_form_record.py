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
"""Query for clients past their initial classification review date in Michigan"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_criteria_state_specific_dataset,
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ID_TYPE_BOOK = "US_MI_DOC_BOOK"
ID_TYPE = "US_MI_DOC"

US_MI_COMPLETE_CLASSIFICATION_REVIEW_FORM_RECORD_VIEW_NAME = (
    "us_mi_complete_classification_review_form_record"
)

US_MI_COMPLETE_CLASSIFICATION_REVIEW_FORM_RECORD_DESCRIPTION = """
    View containing clients eligible for their initial classification review in Michigan
"""

US_MI_COMPLETE_CLASSIFICATION_REVIEW_FORM_RECORD_QUERY_TEMPLATE = f"""
WITH compas_recommended_preprocessed AS (
SELECT 
    state_code,
    person_id,
    IF(assessment_level = 'MEDIUM_HIGH', 'MEDIUM', assessment_level) AS recommended_supervision_level 
FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` sc
WHERE state_code = "US_MI" 
QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, person_id ORDER BY assessment_date DESC, assessment_id)=1
),
eligible_clients AS (
    SELECT 
        tes.state_code,
        tes.person_id,
        pei.external_id, 
        tes.reasons,
        tes.is_eligible,
        tes.is_almost_eligible,
        CASE
            WHEN COALESCE(sai.meets_criteria, FALSE) THEN c.recommended_supervision_level
            WHEN cses.correctional_level = 'HIGH' THEN 'MAXIMUM' 
            WHEN cses.correctional_level = 'MAXIMUM' THEN 'MEDIUM' 
            WHEN cses.correctional_level = 'MEDIUM' THEN 'MINIMUM' 
            --only suggest TRS if is_eligible is strictly FALSE (and is_almost_eligible is also FALSE)
            --in other words, if is_eligible is NULL or TRUE or is_almost_eligible is TRUE than suggest trs
            WHEN cses.correctional_level = 'MINIMUM' AND (tr.is_eligible IS NULL OR tr.is_eligible OR tr.is_almost_eligible) 
                THEN 'TELEPHONE REPORTING' 
            ELSE NULL
        END AS metadata_recommended_supervision_level, 
    FROM (
        SELECT * FROM `{{project_id}}.{{task_eligibility_dataset}}.complete_initial_classification_review_form_materialized` 
        UNION ALL
        SELECT * FROM `{{project_id}}.{{task_eligibility_dataset}}.complete_subsequent_classification_review_form_materialized` 
    ) tes
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON tes.state_code = pei.state_code 
        AND tes.person_id = pei.person_id
        AND pei.id_type = "{ID_TYPE}"
    LEFT JOIN `{{project_id}}.{{criteria_dataset}}.supervision_or_supervision_out_of_state_level_is_sai_materialized` sai
        ON sai.person_id = tes.person_id 
        AND CURRENT_DATE('US/Pacific') BETWEEN sai.start_date AND {nonnull_end_date_exclusive_clause("sai.end_date")}
    #TODO(#20035) replace with supervision level raw text sessions once views agree
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized`cses
        ON tes.state_code = cses.state_code
        AND tes.person_id = cses.person_id
        AND CURRENT_DATE('US/Pacific') BETWEEN cses.start_date AND {nonnull_end_date_exclusive_clause("cses.end_date_exclusive")}
    LEFT JOIN compas_recommended_preprocessed c
        ON tes.state_code = c.state_code
        AND tes.person_id = c.person_id
    LEFT JOIN`{{project_id}}.{{task_eligibility_dataset}}.complete_transfer_to_telephone_reporting_request_materialized` tr
        ON tr.person_id = tes.person_id 
        AND CURRENT_DATE('US/Pacific') BETWEEN tr.start_date AND {nonnull_end_date_exclusive_clause("tr.end_date")}
    WHERE CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND {nonnull_end_date_exclusive_clause("tes.end_date")}
        AND tes.is_eligible
        AND tes.state_code = "US_MI"
    --for clients that have multiple external ids, it is necessary to dedup here 
      QUALIFY ROW_NUMBER() OVER(PARTITION BY tes.person_id ORDER BY person_external_id_id) =1
),
three_progress_notes AS (
  SELECT 
    plan_detail_id,
    STRING_AGG(
        CONCAT(STRING(note_date), ': ', progress_notes), "; "
        ORDER BY STRING(note_date), progress_notes
    ) AS last_3_progress_notes
  FROM (
      SELECT 
        DISTINCT
          app.plan_detail_id,
          app.plan_progress_id,
          DATE(app.last_update_date) AS note_date,
          CONCAT(app.notes, COALESCE(app.notes2, ''), COALESCE(app.notes3, '')) AS progress_notes,
      FROM `{{project_id}}.us_mi_raw_data_up_to_date_views.ADH_PLAN_PROGRESS_latest` app
      INNER JOIN `{{project_id}}.us_mi_raw_data.ADH_PLAN_DETAIL` apd
        USING(plan_detail_id)
      LEFT JOIN `{{project_id}}.us_mi_raw_data_up_to_date_views.ADH_PLAN_OF_SUPERVISION_latest` aps
        USING(plan_of_supervision_id)
      INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON aps.offender_booking_id = pei.external_id
          AND pei.id_type = "{ID_TYPE_BOOK}"
          AND pei.state_code = 'US_MI'
      LEFT JOIN `{{project_id}}.{{sessions_dataset}}.compartment_level_1_super_sessions_materialized` ss
        ON pei.person_id = ss.person_id 
          AND pei.state_code = ss.state_code
          AND CURRENT_DATE BETWEEN ss.start_date AND {nonnull_end_date_clause('ss.end_date')}
      WHERE app.notes IS NOT NULL
        -- Remove notes from before the current super session
        AND ss.start_date < DATE(app.last_update_date)
      -- Will only display latest 3 progress notes
      QUALIFY ROW_NUMBER() OVER(PARTITION BY app.plan_detail_id ORDER BY app.last_update_date DESC) <= 3
    )
  GROUP BY 1
),

case_notes_cte AS (
--- Get together all case_notes

    -- Recent violations (past 6 months)
    SELECT 
        ssv.person_id,
        # Violation type directly from raw data
        violation_type_raw_text AS note_title, 
        # Decision type + text directly from raw data
        IF(svrd.decision != 'INTERNAL_UNKNOWN',
            CONCAT('Decision: ', svrd.decision, ' - ', svrd.decision_raw_text),
            CONCAT('Decision: ', svrd.decision_raw_text)) AS note_body,
        ssv.violation_date AS event_date,
        "Recent violations (past 6 months)" AS criteria, 
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation`  ssv
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_type_entry` svte
        USING(supervision_violation_id)
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_response` svr
        USING(supervision_violation_id)
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_response_decision_entry` svrd
        USING(supervision_violation_response_id)
    WHERE ssv.state_code = 'US_MI'
        AND ssv.violation_date IS NOT NULL
        AND DATE_DIFF(CURRENT_DATE, ssv.violation_date, MONTH)<=6
        AND (svrd.decision IS NULL or svrd.decision != 'VIOLATION_UNFOUNDED')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ssv.person_id, ssv.supervision_violation_id ORDER BY svr.response_date DESC) =1
),

array_case_notes_for_eligible_folks AS (
    SELECT 
      external_id,
      -- Group all notes into an array within a JSON
      TO_JSON(
        ARRAY_AGG(
            STRUCT(note_title, note_body, event_date, criteria)
            ORDER BY note_title, note_body, event_date, criteria
        )
      ) AS case_notes,
    FROM case_notes_cte cn
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON cn.person_id = pei.person_id
            AND pei.id_type = "{ID_TYPE}"
            AND pei.state_code = 'US_MI'
    INNER JOIN eligible_clients
        USING(external_id)
    GROUP BY 1
)

SELECT
    ec.person_id,
    ec.external_id,
    ec.state_code,
    ec.reasons,
    ec.is_eligible,
    ec.is_almost_eligible,
    ec.metadata_recommended_supervision_level,
    cn.case_notes,
FROM eligible_clients ec
LEFT JOIN array_case_notes_for_eligible_folks cn
  USING(external_id)
"""

US_MI_COMPLETE_CLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_MI_COMPLETE_CLASSIFICATION_REVIEW_FORM_RECORD_VIEW_NAME,
    view_query_template=US_MI_COMPLETE_CLASSIFICATION_REVIEW_FORM_RECORD_QUERY_TEMPLATE,
    description=US_MI_COMPLETE_CLASSIFICATION_REVIEW_FORM_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_MI
    ),
    criteria_dataset=task_eligibility_criteria_state_specific_dataset(StateCode.US_MI),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_COMPLETE_CLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER.build_and_print()
