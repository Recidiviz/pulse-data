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
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_criteria_state_specific_dataset,
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MI_COMPLETE_CLASSIFICATION_REVIEW_FORM_RECORD_VIEW_NAME = (
    "us_mi_complete_classification_review_form_record"
)

US_MI_COMPLETE_CLASSIFICATION_REVIEW_FORM_RECORD_DESCRIPTION = """
    View containing clients eligible for their initial classification review in Michigan
"""

US_MI_COMPLETE_CLASSIFICATION_REVIEW_FORM_RECORD_QUERY_TEMPLATE = f"""
WITH compas_recommended_preprocessed AS (
/* This CTE translates COMPAS scores to the recommended supervision level and partitions by person_id to determine
the most recent COMPAS score for each client */ 
SELECT 
    *,
    CASE 
        WHEN assessment_level_raw_text  = 'LOW/LOW' THEN 'MINIMUM'
        WHEN assessment_level_raw_text IN ('LOW/MEDIUM', 'MEDIUM/LOW', 'MEDIUM/MEDIUM', 'MEDIUM/HIGH', 'HIGH/MEDIUM', 'HIGH/LOW', 'LOW/HIGH') THEN 'MEDIUM'
        WHEN assessment_level_raw_text = 'HIGH/HIGH' THEN 'MAXIMUM'
        ELSE NULL
        END AS recommended_supervision_level 
FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` sc
WHERE state_code = "US_MI" 
QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, person_id ORDER BY assessment_date DESC, assessment_id)=1
) 

SELECT
    pei.external_id,
    tes.state_code,
    TO_JSON([STRUCT(NULL AS note_title, (CASE
    WHEN sai.meets_criteria THEN c.recommended_supervision_level
    WHEN cses.correctional_level = 'HIGH' THEN 'MAXIMUM' 
    WHEN cses.correctional_level = 'MAXIMUM' THEN 'MEDIUM' 
    WHEN cses.correctional_level = 'MEDIUM' THEN 'MINIMUM' 
    WHEN cses.correctional_level = 'MINIMUM' THEN 'TELEPHONE REPORTING' 
    ELSE NULL
    END) AS note_body, NULL as event_date, "Recommended supervision level" AS criteria)]) AS case_notes,
    reasons,
FROM `{{project_id}}.{{task_eligibility_dataset}}.all_tasks_materialized` tes
INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    ON tes.state_code = pei.state_code 
    AND tes.person_id = pei.person_id
    AND pei.id_type = "US_MI_DOC"
    AND task_name IN ("COMPLETE_INITIAL_CLASSIFICATION_REVIEW_FORM", "COMPLETE_SUBSEQUENT_CLASSIFICATION_REVIEW_FORM")
LEFT JOIN `{{project_id}}.{{criteria_dataset}}.supervision_level_is_sai_materialized` sai
    ON tes.state_code = sai.state_code
    AND tes.person_id = sai.person_id 
    AND CURRENT_DATE('US/Pacific') BETWEEN sai.start_date AND {nonnull_end_date_exclusive_clause('sai.end_date')}
#TODO(#20035) replace with supervision level raw text sessions once views agree
INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized`cses
    ON tes.state_code = cses.state_code
    AND tes.person_id = cses.person_id
    AND CURRENT_DATE('US/Pacific') BETWEEN cses.start_date AND {nonnull_end_date_exclusive_clause('cses.end_date_exclusive')}
LEFT JOIN compas_recommended_preprocessed c
    ON tes.state_code = c.state_code
    AND tes.person_id = c.person_id
WHERE CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND {nonnull_end_date_exclusive_clause('tes.end_date')}
    AND tes.is_eligible
    AND tes.state_code = "US_MI"
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
