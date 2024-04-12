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
"""Defines a criteria span view that shows spans of time during which someone's supervision level is minimum
low or if their supervision level is minimum in person, their initial assessment score was medium/minimum.
"""

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = (
    "US_MI_SUPERVISION_AND_ASSESSMENT_LEVEL_ELIGIBLE_FOR_TELEPHONE_REPORTING"
)

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone's supervision level is
minimum low OR
minimum in person w/ an initial assessment score of medium or minimum"""

_QUERY_TEMPLATE = f"""
WITH supervision_level_sessions_with_assessments AS (
/* This cte sets the initial assessment level value for a continuous period of 
supervision */
  SELECT DISTINCT
    sls.state_code,
    sls.person_id,
    sls.start_date,
    sls.end_date_exclusive AS end_date,
    sls.date_gap_id,
    sls.supervision_level_raw_text,
    FIRST_VALUE(assessment_level) 
            OVER(PARTITION BY sls.person_id, sls.date_gap_id ORDER BY sap.assessment_date) 
                AS initial_assessment_level,
  FROM `{{project_id}}.{{sessions_dataset}}.supervision_level_raw_text_sessions_materialized` sls
  LEFT JOIN `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` sap
    ON sls.state_code = sap.state_code
    AND sls.person_id = sap.person_id 
    AND sap.assessment_date < {nonnull_end_date_clause('sls.end_date_exclusive')}
    AND sls.start_date < {nonnull_end_date_clause('sap.score_end_date_exclusive')}
  WHERE sls.state_code = "US_MI"
),
eligibility_spans AS (
/* This CTE identifies where the client has an 
eligible combination of supervision and initial assessment level */
  SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    COALESCE((COALESCE(omni_map.is_minimum_low, coms_map.is_minimum_low) OR
        (COALESCE(omni_map.is_minimum_in_person, coms_map.is_minimum_in_person) AND initial_assessment_level IN ('MEDIUM', 'MINIMUM'))), FALSE) AS meets_criteria,
    COALESCE(omni_map.description, coms_map.description) as description,
    initial_assessment_level
  FROM supervision_level_sessions_with_assessments sl
  /* Using regex to match digits in sl.supervision_level_raw_text
   so imputed values with the form d*##IMPUTED are correctly joined */
  LEFT JOIN `{{project_id}}.{{analyst_data_dataset}}.us_mi_supervision_level_raw_text_mappings` omni_map
  ON REGEXP_EXTRACT(sl.supervision_level_raw_text, r'(\\d+)') = \
  omni_map.supervision_level_raw_text AND omni_map.source  = 'OMNI'
  LEFT JOIN `{{project_id}}.{{analyst_data_dataset}}.us_mi_supervision_level_raw_text_mappings` coms_map
  ON REPLACE(sl.supervision_level_raw_text, "##IMPUTED", "") = coms_map.supervision_level_raw_text AND coms_map.source = 'COMS'
)
SELECT * EXCEPT (description, initial_assessment_level),
    TO_JSON(STRUCT(description AS supervision_level_raw_text, 
                    COALESCE(initial_assessment_level, "Not needed") AS initial_assessment_level)) AS reason,
FROM eligibility_spans
"""


VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_MI,
        sessions_dataset=SESSIONS_DATASET,
        analyst_data_dataset=ANALYST_VIEWS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
