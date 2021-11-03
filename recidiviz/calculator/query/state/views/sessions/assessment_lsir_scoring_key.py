# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Static table with scoring calculations for LSI-R questions"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ASSESSMENT_LSIR_SCORING_KEY_VIEW_NAME = "assessment_lsir_scoring_key"

ASSESSMENT_LSIR_SCORING_KEY_VIEW_DESCRIPTION = (
    """Static table with scoring calculations for LSI-R questions"""
)

ASSESSMENT_LSIR_PROTECTIVE_QUESTION_LIST = [
    18,
    19,
    20,
    21,
    23,
    24,
    25,
    27,
    31,
    39,
    40,
    51,
    52,
]

ASSESSMENT_LSIR_SCORING_KEY_QUERY_TEMPLATE = """
/*{description}*/
    WITH question_types_key AS (
        SELECT 
            assessment_question, 
            IF(assessment_question IN ({lsir_protective_question_list}), 'protective', 'risk') AS question_type
        FROM 
            UNNEST(GENERATE_ARRAY(0,54)) assessment_question
    )
    SELECT 
        assessment_question,
        question_type,
        assessment_response,
        CASE 
            WHEN question_type = 'risk' THEN assessment_response 
            WHEN question_type = 'protective' THEN IF(assessment_response IN (0,1), 1, 0) 
        END AS risk_score,
        CASE WHEN question_type = 'protective' THEN assessment_response ELSE 0 END AS protective_factors_score,
        CASE 
            WHEN assessment_question <= 10 THEN 'criminal_history'
            WHEN assessment_question <= 20 THEN 'education_employment'
            WHEN assessment_question <= 22 THEN 'financial'
            WHEN assessment_question <= 26 THEN 'family_marital'
            WHEN assessment_question <= 29 THEN 'accommodation'
            WHEN assessment_question <= 31 THEN 'leisure_recreation'
            WHEN assessment_question <= 36 THEN 'companions'
            WHEN assessment_question <= 45 THEN 'alcohol_drug'
            WHEN assessment_question <= 50 THEN 'emotional_personal'
            WHEN assessment_question <= 54 THEN 'attitudes_orientation'
        END as subscale
    
    FROM question_types_key,
        UNNEST(IF(question_type = 'protective', GENERATE_ARRAY(0,3), GENERATE_ARRAY(0,1))) AS assessment_response
"""

ASSESSMENT_LSIR_SCORING_KEY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=ASSESSMENT_LSIR_SCORING_KEY_VIEW_NAME,
    view_query_template=ASSESSMENT_LSIR_SCORING_KEY_QUERY_TEMPLATE,
    description=ASSESSMENT_LSIR_SCORING_KEY_VIEW_DESCRIPTION,
    lsir_protective_question_list=",".join(
        [str(x) for x in ASSESSMENT_LSIR_PROTECTIVE_QUESTION_LIST]
    ),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ASSESSMENT_LSIR_SCORING_KEY_VIEW_BUILDER.build_and_print()
