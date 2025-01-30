# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Helper SQL fragments that import raw tables for UT
"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause

ORDER_ASSESSMENT_LEVEL_RAW_TEXT = """                CASE
                    WHEN ass.assessment_level_raw_text = 'LOW' THEN 1
                    WHEN ass.assessment_level_raw_text = 'MODERATE' THEN 2
                    WHEN ass.assessment_level_raw_text = 'HIGH' THEN 3
                    WHEN ass.assessment_level_raw_text = 'INTENSIVE' THEN 4
                    ELSE NULL 
                END"""


def assessment_scores_with_first_score_ctes(
    assessment_types_list: list,
) -> str:
    """
    Returns a string with the CTEs for the assessment scores with the first score
    after starting supervision"""

    # Convert list to a string enclosed in parentheses
    assessment_types_string = (
        "(" + ", ".join(f"'{item}'" for item in assessment_types_list) + ")"
    )
    return f""" first_assessment_during_supervision AS (
    SELECT 
        sss.state_code,
        sss.person_id,
        sss.supervision_super_session_id,
        sss.start_date,
        sss.end_date,
        ass.assessment_score AS first_assessment_score,
        ass.assessment_level AS first_assessment_level,
        ass.assessment_level_raw_text AS first_assessment_level_raw_text,
        ass.assessment_date AS first_assessment_date,
        {ORDER_ASSESSMENT_LEVEL_RAW_TEXT} AS first_assessment_level_raw_text_number,
        ass.assessment_type,
    FROM `{{project_id}}.sessions.supervision_super_sessions_materialized` sss
    INNER JOIN `{{project_id}}.sessions.assessment_score_sessions_materialized` ass
        USING(person_id, state_code)
    WHERE ass.assessment_type = {assessment_types_string}
        AND ass.assessment_class = 'RISK'
        AND ass.state_code= 'US_UT'
        AND ass.assessment_date BETWEEN sss.start_date AND {nonnull_end_date_clause('sss.end_date')}
    -- We only keep the first assessment after starting supervision
    QUALIFY ROW_NUMBER() OVER(PARTITION BY sss.state_code, sss.person_id, sss.supervision_super_session_id ORDER BY ass.assessment_date) = 1
    ),

    assessment_scores_with_first_score AS (
    SELECT 
        ass.state_code,
        ass.person_id,
        fads.supervision_super_session_id,
        ass.assessment_date AS start_date,
        ass.score_end_date_exclusive AS end_date,
        ass.assessment_score,
        ass.assessment_level,
        ass.assessment_level_raw_text,
        ass.assessment_date,
        {ORDER_ASSESSMENT_LEVEL_RAW_TEXT} AS assessment_level_raw_text_number,
        fads.first_assessment_score,
        fads.first_assessment_level,
        fads.first_assessment_level_raw_text,
        fads.first_assessment_date,
        fads.first_assessment_level_raw_text_number,
        SAFE_DIVIDE(fads.first_assessment_score - ass.assessment_score, fads.first_assessment_score)*100 AS assessment_score_percent_reduction,
    FROM first_assessment_during_supervision fads
    INNER JOIN `{{project_id}}.sessions.assessment_score_sessions_materialized` ass
        USING(person_id, state_code, assessment_type)
    WHERE ass.assessment_date BETWEEN fads.start_date AND {nonnull_end_date_clause('fads.end_date')}
        AND ass.assessment_type = {assessment_types_string}
    )
"""
