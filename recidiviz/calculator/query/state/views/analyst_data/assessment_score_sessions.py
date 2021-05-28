# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Assessment scores with range of dates for each score"""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    STATE_BASE_DATASET,
    ANALYST_VIEWS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ASSESSMENT_SCORE_SESSIONS_VIEW_NAME = "assessment_score_sessions"

ASSESSMENT_SCORE_SESSIONS_VIEW_DESCRIPTION = (
    """Assessment scores with range of dates for each score"""
)

ASSESSMENT_SCORE_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/   
    SELECT 
        person_id,
        assessment_id,
        state_code,
        assessment_date,
        LEAD(DATE_SUB(assessment_date, INTERVAL 1 DAY)) OVER(PARTITION BY person_id ORDER BY assessment_date) AS score_end_date,
        assessment_type,
        assessment_class,
        assessment_score,
        assessment_level
    FROM 
        (
        SELECT *,
            ROW_NUMBER() OVER(PARTITION BY person_id, assessment_date ORDER BY assessment_score DESC) AS rn
        FROM `{project_id}.{base_dataset}.state_assessment` 
        WHERE assessment_date IS NOT NULL 
            AND (assessment_type = 'LSIR' OR assessment_type LIKE 'ORAS%')
        )
    WHERE rn = 1
    ORDER BY assessment_date
    """

ASSESSMENT_SCORE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=ASSESSMENT_SCORE_SESSIONS_VIEW_NAME,
    view_query_template=ASSESSMENT_SCORE_SESSIONS_QUERY_TEMPLATE,
    description=ASSESSMENT_SCORE_SESSIONS_VIEW_DESCRIPTION,
    base_dataset=STATE_BASE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ASSESSMENT_SCORE_SESSIONS_VIEW_BUILDER.build_and_print()
