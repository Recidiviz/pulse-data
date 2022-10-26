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
"""Assessment scores with range of dates for each score"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ASSESSMENT_SCORE_SESSIONS_VIEW_NAME = "assessment_score_sessions"

ASSESSMENT_SCORE_SESSIONS_VIEW_DESCRIPTION = (
    """Assessment scores with range of dates for each score"""
)

ASSESSMENT_SCORE_SESSIONS_QUERY_TEMPLATE = """
    SELECT
        person_id,
        assessment_id,
        state_code,
        assessment_date,
        LEAD(DATE_SUB(assessment_date, INTERVAL 1 DAY)) OVER(PARTITION BY person_id ORDER BY assessment_date) AS score_end_date,
        assessment_type,
        assessment_class,
        assessment_score,
        assessment_level,
        assessment_level_raw_text,
        assessment_score_bucket,
    FROM
        (
        SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY person_id, assessment_date
                ORDER BY sequence_num DESC
            ) AS rn
        FROM `{project_id}.{normalized_state_dataset}.state_assessment`
        WHERE assessment_date IS NOT NULL
            AND (assessment_type IN ('LSIR','STRONG_R') OR assessment_type LIKE 'ORAS%')
        )
    WHERE rn = 1
    """

ASSESSMENT_SCORE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=ASSESSMENT_SCORE_SESSIONS_VIEW_NAME,
    view_query_template=ASSESSMENT_SCORE_SESSIONS_QUERY_TEMPLATE,
    description=ASSESSMENT_SCORE_SESSIONS_VIEW_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ASSESSMENT_SCORE_SESSIONS_VIEW_BUILDER.build_and_print()
