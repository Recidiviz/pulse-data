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
"""Assessment scores with range of dates for each score"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_ASSESSMENT_TYPES = ["CAF", "ORAS_PRISON_INTAKE"]

ASSESSMENT_SCORE_SESSIONS_VIEW_NAME = "assessment_score_sessions"

ASSESSMENT_SCORE_SESSIONS_VIEW_DESCRIPTION = (
    "Assessment scores with range of dates for each score. Spans are non-overlapping "
    "by person-assessment_type. Note that for ORAS scores, this non-overlapping "
    "condition holds across all ORAS assessment types as if they are one common "
    "assessment, which may lead to unexpected assessment frequency or score change "
    "metrics if calculated from this table."
)

ASSESSMENT_SCORE_SESSIONS_QUERY_TEMPLATE = """
WITH state_assessment AS (
    SELECT 
        person_id,
        assessment_id,
        state_code,
        assessment_date,
        assessment_type,
        CASE
            WHEN assessment_type LIKE "ORAS%" THEN "ORAS"
            ELSE assessment_type END AS assessment_type_short,
        -- Flags assessments that are only used in facilities, not supervision. Some instruments,
        -- e.g. LSIR can be used in both supervision and incarceration settings
        assessment_type IN ('{incarceration_assessment}') AS is_incarceration_only_assessment_type,
        assessment_class,
        assessment_score,
        assessment_level,
        assessment_level_raw_text,
        assessment_score_bucket,
        assessment_metadata,
        sequence_num
    FROM
        `{project_id}.{normalized_state_dataset}.state_assessment`
    WHERE
        -- keep only relevant assessment types
        (
            -- for US_MI, identify COMPAS scales that determine risk level
            (
                state_code = "US_MI" 
                AND assessment_class_raw_text IN ("7/8", "72/73", "8042/8043", "8138/8139")
            )
            -- all other states besides MI
            OR (
                state_code != "US_MI"
                AND (
                    assessment_type IN ("LSIR", "STRONG_R", "CAF", "CSRA", "ACCAT")
                    OR assessment_type LIKE "ORAS%"
                )
            )
            -- TODO(#35297) In AZ, we care about mental health assessments
            OR (
                state_code = 'US_AZ' 
                AND assessment_type = 'INTERNAL_UNKNOWN' 
                AND assessment_class = 'MENTAL_HEALTH'
                )
        )
        AND assessment_date IS NOT NULL
    -- keep only assessments with scores or levels
        AND (
            assessment_score IS NOT NULL
            OR assessment_level IS NOT NULL
        )
    -- Keep the latest assessment per person-assessment_type per day
    QUALIFY
        ROW_NUMBER() OVER(
            PARTITION BY person_id, assessment_type_short, assessment_date
            ORDER BY sequence_num DESC
        ) = 1
    )

SELECT
    person_id,
    assessment_id,
    state_code,
    assessment_date,
    -- Use a person's subsequent assessment date for a given assessment type as the session end date.
    -- Combines all ORAS assessment types into a single category.
    LEAD(assessment_date) OVER(
        PARTITION BY person_id, assessment_type_short ORDER BY assessment_date ASC
    ) AS score_end_date_exclusive,
    LEAD(DATE_SUB(assessment_date, INTERVAL 1 DAY)) OVER(
        PARTITION BY person_id, assessment_type_short ORDER BY assessment_date ASC
    ) AS score_end_date,
    assessment_type,
    is_incarceration_only_assessment_type,
    assessment_class,
    assessment_score,
    assessment_level,
    assessment_level_raw_text,
    assessment_score_bucket,
    assessment_metadata,
FROM
    state_assessment
"""

ASSESSMENT_SCORE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=ASSESSMENT_SCORE_SESSIONS_VIEW_NAME,
    view_query_template=ASSESSMENT_SCORE_SESSIONS_QUERY_TEMPLATE,
    description=ASSESSMENT_SCORE_SESSIONS_VIEW_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    clustering_fields=["state_code", "person_id"],
    incarceration_assessment="', '".join(INCARCERATION_ASSESSMENT_TYPES),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ASSESSMENT_SCORE_SESSIONS_VIEW_BUILDER.build_and_print()
