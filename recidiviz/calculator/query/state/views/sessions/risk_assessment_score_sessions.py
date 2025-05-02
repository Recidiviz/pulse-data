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
"""Sessionized view of risk-assessment scores that have been deduplicated/prioritized to
reflect an "overall" or "primary" assessment score for an individual at a given time.
Spans are non-overlapping by state-person.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

RISK_ASSESSMENT_SCORE_SESSIONS_VIEW_NAME = "risk_assessment_score_sessions"

# TODO(#39399): Revisit and potentially revise how we determine an overall risk
# score/level here. Is there a way we could do this more easily upstream (which may
# perhaps require a schema change), or do we still need to do it here? The logic for the
# deduplication/prioritization of scores might end up needing to differ across states,
# which would make a schema change even more attractive so that we don't have to do all
# the state-specific tweaks here.
RISK_ASSESSMENT_SCORE_SESSIONS_QUERY_TEMPLATE = """
    WITH risk_assessments AS (
        SELECT
            state_code,
            person_id,
            assessment_date,
            assessment_type,
            assessment_class,
            assessment_score,
            assessment_level,
            assessment_level_raw_text,
            assessment_score_bucket,
            assessment_metadata,
        FROM `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized`
        WHERE assessment_class = 'RISK'
    ),
    prioritized_risk_assessments AS (
        SELECT
            ra.*,
        FROM risk_assessments ra
        LEFT JOIN `{project_id}.{sessions_dataset}.assessment_level_dedup_priority` p
            ON ra.assessment_level = p.assessment_level
        /* If someone has multiple assessments on the same day, we deduplicate by
        keeping the assessment with the highest score (or the highest level, if we can't
        deduplicate based on scores). */
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY state_code, person_id, assessment_date
            ORDER BY
                assessment_score DESC,
                COALESCE(assessment_level_priority, 999)
        ) = 1
    )
    /* Create risk-score/risk-level sessions. We create non-overlapping sessions such
    that if a person has a previous risk assessment of one type, that session will end
    when a subsequent risk assessment is completed, even if the subsequent assessment is
    of a different type (as long as it's still in the 'RISK' class). */
    SELECT
        state_code,
        person_id,
        assessment_date,
        LEAD(assessment_date) OVER state_person_window AS score_end_date_exclusive,
        LEAD(DATE_SUB(assessment_date, INTERVAL 1 DAY)) OVER state_person_window AS score_end_date,
        assessment_type,
        assessment_class,
        assessment_score,
        assessment_level,
        assessment_level_raw_text,
        assessment_score_bucket,
        assessment_metadata,
    FROM prioritized_risk_assessments
    WINDOW state_person_window AS (
        PARTITION BY state_code, person_id
        ORDER BY assessment_date
    )
"""

RISK_ASSESSMENT_SCORE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=RISK_ASSESSMENT_SCORE_SESSIONS_VIEW_NAME,
    view_query_template=RISK_ASSESSMENT_SCORE_SESSIONS_QUERY_TEMPLATE,
    description=__doc__,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        RISK_ASSESSMENT_SCORE_SESSIONS_VIEW_BUILDER.build_and_print()
