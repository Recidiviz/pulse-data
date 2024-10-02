# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""View with risk assessments"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Risk assessments"

_SOURCE_DATA_QUERY_TEMPLATE = f"""
SELECT
    state_code,
    person_id,
    assessment_type,
    assessment_date,
    assessment_score,
    assessment_score_change,
    IFNULL(assessment_score_change, NULL) > 0 AS assessment_score_increase,
    IFNULL(assessment_score_change, NULL) < 0 AS assessment_score_decrease,
FROM (
    SELECT
        a.state_code,
        a.person_id,
        assessment_type,
        assessment_date,
        assessment_score,
        # assessment score change within the same SSS
        assessment_score - LAG(assessment_score) OVER (PARTITION BY
            a.state_code, a.person_id, assessment_type, sss.start_date
            ORDER BY assessment_date
        ) AS assessment_score_change,
    FROM
        `{{project_id}}.sessions.assessment_score_sessions_materialized` a
    LEFT JOIN
        `{{project_id}}.sessions.supervision_super_sessions_materialized` sss
    ON
        a.state_code = sss.state_code
        AND a.person_id = sss.person_id
        AND a.assessment_date BETWEEN sss.start_date AND {nonnull_end_date_exclusive_clause("sss.end_date_exclusive")}
    WHERE
        assessment_score IS NOT NULL
        AND assessment_type IS NOT NULL
)
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.RISK_SCORE_ASSESSMENT,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "assessment_type",
        "assessment_score",
        "assessment_score_change",
        "assessment_score_increase",
        "assessment_score_decrease",
    ],
    event_date_col="assessment_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
