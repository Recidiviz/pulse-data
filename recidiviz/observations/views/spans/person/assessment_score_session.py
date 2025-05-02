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
"""Spans of time between assessment scores/levels of the same class and type."""

from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#39399): Aggregate/deduplicate in this query in some way. This query is still at
# the state-person-class-type level and doesn't aggregate or deduplicate currently (such
# that if someone had two different assessments of different types and/or classes with
# open spans, they'd have two spans coming out of this query). Alternatively, if we
# choose not to aggregate/deduplicate here, we need to make sure that metrics using
# this span view are correctly accounting for how this view is structured.
_SOURCE_DATA_QUERY_TEMPLATE = """
SELECT 
    state_code,
    person_id,
    assessment_date,
    score_end_date_exclusive,
    assessment_class,
    assessment_type,
    assessment_score,
    assessment_level,
FROM
    `{project_id}.sessions.assessment_score_sessions_materialized`
WHERE
    assessment_date IS NOT NULL
    AND assessment_class IS NOT NULL
    AND assessment_type IS NOT NULL
    AND (assessment_score IS NOT NULL OR assessment_level IS NOT NULL)
"""

VIEW_BUILDER: SpanObservationBigQueryViewBuilder = SpanObservationBigQueryViewBuilder(
    span_type=SpanType.ASSESSMENT_SCORE_SESSION,
    description=__doc__,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "assessment_class",
        "assessment_type",
        "assessment_score",
        "assessment_level",
    ],
    span_start_date_col="assessment_date",
    span_end_date_col="score_end_date_exclusive",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
