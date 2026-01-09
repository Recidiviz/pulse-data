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
# ============================================================================
"""Describes spans of time during which an ORAS supervision assessment is completed and active."""

from google.cloud import bigquery

from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "ORAS_COMMUNITY_SUPERVISION_COMPLETED"

_QUERY_TEMPLATE = """
  SELECT
    state_code,
    person_id,
    assessment_date AS start_date,
    score_end_date AS end_date,
    TRUE AS meets_criteria,
    TO_JSON(STRUCT(
            TRUE AS is_oras_active,
            assessment_date AS assessment_date
        )) AS reason,
    TRUE AS is_oras_active,
    assessment_date AS assessment_date,
  FROM
    `{project_id}.sessions.assessment_score_sessions_materialized`
  WHERE
    assessment_type = 'ORAS_COMMUNITY_SUPERVISION'
    AND assessment_class = 'RISK'
    AND assessment_date IS DISTINCT FROM score_end_date
"""

_REASONS_FIELDS = [
    ReasonsField(
        name="is_oras_active",
        type=bigquery.enums.StandardSqlTypeNames.BOOL,
        description="Binary Response indicator for if a resident has an active ORAS assessment",
    ),
    ReasonsField(
        name="assessment_date",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="Date when the assessment was implemented",
    ),
]

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=_REASONS_FIELDS,
        meets_criteria_default=False,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
