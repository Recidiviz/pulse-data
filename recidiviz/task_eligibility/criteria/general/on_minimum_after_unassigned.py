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
"""Defines a criterion span view that shows spans of time during which someone is on Minimum supervision level
directly after Unassigned level.
"""

from google.cloud import bigquery

from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "ON_MINIMUM_AFTER_UNASSIGNED"

_QUERY_TEMPLATE = """
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        TRUE AS meets_criteria,
        TO_JSON(STRUCT(
            start_date AS supervision_level_start_date
        )) AS reason,
        supervision_level,
        start_date AS supervision_level_start_date
    FROM `{project_id}.sessions.supervision_level_sessions_materialized`
    WHERE supervision_level = 'MINIMUM' AND previous_supervision_level = 'UNASSIGNED'
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    meets_criteria_default=False,
    reasons_fields=[
        ReasonsField(
            name="supervision_level_start_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date when the client started minimum directly following unassigned level",
        ),
    ],
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
