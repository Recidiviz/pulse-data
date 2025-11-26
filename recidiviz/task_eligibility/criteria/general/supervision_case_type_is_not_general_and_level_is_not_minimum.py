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
"""
Defines the criteria for determining if a client has a GENERAL supervision case type.
"""

from google.cloud import bigquery

from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    ReasonsField,
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SUPERVISION_CASE_TYPE_IS_NOT_GENERAL_AND_LEVEL_IS_NOT_MINIMUM"

_QUERY_TEMPLATE = """
SELECT
    ctsl.state_code,
    ctsl.person_id,
    ctsl.start_date,
    ctsl.end_date,
    NOT (ctsl.case_type = 'GENERAL' AND ctsl.supervision_level = 'MINIMUM') AS meets_criteria,
    TO_JSON(
        STRUCT(
            ctsl.case_type AS case_type,
            ctsl.supervision_level AS supervision_level
    )) AS reason,
    ctsl.case_type,
    ctsl.supervision_level,
FROM `{project_id}.tasks_views.case_type_supervision_level_spans_materialized` ctsl
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=__doc__,
        reasons_fields=[
            ReasonsField(
                name="case_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Supervision case type",
            ),
            ReasonsField(
                name="supervision_level",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Supervision level",
            ),
        ],
        meets_criteria_default=True,
    )
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
