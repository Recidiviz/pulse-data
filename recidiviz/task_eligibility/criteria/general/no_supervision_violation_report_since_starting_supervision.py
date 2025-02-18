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
"""Defines a criterion span view that shows spans of time during which there have been
no supervision violations since starting supervision.
"""

from google.cloud import bigquery

from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.placeholder_criteria_builders import (
    state_agnostic_placeholder_criteria_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "NO_SUPERVISION_VIOLATION_REPORT_SINCE_STARTING_SUPERVISION"

# TODO(#37440): Replace with actual criteria. Can re-use no supervision violation report logic but need to add a
# join to supervision sessions
VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = state_agnostic_placeholder_criteria_view_builder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    reasons_fields=[
        ReasonsField(
            name="latest_violations_resulting_in_violation_reports_since_supervision",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Date(s) when the violation(s) occurred",
        ),
        ReasonsField(
            name="supervision_start_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date when the most recent period of supervision began",
        ),
    ],
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
