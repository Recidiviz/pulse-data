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
# ============================================================================
"""Spans of time when someone does not have any filed detainers that have not been disposed, in Arkansas."""
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.placeholder_criteria_builders import (
    state_specific_placeholder_criteria_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AR_NO_FILED_BUT_UNDISPOSED_DETAINERS"

_DESCRIPTION = """Spans of time when someone does not have any filed detainers that have not been disposed, in Arkansas."""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = state_specific_placeholder_criteria_view_builder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    reasons_fields=[
        ReasonsField(
            name="filed_but_undisposed_detainers",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="List of detainers that have been filed but not disposed for this person.",
        )
    ],
    state_code=StateCode.US_AR,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
