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
"""Describes the spans of time when a client's latest Vantage (StrongR) assessment shows them scoring not High
 on all need levels.

Note that if a client has not yet had any assessments, they will not meet this criteria

Also note that this criteria is specific to StrongR assessment, which at the time of this writing was only used in
TN but could in the future be used in other states
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

_CRITERIA_NAME = "ASSESSED_NOT_HIGH_ON_STRONG_R_DOMAINS"

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = state_agnostic_placeholder_criteria_view_builder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    reasons_fields=[
        ReasonsField(
            name="assessment_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date of latest StrongR assessment",
        ),
        ReasonsField(
            name="assessment_metadata",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Metadata containing on all the needs and their levels for this assessment",
        ),
    ],
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
