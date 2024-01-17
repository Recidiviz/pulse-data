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
# ============================================================================
"""Spans of time when someone hasn't had a supervision sanction in the past 6 months
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    VIOLATIONS_FOUND_WHERE_CLAUSE,
    violations_within_time_interval_criteria_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_OR_NO_SUPERVISION_SANCTIONS_WITHIN_6_MONTHS"

_DESCRIPTION = """Spans of time when someone hasn't had a supervision sanction in the past 6 months"""

VIEW_BUILDER: TaskCriteriaBigQueryViewBuilder = (
    violations_within_time_interval_criteria_builder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        date_interval=6,
        date_part="MONTH",
        where_clause=VIOLATIONS_FOUND_WHERE_CLAUSE,
        violation_date_name_in_reason_blob="latest_sanction_date",
        display_single_violation_date=True,
        state_code=StateCode.US_OR,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
