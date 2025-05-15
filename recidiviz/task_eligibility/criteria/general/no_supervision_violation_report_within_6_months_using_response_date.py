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
no supervision violations resulting in violation reports within the past 6 months (based
on response date, not violation date).
"""

from typing import cast

from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    supervision_violations_within_time_interval_criteria_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "NO_SUPERVISION_VIOLATION_REPORT_WITHIN_6_MONTHS_USING_RESPONSE_DATE"

_WHERE_CLAUSE_ADDITION = """
/* Only keep 'VIOLATION_REPORT' responses. Note that this will exclude responses with
missing `response_type` values, as well as responses with other types from the
StateSupervisionViolationResponseType enum (such as 'CITATION' and
'PERMANENT_DECISION'). */
AND vr.response_type='VIOLATION_REPORT'
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = cast(
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    supervision_violations_within_time_interval_criteria_builder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        date_interval=6,
        date_part="MONTH",
        where_clause_addition=_WHERE_CLAUSE_ADDITION,
        violation_date_name_in_reason_blob="latest_violation_report_dates",
        use_response_date=True,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
