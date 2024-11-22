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
"""Defines a criterion span view that shows spans of time during which there have been
no supervision violations resulting in violation reports within the past 2 years (based
on violation date, not report date).
"""

from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    violations_within_time_interval_criteria_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "NO_SUPERVISION_VIOLATION_REPORT_WITHIN_2_YEARS"

_WHERE_CLAUSE = """
/* Only keep 'VIOLATION_REPORT' responses. Note that this will exclude responses with
missing `response_type` values, as well as responses with other types from the
StateSupervisionViolationResponseType enum (such as 'CITATION' and
'PERMANENT_DECISION'). */
WHERE response_type='VIOLATION_REPORT'
"""

# TODO(#34676): Update criterion as needed when fixing the general criterion builder
# `violations_within_time_interval_criteria_builder()`.
# Currently, this general criterion builder can create incorrect reasons blobs with
# duplicate violation dates (in cases where there are multiple responses per violation).
# Therefore, in this criterion, we currently choose just the most recent violation to
# display in the reasons blob (via `display_single_violation_date=True`), and the
# relevant reasons field is named to indicate that this is just the most recent one.
# More generally, we may decide to rework this criterion more generally to fit in with
# other sanctions-shaped criteria; however, for now, we'll just use this existing
# criterion builder.
# NB: criterion currently uses *violation dates* to assess eligibility, not *response
# dates*. This is intended to prevent clients from being penalized by delays in the
# submission of violation reports.
VIEW_BUILDER: TaskCriteriaBigQueryViewBuilder = violations_within_time_interval_criteria_builder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    date_interval=2,
    date_part="YEAR",
    violation_date_name_in_reason_blob="latest_violation_resulting_in_violation_report",
    display_single_violation_date=True,
    where_clause=_WHERE_CLAUSE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
