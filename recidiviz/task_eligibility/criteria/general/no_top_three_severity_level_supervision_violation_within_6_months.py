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
"""Defines a criterion span view that shows spans of time during which there has not
been a supervision violation, categorized within one of the top three `violation_severity`
levels (or uncategorized), within the past 6 months on supervision.
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

_CRITERIA_NAME = "NO_TOP_THREE_SEVERITY_LEVEL_SUPERVISION_VIOLATION_WITHIN_6_MONTHS"

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = cast(
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    supervision_violations_within_time_interval_criteria_builder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        date_interval=6,
        date_part="MONTH",
        # Specifically includes high-severity violations, and those without labelled severity.
        # This is geared towards the Nebraska use case, where violations without specified
        # severity are those most likely to result in revocation.
        where_clause_addition="AND (violation_severity IS NULL OR violation_severity IN ('HIGHEST', 'SECOND_HIGHEST', 'THIRD_HIGHEST'))",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
