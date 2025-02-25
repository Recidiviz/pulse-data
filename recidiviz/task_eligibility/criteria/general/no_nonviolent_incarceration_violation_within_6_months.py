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
# =============================================================================
"""Defines a criteria span view that shows spans of time during which there
are no major nonviolent violations within 6 months during incarceration."""

from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    incarceration_violations_within_time_interval_criteria_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "NO_NONVIOLENT_INCARCERATION_VIOLATION_WITHIN_6_MONTHS"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which there
are no major nonviolent violations within 6 months during incarceration."""

_INCIDENT_TYPE_CLAUSE = """WHERE incident_type NOT IN ('VIOLENCE', 'MINOR_OFFENSE')"""

VIEW_BUILDER: TaskCriteriaBigQueryViewBuilder = (
    incarceration_violations_within_time_interval_criteria_builder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        date_interval=6,
        violation_date_name_in_reason_blob="latest_violations",
        incident_type=_INCIDENT_TYPE_CLAUSE,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
