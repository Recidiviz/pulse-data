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
"""Defines a criterion span view that shows spans of time during which someone has
completed at least 60 days on 'INTAKE' supervision.
"""

from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    get_minimum_time_served_criteria_query,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "ON_INTAKE_FOR_60_DAYS"

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    get_minimum_time_served_criteria_query(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        time_served_interval="DAY",
        minimum_time_served=60,
        supervision_levels=["INTAKE"],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
