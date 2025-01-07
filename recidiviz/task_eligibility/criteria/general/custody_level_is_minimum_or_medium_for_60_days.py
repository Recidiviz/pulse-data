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
"""This criteria view builder defines spans of time where residents have been
MINIMUM or MEDIUM custody level for at least 60 days as tracked by our `sessions` dataset.
"""

from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    get_minimum_time_served_criteria_query,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "CUSTODY_LEVEL_IS_MINIMUM_OR_MEDIUM_FOR_60_DAYS"

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    get_minimum_time_served_criteria_query(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        time_served_interval="DAY",
        minimum_time_served=60,
        custody_levels=["MINIMUM", "MEDIUM"],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
