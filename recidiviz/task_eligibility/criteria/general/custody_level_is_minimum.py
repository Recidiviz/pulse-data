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
"""This criteria view builder defines spans of time where residents are 
MINIMUM custody level as tracked by our `sessions` dataset.
"""
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    custody_or_supervision_level_criteria_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "CUSTODY_LEVEL_IS_MINIMUM"

_DESCRIPTION = """This criteria view builder defines spans of time where residents are
MINIMUM custody level as tracked by our `sessions` dataset.
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    custody_or_supervision_level_criteria_builder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        levels_lst=["MINIMUM"],
        level_in_reason_blob="custody_level AS custody_level",
        start_date_name_in_reason_blob="start_date AS custody_level_start_date",
        level_meets_criteria="TRUE",
        compartment_level_1_filter="INCARCERATION",
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
