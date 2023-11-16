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

"""Defines a criteria view that shows spans of time for
which residents have a custody level of "Medium".
"""
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_me_query_fragments import (
    cis_112_custody_level_criteria,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ME_CUSTODY_LEVEL_IS_MEDIUM"

_DESCRIPTION = """Defines a criteria view that shows spans of time where
clients have a custody level of "Medium".
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    cis_112_custody_level_criteria(
        _CRITERIA_NAME, _DESCRIPTION, custody_levels=""" "Medium" """
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
