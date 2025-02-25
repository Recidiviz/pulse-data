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
"""Defines a criteria span view that shows spans of time during which
someone on supervision in California had employment for between 12 and 14 months.
"""

from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_ca_query_fragments import (
    employment_milestone_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_CA_EMPLOYMENT_FOR_12_TO_14_MONTHS"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone on supervision in California had employment for between 12 and 14 months.
"""


VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    employment_milestone_builder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        from_x_months=12,
        to_x_months=14,
    )
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
