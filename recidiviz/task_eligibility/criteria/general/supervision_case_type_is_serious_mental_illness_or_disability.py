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
"""Spans of time when a client's supervision case type is
'SERIOUS_MENTAL_ILLNESS_OR_DISABILITY'.
"""

from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    supervision_case_type_is_criteria_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SUPERVISION_CASE_TYPE_IS_SERIOUS_MENTAL_ILLNESS_OR_DISABILITY"

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    supervision_case_type_is_criteria_builder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        case_types=["SERIOUS_MENTAL_ILLNESS_OR_DISABILITY"],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
