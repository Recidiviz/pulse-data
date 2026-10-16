# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Identifies when someone is within their first 4 months of continuous state prison
custody (custodial_authority = STATE_PRISON)."""

from recidiviz.task_eligibility.criteria.general import (
    in_state_prison_custody_at_least_4_months,
)
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
    sub_criteria=in_state_prison_custody_at_least_4_months.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
