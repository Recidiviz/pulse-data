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
# ============================================================================
"""Defines a criteria span view that shows spans of time during which someone is not past
their date for a written scc review from an ADD. Residents are entitled to a written review every 2 months.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.placeholder_criteria_builders import (
    state_specific_placeholder_criteria_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_PAST_ADD_WRITTEN_REVIEW_FOR_SCC_DATE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone is not past
their date for a written scc review from an ADD. Residents are entitled to an in person review every 2 months. 
"""

_REASON_QUERY = "TO_JSON(STRUCT('9999-99-99' as scc_eligible_date, 1 as number_of_expected_reviews))"

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    state_specific_placeholder_criteria_view_builder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        reason_query=_REASON_QUERY,
        state_code=StateCode.US_MI,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
