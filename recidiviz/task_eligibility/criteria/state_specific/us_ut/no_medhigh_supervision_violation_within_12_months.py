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
# ============================================================================
"""Spans of time when someone in UT has been 3 months or more without a medium or high level
violation while on supervision.
"""
from typing import cast

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    supervision_violations_within_time_interval_criteria_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_UT_NO_MEDHIGH_SUPERVISION_VIOLATION_WITHIN_12_MONTHS"

query_dict = cast(
    dict,
    supervision_violations_within_time_interval_criteria_builder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        date_interval=12,
        date_part="MONTH",
        where_clause_addition="AND JSON_VALUE(violation_metadata, '$.sanction_level') IN ('3', '2')",
        return_view_builder=False,
    ),
)

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        reasons_fields=query_dict["reasons_fields"],
        state_code=StateCode.US_UT,
        criteria_spans_query_template=query_dict["criteria_query"],
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
