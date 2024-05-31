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
"""Describes spans of time during which a candidate is potentially eligible for
    special circumstances supervision due to the time they've served on supervision,
    according to the following logic:
    1. life sentence: must serve 7 years on supervision
    2. non-life sentence for violent case: must serve 5 years on supervision
    3. non-life sentence for non-violent case: must serve 3 years on supervision
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

_CRITERIA_NAME = "US_PA_MEETS_SPECIAL_CIRCUMSTANCES_CRITERIA_FOR_TIME_SERVED"

_DESCRIPTION = """Describes spans of time during which a candidate is potentially eligible for
    special circumstances supervision due to the time they've served on supervision,
    according to the following logic:
    1. life sentence: must serve 7 years on supervision
    2. non-life sentence for violent case: must serve 5 years on supervision
    3. non-life sentence for non-violent case: must serve 3 years on supervision
"""


_REASON_QUERY = """TO_JSON(STRUCT(False AS is_life_sentence,
                                  False AS is_violent_case,
                                  '9999-99-99' AS eligible_date))"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    state_specific_placeholder_criteria_view_builder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        reason_query=_REASON_QUERY,
        state_code=StateCode.US_PA,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
