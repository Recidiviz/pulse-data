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
"""Defines a criteria span view that shows spans of time during which someone is past
their parole/dual supervision early discharge date, computed using US_MI specific logic.
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

_CRITERIA_NAME = "US_MI_PAROLE_DUAL_SUPERVISION_PAST_EARLY_DISCHARGE_DATE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone is past their parole/dual supervision early discharge date, computed using
US_MI specific logic.
        - If serving life, has served at least four years
        - If serving under MCL 750.317, 750.520b, 750.520c, 750.520d, 750.520f, 750.529, 750.349, 750.350, 750.213, 
             750.110 (with max term of 15+ years), 750.110a (with max term of 15+ years), 
            or habitual offender with underlying offense of one of those charges, has served at least two years
"""
_REASON_QUERY = "TO_JSON(STRUCT(compartment_level_2 AS sentence_type,'9999-99-99' as eligible_date))"
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
