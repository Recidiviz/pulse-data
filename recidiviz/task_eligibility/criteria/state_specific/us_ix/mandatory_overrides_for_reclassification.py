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
"""
Defines a criteria span view that shows spans of time during which
someone does not have a mandatory override that would prevent them from
being reclassified to a lower custody level.
"""

from recidiviz.task_eligibility.criteria.general import (
    incarceration_not_within_3_years_of_tpd_and_life_sentence,
    not_incarceration_within_20_years_of_full_term_completion_date,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    detainers_for_reclassification,
    parole_hearing_date_greater_than_5_years_away,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.types import assert_type

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone has a mandatory override that would prevent them from
being reclassified to a lower custody level.
"""


VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = assert_type(
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
        logic_type=TaskCriteriaGroupLogicType.OR,
        criteria_name="US_IX_MANDATORY_OVERRIDES_FOR_RECLASSIFICATION",
        sub_criteria_list=[
            incarceration_not_within_3_years_of_tpd_and_life_sentence.VIEW_BUILDER,
            parole_hearing_date_greater_than_5_years_away.VIEW_BUILDER,
            not_incarceration_within_20_years_of_full_term_completion_date.VIEW_BUILDER,
            detainers_for_reclassification.VIEW_BUILDER,
        ],
        allowed_duplicate_reasons_keys=[],
        reasons_aggregate_function_override={"eligible_offenses": "ARRAY_CONCAT_AGG"},
    ),
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
