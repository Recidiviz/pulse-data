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
"""Selects all spans of time in which a person is a candidate for TRAS risk assessment
in Texas. This includes people who meet the following criteria:
- Meets risk assessment applicable conditions
- Not under supervision within 6 months of release date
- Not critical understaffing TRAS exempt
"""

from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    meets_risk_assessment_applicable_conditions,
    not_critical_understaffing_tras_exempt,
    not_supervision_within_6_months_of_release_date,
)
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "US_TX_TRAS_ELIGIBLE_POPULATION"

_CRITERIA_GROUP = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name=_POPULATION_NAME,
    sub_criteria_list=[
        meets_risk_assessment_applicable_conditions.VIEW_BUILDER,
        not_supervision_within_6_months_of_release_date.VIEW_BUILDER,
        not_critical_understaffing_tras_exempt.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[
        "case_type",
        "supervision_level",
    ],
)

VIEW_BUILDER: StateSpecificTaskCandidatePopulationBigQueryViewBuilder = (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder.from_criteria_group(
        criteria_group=_CRITERIA_GROUP, population_name=_POPULATION_NAME
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
