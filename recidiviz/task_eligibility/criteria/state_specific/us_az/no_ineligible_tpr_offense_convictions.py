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
# =============================================================================
"""Shows the eligibility spans for resident in AZ who are not serving a
sentence baring their eligibility for TPR
"""
from recidiviz.task_eligibility.criteria.state_specific.us_az import (
    no_arson_conviction,
    no_dangerous_crimes_against_children_conviction,
    no_sexual_offense_conviction,
    no_violent_conviction_unless_assault_or_aggravated_assault_or_robbery_conviction,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    AndTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = AndTaskCriteriaGroup(
    criteria_name="US_AZ_NO_INELIGIBLE_TPR_OFFENSE_CONVICTIONS",
    sub_criteria_list=[
        no_sexual_offense_conviction.VIEW_BUILDER,
        no_arson_conviction.VIEW_BUILDER,
        no_dangerous_crimes_against_children_conviction.VIEW_BUILDER,
        no_violent_conviction_unless_assault_or_aggravated_assault_or_robbery_conviction.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=["ineligible_offenses"],
    reasons_aggregate_function_override={"ineligible_offenses": "ARRAY_CONCAT_AGG"},
    reasons_aggregate_function_use_ordering_clause={"ineligible_offenses"},
).as_criteria_view_builder

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
