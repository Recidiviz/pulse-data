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
"""
Defines a criteria span view that shows spans of time during which
someone meets the date based criteria for proximity to release points.
"""
from recidiviz.task_eligibility.criteria.general import (
    incarceration_within_3_years_of_ftcd_or_tpd_and_not_serving_life_sentence,
    incarceration_within_3_years_of_tpd_and_life_sentence,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    incarceration_within_5_years_of_ftcd_and_3_years_of_phd_and_not_serving_life_sentence,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone meets the date based criteria for proximity to release points: 
    - A TPD or FTRD within three years or
    - A PHD within three years and is within five years of FTRD
    - If serving life, a TPD within three years
"""


VIEW_BUILDER = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.OR,
    criteria_name="US_IX_MEETS_DATE_BASED_CRITERIA_FOR_PROXIMITY_TO_RELEASE_POINTS",
    sub_criteria_list=[
        incarceration_within_3_years_of_ftcd_or_tpd_and_not_serving_life_sentence.VIEW_BUILDER,
        incarceration_within_5_years_of_ftcd_and_3_years_of_phd_and_not_serving_life_sentence.VIEW_BUILDER,
        incarceration_within_3_years_of_tpd_and_life_sentence.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[
        "full_term_completion_date",
        "group_projected_parole_release_date",
        "ineligible_offenses",
    ],
    reasons_aggregate_function_override={"ineligible_offenses": "ARRAY_CONCAT_AGG"},
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
