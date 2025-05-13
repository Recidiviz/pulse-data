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
someone is incarcerated within 5 years of their full term completion date
and 3 years of their parole hearing date and is not serving a life sentence.
"""
from recidiviz.task_eligibility.criteria.general import (
    incarceration_within_5_years_of_full_term_completion_date,
    not_serving_a_life_sentence,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    parole_hearing_date_upcoming_within_3_years,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone is incarcerated within 5 years of their full term completion date
and 3 years of their parole hearing date and is not serving a life sentence.
"""

VIEW_BUILDER = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name="US_IX_INCARCERATION_WITHIN_5_YEARS_OF_FTCD_AND_3_YEARS_OF_PHD_AND_NOT_SERVING_LIFE_SENTENCE",
    sub_criteria_list=[
        incarceration_within_5_years_of_full_term_completion_date.VIEW_BUILDER,
        parole_hearing_date_upcoming_within_3_years.VIEW_BUILDER,
        not_serving_a_life_sentence.VIEW_BUILDER,
    ],
    reasons_aggregate_function_override={"ineligible_offenses": "ARRAY_CONCAT_AGG"},
    allowed_duplicate_reasons_keys=[],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
