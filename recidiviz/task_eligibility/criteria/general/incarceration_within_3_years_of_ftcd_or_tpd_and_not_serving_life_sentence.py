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
someone is incarcerated within 3 years of their full term completion date (FTRD)
or projected parole release date (TPD) and is not serving a life sentence.
"""
from recidiviz.task_eligibility.criteria.general import (
    incarceration_within_3_years_of_ftcd_or_tpd,
    not_serving_a_life_sentence,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateAgnosticTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = StateAgnosticTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name="INCARCERATION_WITHIN_3_YEARS_OF_FTCD_OR_TPD_AND_NOT_SERVING_LIFE_SENTENCE",
    sub_criteria_list=[
        incarceration_within_3_years_of_ftcd_or_tpd.VIEW_BUILDER,
        not_serving_a_life_sentence.VIEW_BUILDER,
    ],
    reasons_aggregate_function_override={"ineligible_offenses": "ARRAY_CONCAT_AGG"},
    allowed_duplicate_reasons_keys=[],
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
