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
Defines a criteria span view that shows spans of time during which someone is serving a life sentence
and their projected parole release date (TPD) is not within 3 years. Only TPDs greater than 3 years away
meet this requirement (TPDs in the past will not).
"""

from recidiviz.task_eligibility.criteria.general import (
    not_incarceration_within_3_years_of_projected_parole_release_date,
    serving_a_life_sentence,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateAgnosticTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = StateAgnosticTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name="INCARCERATION_NOT_WITHIN_3_YEARS_OF_TPD_AND_LIFE_SENTENCE",
    sub_criteria_list=[
        serving_a_life_sentence.VIEW_BUILDER,
        not_incarceration_within_3_years_of_projected_parole_release_date.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
