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
if someone is serving a life sentence, their tentative parole date is
not within 3 years.
"""

from recidiviz.task_eligibility.criteria.general import serving_a_life_sentence
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    not_tentative_parole_date_within_3_years,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    AndTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
if someone is serving a life sentence, their tentative parole date is 
not within 3 years.
"""


VIEW_BUILDER = AndTaskCriteriaGroup(
    criteria_name="US_IX_SERVING_A_LIFE_SENTENCE_AND_NOT_TPD_WITHIN_3_YEARS",
    sub_criteria_list=[
        serving_a_life_sentence.VIEW_BUILDER,
        not_tentative_parole_date_within_3_years.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[],
).as_criteria_view_builder

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
