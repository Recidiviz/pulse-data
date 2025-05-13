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
"""
Shows the spans of time during which someone in ID is eligible time-wise
for a transfer to a Community Reentry Center (CRC) for work-release.
For this to be true, the person must have one of the following three conditions:
    1. Tentative Parole Date (TPD) within eighteen (18) months OR
        Full Term Release Date (FTRD) within eighteen (18) months
    2. Early Release Date (EPRD) within 18 months AND
        Full Term Release Date (FTRD) within 15 years
    3. Life sentence AND
        Tentative Parole Date (TPD) within 1 year
"""


from recidiviz.task_eligibility.criteria.general import (
    incarceration_within_1_year_of_upcoming_tpd_and_life_sentence,
    incarceration_within_18_months_of_ftcd_or_upcoming_tpd,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    incarceration_within_18_months_of_eprd_and_15_years_of_ftcd,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    OrTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.types import assert_type

_DESCRIPTION = """
Shows the spans of time during which someone in ID is eligible time-wise
for a transfer to a Community Reentry Center (CRC) for work-release. 
For this to be true, the person must have one of the following three conditions:
    1. Tentative Parole Date (TPD) within 18 months OR
        Full Term Release Date (FTRD) within 18 months 
    2. Early Release Date (EPRD) within 18 months AND
        Full Term Release Date (FTRD) within 15 years
    3. Life sentence AND
        Tentative Parole Date (TPD) within 1 year
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = assert_type(
    OrTaskCriteriaGroup(
        criteria_name="US_IX_CRC_WORK_RELEASE_TIME_BASED_CRITERIA",
        sub_criteria_list=[
            incarceration_within_18_months_of_ftcd_or_upcoming_tpd.VIEW_BUILDER,
            incarceration_within_18_months_of_eprd_and_15_years_of_ftcd.VIEW_BUILDER,
            incarceration_within_1_year_of_upcoming_tpd_and_life_sentence.VIEW_BUILDER,
        ],
        allowed_duplicate_reasons_keys=[
            "full_term_completion_date",
            "group_projected_parole_release_date",
        ],
    ).as_criteria_view_builder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
