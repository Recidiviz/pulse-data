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
for a transfer to a Community Reentry Center (CRC) as a resident worker.
For this to be true, the person must have one of the following three conditions:
    1. Tentative Parole Date (TPD) within seven (7) years OR
        Full Term Release Date (FTRD) within seven (7) years
    2. Parole Eligibility Date (PED) within seven (7) years AND
        Parole Hearing Date (PHD) within seven (7) years AND
        Full Term Release Date (FTRD) within 20 years
    3. Life sentence AND
        Tentative Parole Date (TPD) within 3 years
"""
from recidiviz.task_eligibility.criteria.general import (
    incarceration_within_3_years_of_tpd_and_life_sentence,
    incarceration_within_7_years_of_ftcd_or_tpd,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    incarceration_within_7_years_of_ped_and_phd_and_20_years_of_ftcd,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    OrTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """
Shows the spans of time during which someone in ID is eligible time-wise
for a transfer to a Community Reentry Center (CRC) as a resident worker. 
For this to be true, the person must have one of the following three conditions:
    1. Tentative Parole Date (TPD) within seven (7) years OR
        Full Term Release Date (FTRD) within seven (7) years 
    2. Parole Eligibility Date (PED) within seven (7) years AND
        Parole Hearing Date (PHD) within seven (7) years AND
        Full Term Release Date (FTRD) within 20 years
    3. Life sentence AND 
        Tentative Parole Date (TPD) within 3 years
"""

VIEW_BUILDER = OrTaskCriteriaGroup(
    criteria_name="US_IX_CRC_RESIDENT_WORKER_TIME_BASED_CRITERIA",
    sub_criteria_list=[
        incarceration_within_7_years_of_ftcd_or_tpd.VIEW_BUILDER,
        incarceration_within_7_years_of_ped_and_phd_and_20_years_of_ftcd.VIEW_BUILDER,
        incarceration_within_3_years_of_tpd_and_life_sentence.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[
        "full_term_completion_date",
        "group_projected_parole_release_date",
    ],
).as_criteria_view_builder

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
