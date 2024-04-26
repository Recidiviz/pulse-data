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
# =============================================================================
"""
Shows the spans of time during which someone in ID is eligible
for a transfer to the Expanded Community Reentry Center program.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    release_to_community_confinement_supervision,
)
from recidiviz.task_eligibility.criteria.general import (
    custody_level_is_minimum,
    not_serving_for_sexual_offense,
    not_serving_for_violent_offense,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    in_crc_facility_or_pwcc_unit_1,
    in_crc_facility_or_pwcc_unit_1_for_60_days,
    incarceration_within_6_months_of_ftcd_or_ped_or_tpd,
    no_absconsion_escape_and_eluding_police_offenses_within_10_years,
    no_class_a_or_b_dor_for_6_months,
    no_detainers_for_xcrc_and_crc,
    no_sex_offender_alert,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """
Shows the spans of time during which someone in ID is eligible
for a transfer to the Expanded Community Reentry Center program.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    task_name="TRANSFER_TO_XCRC_REQUEST",
    description=_DESCRIPTION,
    candidate_population_view_builder=incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        in_crc_facility_or_pwcc_unit_1.VIEW_BUILDER,
        in_crc_facility_or_pwcc_unit_1_for_60_days.VIEW_BUILDER,
        incarceration_within_6_months_of_ftcd_or_ped_or_tpd.VIEW_BUILDER,
        custody_level_is_minimum.VIEW_BUILDER,
        no_class_a_or_b_dor_for_6_months.VIEW_BUILDER,
        no_detainers_for_xcrc_and_crc.VIEW_BUILDER,
        not_serving_for_sexual_offense.VIEW_BUILDER,
        no_absconsion_escape_and_eluding_police_offenses_within_10_years.VIEW_BUILDER,
        no_sex_offender_alert.VIEW_BUILDER,
        not_serving_for_violent_offense.VIEW_BUILDER,
    ],
    completion_event_builder=release_to_community_confinement_supervision.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
