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
for a transfer to a Community Reentry Center (CRC) as a resident worker.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_ix import (
    transfer_to_reentry_center,
)
from recidiviz.task_eligibility.criteria.general import (
    custody_level_is_minimum,
    no_absconsion_within_10_years,
    not_serving_for_sexual_offense,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    no_detainers_for_crc,
    no_eluding_police_offense_within_10_years,
    no_escape_offense_within_10_years,
    not_in_crc_facility,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """
Shows the spans of time during which someone in ID is eligible
for a transfer to a Community Reentry Center (CRC) as a resident worker.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    task_name="TRANSFER_TO_CRC_RESIDENT_WORKER_REQUEST",
    description=_DESCRIPTION,
    candidate_population_view_builder=incarceration_population.VIEW_BUILDER,
    # TODO(#22758) add the rest of the criteria
    criteria_spans_view_builders=[
        custody_level_is_minimum.VIEW_BUILDER,
        no_detainers_for_crc.VIEW_BUILDER,
        # TODO(#22759) need to hydrate sex offense
        not_serving_for_sexual_offense.VIEW_BUILDER,
        no_escape_offense_within_10_years.VIEW_BUILDER,
        no_absconsion_within_10_years.VIEW_BUILDER,
        no_eluding_police_offense_within_10_years.VIEW_BUILDER,
        # Resident worker time-based criteria (16)
        not_in_crc_facility.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_reentry_center.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
