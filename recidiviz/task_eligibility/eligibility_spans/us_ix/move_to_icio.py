# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
Shows the spans of time when someone in ID is eligible
for a move to Idaho Correctional Institution-Orofino (ICIO)
according to IDOC's Placement Matrix
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    transfer_prison_facility,
)
from recidiviz.task_eligibility.criteria.general import (
    custody_level_is_minimum_or_medium_or_close_in_protective_custody,
    is_male,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    not_in_icio,
    not_is_parole_violator,
    not_serving_a_rider_sentence,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    task_name="MOVE_TO_ICIO",
    description=__doc__,
    candidate_population_view_builder=incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        is_male.VIEW_BUILDER,
        custody_level_is_minimum_or_medium_or_close_in_protective_custody.VIEW_BUILDER,
        not_in_icio.VIEW_BUILDER,
        not_serving_a_rider_sentence.VIEW_BUILDER,
        not_is_parole_violator.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_prison_facility.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
