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
# =============================================================================
"""
Shows the spans of time during which someone in ID is eligible
for a transfer to a Community Reentry Center (CRC)-like bed in South Idaho
Correctional Institution (SICI) according to criteria B.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_ix import (
    granted_work_release,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    in_sici_or_has_d3_through_d7_release_note,
    no_recent_marked_ineligible_unless_medical,
)
from recidiviz.task_eligibility.eligibility_spans.us_ix.transfer_to_crc_work_release_request import (
    CRC_WORK_RELEASE_NOT_TIME_BASED,
    CRC_WORK_RELEASE_TIME_BASED,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    task_name="TRANSFER_TO_CRC_LIKE_BED_SICI_B_REQUEST",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        # Not time-based criteria for CRC work release
        *CRC_WORK_RELEASE_NOT_TIME_BASED,
        # Time-based criteria for CRC work release
        *CRC_WORK_RELEASE_TIME_BASED,
        # Must be a resident of SICI or expected to be released to D3/D4/D5/D6/D7
        in_sici_or_has_d3_through_d7_release_note.VIEW_BUILDER,
        # Not denied for CRC work release unless only reason is MEDICAL
        no_recent_marked_ineligible_unless_medical.VIEW_BUILDER,
    ],
    # TODO(#54358): Find out which completion event should be used here
    completion_event_builder=granted_work_release.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
