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
Shows the spans of time during which someone in ID is eligible
for a transfer to a Community Reentry Center (CRC)-like bed in South Idaho
Correctional Institution (SICI) or Idaho Correctional Institution-Orofino (ICIO)
according to criteria A.

Criteria A uses its own time-based criteria based on offense severity,
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_ix import (
    transfer_to_minimum_facility,
)
from recidiviz.task_eligibility.criteria.general import (
    custody_level_is_minimum,
    not_serving_for_violent_offense,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    crc_like_bed_time_based_criteria,
    in_sici_or_icio_or_relevant_release_notes,
    not_denied_for_crc,
)
from recidiviz.task_eligibility.criteria_condition import (
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
)
from recidiviz.task_eligibility.eligibility_spans.us_ix.transfer_to_crc_work_release_request import (
    CRC_WORK_RELEASE_NOT_TIME_BASED,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    task_name="TRANSFER_TO_CRC_LIKE_BED_A_FACILITY_BASED_REQUEST",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        # Not time-based criteria for CRC work release
        *CRC_WORK_RELEASE_NOT_TIME_BASED,
        # Time-based criteria (low severity OR high severity)
        crc_like_bed_time_based_criteria.VIEW_BUILDER,
        # Must be a resident of SICI, or ICIO, or expected to be released in a relevant district
        in_sici_or_icio_or_relevant_release_notes.VIEW_BUILDER,
    ],
    # TODO(#54358): Hydrate completion event
    completion_event_builder=transfer_to_minimum_facility.VIEW_BUILDER,
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            NotEligibleCriteriaCondition(
                criteria=not_serving_for_violent_offense.VIEW_BUILDER,
                description="Serving a sentence for a violent offense",
            ),
            NotEligibleCriteriaCondition(
                criteria=not_denied_for_crc.VIEW_BUILDER,
                description="Denied for CRC eligibility",
            ),
            NotEligibleCriteriaCondition(
                criteria=custody_level_is_minimum.VIEW_BUILDER,
                description="Custody level is not minimum",
            ),
        ],
        at_most_n_conditions_true=1,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
