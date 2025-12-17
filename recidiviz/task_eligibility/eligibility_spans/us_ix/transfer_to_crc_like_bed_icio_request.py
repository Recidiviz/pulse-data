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
for a transfer to a Community Reentry Center (CRC)-like bed in Idaho Correctional
Institutional Orofino (ICIO)
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_ix import (
    granted_work_release,
)
from recidiviz.task_eligibility.criteria.general import (
    custody_level_is_minimum,
    is_male,
    not_serving_for_violent_offense,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    has_a_d1_or_d2_release_note,
    in_icio,
    not_denied_for_crc,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix.crc_work_release_time_based_criteria import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.task_eligibility.eligibility_spans.us_ix.transfer_to_crc_work_release_request import (
    VIEW_BUILDER as TRANSFER_TO_CRC_WORK_RELEASE_REQUEST_VIEW_BUILDER,
)
from recidiviz.task_eligibility.eligibility_spans.us_ix.transfer_to_crc_work_release_request import (
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    task_name="TRANSFER_TO_CRC_LIKE_BED_ICIO_REQUEST",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        # All of CRC work-release criteria
        *TRANSFER_TO_CRC_WORK_RELEASE_REQUEST_VIEW_BUILDER.criteria_spans_view_builders,
        # Must be male (ICIO is a men's facility)
        is_male.VIEW_BUILDER,
        # Must be a resident of ICIO or expected to be released to D1/D2
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.OR,
            criteria_name="US_IX_IN_ICIO_OR_HAS_D1_OR_D2_RELEASE_NOTE",
            sub_criteria_list=[
                in_icio.VIEW_BUILDER,
                has_a_d1_or_d2_release_note.VIEW_BUILDER,
            ],
        ),
    ],
    # TODO(#54358): Find out which completion event should be used here
    completion_event_builder=granted_work_release.VIEW_BUILDER,
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
