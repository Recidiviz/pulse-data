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
"""Shows the eligibility spans for supervision clients in AZ who are eligible
for a transfer to CPR."""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_az import (
    transfer_to_cpr_unit,
)
from recidiviz.task_eligibility.criteria.general import (
    custody_level_is_close,
    no_escape_violations,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az import (
    custody_level_is_true_medium,
    in_cpr_eligible_unit,
    no_cpr_relevant_violent_disciplinaries_within_12_months,
    no_cpr_relevant_violent_disciplinaries_within_24_months,
    no_escape_convictions,
    no_program_refusals_within_12_months,
    no_staff_assaults_within_12_months,
    no_staff_assaults_within_36_months,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Close custody, No violent violations within 24 months, No staff assaults within 36 months
_ELIGIBLE_FOR_CLOSE_DOWNGRADE = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name="US_AZ_CLOSE_AND_NO_VIOLATIONS",
    sub_criteria_list=[
        custody_level_is_close.VIEW_BUILDER,
        no_cpr_relevant_violent_disciplinaries_within_24_months.VIEW_BUILDER,
        no_staff_assaults_within_36_months.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[],
)
# TRUE Medium custody, No violent violations within 12 months, No staff assaults within 12 months
_ELIGIBLE_FOR_MEDIUM_DOWNGRADE = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name="US_AZ_MEDIUM_AND_NO_VIOLATIONS",
    sub_criteria_list=[
        custody_level_is_true_medium.VIEW_BUILDER,
        no_cpr_relevant_violent_disciplinaries_within_12_months.VIEW_BUILDER,
        no_staff_assaults_within_12_months.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[],
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_AZ,
    task_name="TRANSITION_TO_CPR_REQUEST",
    description=__doc__,
    candidate_population_view_builder=incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.OR,
            criteria_name="US_AZ_MEDIUM_OR_CLOSE_WITH_NO_RELEVANT_VIOLATIONS",
            sub_criteria_list=[
                _ELIGIBLE_FOR_MEDIUM_DOWNGRADE,
                _ELIGIBLE_FOR_CLOSE_DOWNGRADE,
            ],
            allowed_duplicate_reasons_keys=[
                "custody_level",
                "custody_level_start_date",
            ],
        ),
        # Have no documented refusal to participate in required programs within the previous 12 months
        no_program_refusals_within_12_months.VIEW_BUILDER,
        # Escape-related offenses or incidents
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.AND,
            criteria_name="US_AZ_NO_ESCAPE_INCIDENTS",
            sub_criteria_list=[
                no_escape_violations.VIEW_BUILDER,
                no_escape_convictions.VIEW_BUILDER,
            ],
            allowed_duplicate_reasons_keys=[],
        ),
        # Additional ask to add CPR eligible units
        in_cpr_eligible_unit.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_cpr_unit.VIEW_BUILDER,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
