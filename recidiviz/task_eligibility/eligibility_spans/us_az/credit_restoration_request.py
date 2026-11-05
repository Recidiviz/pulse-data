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
"""Shows the eligibility spans for incarcerated people who are eligible to apply for a restoration of their credits."""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import good_time_reinstated
from recidiviz.task_eligibility.criteria.general import (
    custody_level_is_not_max,
    not_serving_a_life_sentence,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az import (
    meets_functional_literacy_tpr,
    no_program_refusals_within_12_months,
    not_serving_flat_sentence,
    within_5_years_of_earliest_release_date,
    within_6_months_of_earliest_release_date,
)
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateSpecificInvertedTaskCriteriaBigQueryViewBuilder,
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

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_AZ,
    task_name="CREDIT_RESTORATION_REQUEST",
    description=__doc__,
    candidate_population_view_builder=incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        # NOT Active STG Status
        # NOT on Maximum Custody
        custody_level_is_not_max.VIEW_BUILDER,
        # No Failure to Meet Literacy Standards
        meets_functional_literacy_tpr.VIEW_BUILDER,
        # No refusal of mandatory programming
        no_program_refusals_within_12_months.VIEW_BUILDER,
        # Subsequent Rule Violations
        # Release Date more than 6 months and less than 5 years out
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.AND,
            criteria_name="US_AZ_MIN_6_MONTHS_MAX_5_YEARS_UNTIL_RELEASE",
            sub_criteria_list=[
                within_5_years_of_earliest_release_date.VIEW_BUILDER,
                StateSpecificInvertedTaskCriteriaBigQueryViewBuilder(
                    sub_criteria=within_6_months_of_earliest_release_date.VIEW_BUILDER,
                ),
            ],
            allowed_duplicate_reasons_keys=["release_date"],
        ),
        # Earned Incentive Program
        # Not serving a flat sentence
        not_serving_flat_sentence.VIEW_BUILDER,
        # Not serving a life sentence
        not_serving_a_life_sentence.VIEW_BUILDER,
    ],
    completion_event_builder=good_time_reinstated.VIEW_BUILDER,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
