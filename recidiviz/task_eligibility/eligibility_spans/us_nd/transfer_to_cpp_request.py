# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
Shows the spans of time during which someone in ND is eligible
for a transfer to CPP (Community Placement Program)
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    release_to_community_confinement_supervision,
)
from recidiviz.task_eligibility.criteria.general import (
    custody_level_is_minimum_or_medium,
    custody_level_is_minimum_or_medium_for_60_days,
    incarceration_past_half_full_term_release_date,
    incarceration_within_30_months_of_full_term_completion_date,
    no_absconsion_within_1_year,
    no_highest_or_second_highest_severity_incarceration_incidents_within_90_days,
)
from recidiviz.task_eligibility.criteria.state_specific.us_nd import (
    incarceration_past_85_percent_of_sentence,
    incarceration_past_parole_review_date_plus_one_month,
    incarceration_within_5_or_more_months_of_parole_review_date,
    no_escape_offense_within_1_year,
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
    state_code=StateCode.US_ND,
    task_name="TRANSFER_TO_CPP_REQUEST",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        # 1. In minimum or medium custody level for 60 days
        custody_level_is_minimum_or_medium.VIEW_BUILDER,
        custody_level_is_minimum_or_medium_for_60_days.VIEW_BUILDER,
        # 2. Served more than 1/2 of sentence
        incarceration_past_half_full_term_release_date.VIEW_BUILDER,
        # 3. 30 months before full term completion date
        incarceration_within_30_months_of_full_term_completion_date.VIEW_BUILDER,
        # 4. No Level II or III infractions in the past 90 days
        no_highest_or_second_highest_severity_incarceration_incidents_within_90_days.VIEW_BUILDER,
        # 5. Reached their 85% release date (for applicable offenses)
        incarceration_past_85_percent_of_sentence.VIEW_BUILDER,
        # 6. No history of escape of absconsion in the past year
        no_absconsion_within_1_year.VIEW_BUILDER,
        no_escape_offense_within_1_year.VIEW_BUILDER,
        # 7. Parole Review Date is at least 5 months in the future
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.OR,
            criteria_name="US_ND_INCARCERATION_WITHIN_5_OR_MORE_MONTHS_OF_PRD_OR_PAST_PRD_PLUS_ONE_MONTH",
            sub_criteria_list=[
                incarceration_within_5_or_more_months_of_parole_review_date.VIEW_BUILDER,
                incarceration_past_parole_review_date_plus_one_month.VIEW_BUILDER,
            ],
            allowed_duplicate_reasons_keys=[
                "parole_review_date",
            ],
        ),
    ],
    completion_event_builder=release_to_community_confinement_supervision.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
