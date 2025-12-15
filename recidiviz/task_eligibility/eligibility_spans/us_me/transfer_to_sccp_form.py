# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Shows the spans of time during which someone in ME is eligible
for a transfer to SCCP (Supervised Community Confinement Program).
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    release_to_community_confinement_supervision,
)
from recidiviz.task_eligibility.criteria.state_specific.us_me import (
    custody_level_is_minimum_or_community,
    no_class_a_or_b_violation_for_90_days,
    no_detainers_warrants_or_other,
    served_x_portion_of_sentence,
    x_months_remaining_on_sentence,
)
from recidiviz.task_eligibility.criteria_condition import (
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_ME,
    task_name="TRANSFER_TO_SCCP_FORM",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        custody_level_is_minimum_or_community.VIEW_BUILDER,
        served_x_portion_of_sentence.VIEW_BUILDER,
        x_months_remaining_on_sentence.VIEW_BUILDER,
        no_detainers_warrants_or_other.VIEW_BUILDER,
        no_class_a_or_b_violation_for_90_days.VIEW_BUILDER,
    ],
    completion_event_builder=release_to_community_confinement_supervision.VIEW_BUILDER,
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            TimeDependentCriteriaCondition(
                criteria=x_months_remaining_on_sentence.VIEW_BUILDER,
                reasons_date_field="eligible_date",
                interval_length=6,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Within 6 months of 30/24 month eligibility date",
            ),
            TimeDependentCriteriaCondition(
                criteria=served_x_portion_of_sentence.VIEW_BUILDER,
                reasons_date_field="eligible_date",
                interval_length=6,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Within 6 months of 1/2 or 2/3 of sentence served",
            ),
            NotEligibleCriteriaCondition(
                criteria=no_class_a_or_b_violation_for_90_days.VIEW_BUILDER,
                description="One disciplinary violation away from eligibility",
            ),
        ],
        at_most_n_conditions_true=1,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
