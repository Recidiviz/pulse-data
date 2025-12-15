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
"""Shows the spans of time during which
someone in MI is eligible for minimum telephone reporting.
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    transfer_to_limited_supervision,
)
from recidiviz.task_eligibility.criteria.general import (
    on_minimum_supervision_at_least_six_months,
    supervision_not_past_full_term_completion_date_or_upcoming_90_days,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mi import (
    if_serving_an_ouil_or_owi_has_completed_12_months_on_supervision,
    no_sora_conditions,
    not_serving_delayed_sentence,
    not_serving_ineligible_offenses_for_telephone_reporting,
    supervision_and_assessment_level_eligible_for_telephone_reporting,
    supervision_level_is_not_modified,
    supervision_specialty_is_not_rposn,
)
from recidiviz.task_eligibility.criteria_condition import (
    EligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    task_name="COMPLETE_TRANSFER_TO_TELEPHONE_REPORTING_REQUEST",
    description=__doc__,
    candidate_population_view_builder=probation_parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        not_serving_ineligible_offenses_for_telephone_reporting.VIEW_BUILDER,
        supervision_not_past_full_term_completion_date_or_upcoming_90_days.VIEW_BUILDER,
        on_minimum_supervision_at_least_six_months.VIEW_BUILDER,
        supervision_and_assessment_level_eligible_for_telephone_reporting.VIEW_BUILDER,
        supervision_specialty_is_not_rposn.VIEW_BUILDER,
        if_serving_an_ouil_or_owi_has_completed_12_months_on_supervision.VIEW_BUILDER,
        supervision_level_is_not_modified.VIEW_BUILDER,
        no_sora_conditions.VIEW_BUILDER,
        not_serving_delayed_sentence.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_limited_supervision.VIEW_BUILDER,
    # Clients are almost eligible for telephone reporting if they are within 30 days from being fully eligible,
    # which means they are both within 30 days of completing six months on minimum supervision and
    # if serving for an ouil or owi, within 30 days of completing one year on supervision.
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    TimeDependentCriteriaCondition(
                        criteria=on_minimum_supervision_at_least_six_months.VIEW_BUILDER,
                        reasons_date_field="minimum_time_served_date",
                        interval_length=30,
                        interval_date_part=BigQueryDateInterval.DAY,
                        description="Within 30 days of six months on minimum supervision",
                    ),
                    EligibleCriteriaCondition(
                        criteria=on_minimum_supervision_at_least_six_months.VIEW_BUILDER,
                        description="Completed six months on minimum supervision",
                    ),
                ],
                at_least_n_conditions_true=1,
            ),
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    TimeDependentCriteriaCondition(
                        criteria=if_serving_an_ouil_or_owi_has_completed_12_months_on_supervision.VIEW_BUILDER,
                        reasons_date_field="one_year_on_supervision_date",
                        interval_length=30,
                        interval_date_part=BigQueryDateInterval.DAY,
                        description="Within 30 days of one year on supervision",
                    ),
                    EligibleCriteriaCondition(
                        criteria=if_serving_an_ouil_or_owi_has_completed_12_months_on_supervision.VIEW_BUILDER,
                        description="Completed one year on supervision",
                    ),
                ],
                at_least_n_conditions_true=1,
            ),
        ],
        at_least_n_conditions_true=2,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
