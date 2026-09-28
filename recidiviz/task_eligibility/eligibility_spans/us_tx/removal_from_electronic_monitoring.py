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
"""Shows the spans of time during which someone in TX, who has an electronic
monitoring case type, is eligible to have that case type removed.
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_tx import (
    case_type_downgrade_from_electronic_monitoring,
)
from recidiviz.task_eligibility.criteria.general import (
    at_least_60_days_since_negative_drug_test_streak_began,
    no_supervision_violation_within_30_days,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    at_least_30_days_since_last_rejection_for_em_removal,
    at_least_60_days_since_facility_or_temporary_housing_ended,
    electronic_monitoring_for_at_least_60_days,
    not_interstate_compact_in,
)
from recidiviz.task_eligibility.criteria_condition import (
    EligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TX,
    task_name="REMOVAL_FROM_ELECTRONIC_MONITORING",
    description=__doc__,
    candidate_population_view_builder=active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        electronic_monitoring_for_at_least_60_days.VIEW_BUILDER,
        at_least_30_days_since_last_rejection_for_em_removal.VIEW_BUILDER,
        at_least_60_days_since_facility_or_temporary_housing_ended.VIEW_BUILDER,
        at_least_60_days_since_negative_drug_test_streak_began.VIEW_BUILDER,
        no_supervision_violation_within_30_days.VIEW_BUILDER,
        not_interstate_compact_in.VIEW_BUILDER,
    ],
    completion_event_builder=case_type_downgrade_from_electronic_monitoring.VIEW_BUILDER,
    # Two criteria resolve purely by waiting: the electronic monitoring duration and the
    # wait after a facility or temporary or supportive housing. Someone counts as almost
    # eligible only when every one of those clocks is either already met or within 30
    # days, so a person who is days away on one but two months away on the other is not
    # surfaced.
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    EligibleCriteriaCondition(
                        criteria=electronic_monitoring_for_at_least_60_days.VIEW_BUILDER,
                        description="On electronic monitoring for at least 60 days",
                    ),
                    TimeDependentCriteriaCondition(
                        criteria=electronic_monitoring_for_at_least_60_days.VIEW_BUILDER,
                        reasons_date_field="eligible_date",
                        interval_length=30,
                        interval_date_part=BigQueryDateInterval.DAY,
                        description="Within 30 days of being on electronic monitoring for 60 days",
                    ),
                ],
                at_least_n_conditions_true=1,
            ),
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    EligibleCriteriaCondition(
                        criteria=at_least_60_days_since_facility_or_temporary_housing_ended.VIEW_BUILDER,
                        description="Out of a facility or temporary or supportive housing for at least 60 days",
                    ),
                    TimeDependentCriteriaCondition(
                        criteria=at_least_60_days_since_facility_or_temporary_housing_ended.VIEW_BUILDER,
                        reasons_date_field="eligible_date",
                        interval_length=30,
                        interval_date_part=BigQueryDateInterval.DAY,
                        description="Within 30 days of being out of a facility or temporary or supportive housing for 60 days",
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
