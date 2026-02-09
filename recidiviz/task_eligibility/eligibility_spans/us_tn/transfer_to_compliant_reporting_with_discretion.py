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
"""Shows the spans of time during which someone in TN may be eligible for compliant reporting, with
discretion related to:
- missing/outdated sentencing information
- zero tolerance codes suggesting outdated sentencing information
- ineligible offense types for expired sentences (but not sentences that expired 10+ years ago)
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_tn import (
    transfer_to_limited_supervision,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    fines_fees_eligible,
    ineligible_for_compliant_reporting_no_further_requirement,
    negative_arrest_check_in_past_year,
    no_high_sanctions_in_past_year,
    no_recent_compliant_reporting_rejections,
    on_eligible_level_for_sufficient_time,
)
from recidiviz.task_eligibility.criteria_condition import (
    EligibleCriteriaCondition,
    LessThanOrEqualCriteriaCondition,
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.eligibility_spans.us_tn.transfer_to_compliant_reporting_no_discretion import (
    _REQUIRED_CRITERIA,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="TRANSFER_TO_COMPLIANT_REPORTING_WITH_DISCRETION",
    description=__doc__,
    candidate_population_view_builder=probation_parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        criteria.VIEW_BUILDER for criteria in _REQUIRED_CRITERIA
    ]
    + [ineligible_for_compliant_reporting_no_further_requirement.VIEW_BUILDER],
    completion_event_builder=transfer_to_limited_supervision.VIEW_BUILDER,
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            # Pathway 1 for almost-eligibility: someone is missing up to one of the four
            # AE conditions (for time at level, recent CR rejections, fines/fees, or
            # high sanctions). For this pathway, it doesn't matter whether someone has
            # a recent negative arrest check or not; either way, we want them to be
            # almost eligible if they meet up to one of the four AE conditions here.
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    TimeDependentCriteriaCondition(
                        criteria=on_eligible_level_for_sufficient_time.VIEW_BUILDER,
                        reasons_date_field="eligible_date",
                        interval_length=3,
                        interval_date_part=BigQueryDateInterval.MONTH,
                        description="3 months away from enough time on minimum / medium",
                    ),
                    NotEligibleCriteriaCondition(
                        criteria=no_recent_compliant_reporting_rejections.VIEW_BUILDER,
                        description="Recent CR rejections (not permanent)",
                    ),
                    LessThanOrEqualCriteriaCondition(
                        criteria=fines_fees_eligible.VIEW_BUILDER,
                        reasons_numerical_field="amount_owed",
                        value=2000,
                        description="< $2,000 in fines and fees remaining",
                    ),
                    # Almost eligible - within 3 months of latest highest sanction being 1+ year old.
                    # Since the last high sanction is in the past use a negative time interval (-9 months)
                    # to determine when the latest_high_sanction_date is strictly more than 9 months old
                    TimeDependentCriteriaCondition(
                        criteria=no_high_sanctions_in_past_year.VIEW_BUILDER,
                        reasons_date_field="latest_high_sanction_date",
                        interval_length=-9,
                        interval_date_part=BigQueryDateInterval.MONTH,
                        description="Within 3 months of latest highest sanction being 1+ year old",
                    ),
                ],
                at_most_n_conditions_true=1,
            ),
            # Pathway 2 for almost-eligibility: someone is missing a negative arrest
            # check AND meets all of the criteria for the four AE conditions (for time
            # at level, recent CR rejections, fines/fees, or high sanctions). In other
            # words, the only criterion they're missing is the negative-arrest-check
            # one.
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    EligibleCriteriaCondition(
                        criteria=on_eligible_level_for_sufficient_time.VIEW_BUILDER,
                        description="Enough time on minimum / medium",
                    ),
                    EligibleCriteriaCondition(
                        criteria=no_recent_compliant_reporting_rejections.VIEW_BUILDER,
                        description="No recent CR rejections (not permanent)",
                    ),
                    EligibleCriteriaCondition(
                        criteria=fines_fees_eligible.VIEW_BUILDER,
                        description="Meets fines/fees requirement",
                    ),
                    EligibleCriteriaCondition(
                        criteria=no_high_sanctions_in_past_year.VIEW_BUILDER,
                        description="No highest sanction in past year",
                    ),
                    NotEligibleCriteriaCondition(
                        criteria=negative_arrest_check_in_past_year.VIEW_BUILDER,
                        description="No ARRN in last year",
                    ),
                ],
                at_least_n_conditions_true=5,
            ),
        ],
        # Someone will not be able to meet both of the conditions above (because they
        # can't simultaneously fully meet all criteria for the four AE conditions and
        # meet the AE condition for one of the criteria).
        at_least_n_conditions_true=1,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
