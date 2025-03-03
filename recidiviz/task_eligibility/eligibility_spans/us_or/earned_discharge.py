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
"""Task eligibility spans view that shows the spans of time when someone in OR is
eligible for earned discharge (EDIS) for at least one sentence.
"""

from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.calculator.query.state.views.analyst_data.us_or.us_or_earned_discharge_sentence_eligibility_spans import (
    US_OR_EARNED_DISCHARGE_SENTENCE_ALMOST_ELIGIBLE_INTERVAL_DATE_PART,
    US_OR_EARNED_DISCHARGE_SENTENCE_ALMOST_ELIGIBLE_INTERVAL_LENGTH,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_or import (
    early_discharge,
)
from recidiviz.task_eligibility.criteria.state_specific.us_or import (
    no_supervision_sanctions_within_6_months,
    not_on_conditional_discharge_or_diversion_supervision,
    not_on_second_look_conditional_release,
    sentence_eligible,
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
    state_code=StateCode.US_OR,
    task_name="EARNED_DISCHARGE",
    description=__doc__,
    candidate_population_view_builder=active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        no_supervision_sanctions_within_6_months.VIEW_BUILDER,
        # Conditional-discharge and diversion cases should be excluded already by our
        # sentence-level criteria, but we have these person-level exclusions in here as
        # a backstop against bad data entry, per guidance from ODOC.
        not_on_conditional_discharge_or_diversion_supervision.VIEW_BUILDER,
        # Similar reasoning for the Second Look exclusion: we presumably shouldn't see
        # any SL cases coming through to this point anyways, but ODOC uses this
        # exclusion as another backstop against data-quality issues.
        not_on_second_look_conditional_release.VIEW_BUILDER,
        sentence_eligible.VIEW_BUILDER,
    ],
    completion_event_builder=early_discharge.VIEW_BUILDER,
    # Clients are almost eligible in Oregon for earned discharge if they are 2 months
    # away from becoming eligible:
    #   (Less than 60 days from the latest supervision sanction being 6 months old
    #       OR no sanctions in the last 6 months)
    #   AND
    #   (Less than 60 days from the earliest sentence eligibility date
    #       OR at least 1 sentence is eligible)
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    TimeDependentCriteriaCondition(
                        criteria=no_supervision_sanctions_within_6_months.VIEW_BUILDER,
                        reasons_date_field="violation_expiration_date",
                        interval_length=60,
                        interval_date_part=BigQueryDateInterval.DAY,
                        description="Supervision sanction less than 60 days from being 6 months old",
                    ),
                    EligibleCriteriaCondition(
                        criteria=no_supervision_sanctions_within_6_months.VIEW_BUILDER,
                        description="No supervision sanctions within the past 6 months",
                    ),
                ],
                at_least_n_conditions_true=1,
            ),
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    TimeDependentCriteriaCondition(
                        criteria=sentence_eligible.VIEW_BUILDER,
                        reasons_date_field="earliest_sentence_eligibility_date",
                        interval_length=US_OR_EARNED_DISCHARGE_SENTENCE_ALMOST_ELIGIBLE_INTERVAL_LENGTH,
                        interval_date_part=US_OR_EARNED_DISCHARGE_SENTENCE_ALMOST_ELIGIBLE_INTERVAL_DATE_PART,
                        description="Less than 60 days from the earliest sentence eligibility date across all active sentences",
                    ),
                    EligibleCriteriaCondition(
                        criteria=sentence_eligible.VIEW_BUILDER,
                        description="At least one sentence has passed the EDIS sentence eligibility date",
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
