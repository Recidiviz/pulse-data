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
"""Shows the spans of time during which someone in NE is eligible for good time restoration.
This TES span is used to be able to compute almost eligibility for the JII-facing good time
restoration tablet app separately from the staff-facing good time restoration workflows.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import good_time_reinstated
from recidiviz.task_eligibility.criteria.general import (
    no_highest_severity_incarceration_sanctions_within_1_year_of_report,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ne import (
    less_than_3_udc_mrs_in_past_6_months,
    no_idc_mrs_in_past_6_months,
    no_ongoing_clinical_treatment_program_refusal,
    not_in_ltrh_for_90_days,
)
from recidiviz.task_eligibility.criteria_condition import (
    BigQueryDateInterval,
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.eligibility_spans.us_ne.good_time_restoration_30_days import (
    US_NE_GOOD_TIME_RESTORATION_30_DAYS_CRITERIA_BUILDERS,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_ALMOST_ELIGIBLE_MONTHS = 3

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_NE,
    task_name="GOOD_TIME_RESTORATION_JII_APP",
    description=__doc__,
    candidate_population_view_builder=incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=US_NE_GOOD_TIME_RESTORATION_30_DAYS_CRITERIA_BUILDERS,
    completion_event_builder=good_time_reinstated.VIEW_BUILDER,
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            TimeDependentCriteriaCondition(
                criteria=no_highest_severity_incarceration_sanctions_within_1_year_of_report.VIEW_BUILDER,
                reasons_date_field="latest_eligible_date",
                interval_length=_ALMOST_ELIGIBLE_MONTHS,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Only missing no Class I MRs in the last year",
            ),
            TimeDependentCriteriaCondition(
                criteria=no_idc_mrs_in_past_6_months.VIEW_BUILDER,
                reasons_date_field="latest_eligible_date",
                interval_length=_ALMOST_ELIGIBLE_MONTHS,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Only missing no IDC MRs in the last 6 months",
            ),
            TimeDependentCriteriaCondition(
                criteria=less_than_3_udc_mrs_in_past_6_months.VIEW_BUILDER,
                reasons_date_field="latest_eligible_date",
                interval_length=_ALMOST_ELIGIBLE_MONTHS,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Only missing fewer than 3 UDC MRs in the last 6 months",
            ),
            NotEligibleCriteriaCondition(
                criteria=not_in_ltrh_for_90_days.VIEW_BUILDER,
                description="Only missing no LTRH in the last 90 days",
            ),
            NotEligibleCriteriaCondition(
                criteria=no_ongoing_clinical_treatment_program_refusal.VIEW_BUILDER,
                description="Only no ongoing clinical treatment recommendations",
            ),
        ],
        at_most_n_conditions_true=1,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
