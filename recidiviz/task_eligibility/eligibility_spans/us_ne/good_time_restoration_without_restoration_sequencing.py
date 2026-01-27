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
"""Spans of time when a JII in NE is eligible for good time restoration without taking 
into account restoration sequencing (i.e. ~2 weeks between restoration events). This is
used to determine how many times good time has been restored while a JII is continuously
eligible for good time restoration.
"""
from typing import List

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import good_time_reinstated
from recidiviz.task_eligibility.criteria.general import (
    no_highest_severity_incarceration_sanctions_within_1_year_of_report,
    no_revocation_incarceration_starts_in_last_90_days,
    under_state_prison_or_supervision_custodial_authority_without_absconsion_at_least_one_year,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ne import (
    has_lost_restorable_good_time,
    less_than_3_udc_mrs_in_past_6_months,
    no_gt_restoration_denials_in_last_90_days,
    no_idc_mrs_in_past_6_months,
    no_ongoing_clinical_treatment_program_refusal,
    not_in_ltrh_for_90_days,
    over_4_months_from_trd,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_NE_GOOD_TIME_RESTORATION_30_DAYS_CRITERIA_BUILDERS_WITHOUT_RESTORATION_SEQUENCING: List[
    TaskCriteriaBigQueryViewBuilder
] = [
    has_lost_restorable_good_time.VIEW_BUILDER,
    not_in_ltrh_for_90_days.VIEW_BUILDER,
    under_state_prison_or_supervision_custodial_authority_without_absconsion_at_least_one_year.VIEW_BUILDER,
    no_highest_severity_incarceration_sanctions_within_1_year_of_report.VIEW_BUILDER,
    no_idc_mrs_in_past_6_months.VIEW_BUILDER,
    less_than_3_udc_mrs_in_past_6_months.VIEW_BUILDER,
    over_4_months_from_trd.VIEW_BUILDER,
    no_ongoing_clinical_treatment_program_refusal.VIEW_BUILDER,
    no_gt_restoration_denials_in_last_90_days.VIEW_BUILDER,
    no_revocation_incarceration_starts_in_last_90_days.VIEW_BUILDER,
]


VIEW_BUILDER: SingleTaskEligibilitySpansBigQueryViewBuilder = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_NE,
    description=__doc__,
    task_name="GOOD_TIME_RESTORATION_WITHOUT_RESTORATION_SEQUENCING",
    criteria_spans_view_builders=US_NE_GOOD_TIME_RESTORATION_30_DAYS_CRITERIA_BUILDERS_WITHOUT_RESTORATION_SEQUENCING,
    candidate_population_view_builder=incarceration_population.VIEW_BUILDER,
    completion_event_builder=good_time_reinstated.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
