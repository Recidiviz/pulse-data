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
"""Shows the spans of time during which someone in TN may be eligible for compliant reporting.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_tn import (
    transfer_to_limited_supervision,
)
from recidiviz.task_eligibility.criteria.general import (
    has_active_sentence,
    supervision_level_is_not_furlough,
    supervision_level_is_not_intake,
    supervision_level_is_not_internal_unknown,
    supervision_level_is_not_interstate_compact,
    supervision_level_is_not_residential_program,
    supervision_level_is_not_unassigned,
    supervision_not_past_full_term_completion_date_or_upcoming_90_days,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    fines_fees_eligible,
    ineligible_offenses_expired,
    no_arrests_in_past_year,
    no_dui_offense_in_past_5_years,
    no_high_sanctions_in_past_year,
    no_murder_convictions,
    no_prior_record_with_ineligible_cr_offense,
    no_recent_compliant_reporting_rejections,
    no_zero_tolerance_codes_spans,
    not_in_judicial_district_17_while_on_probation,
    not_on_life_sentence_or_lifetime_supervision,
    not_permanently_rejected_from_compliant_reporting,
    not_serving_ineligible_cr_offense,
    not_serving_unknown_cr_offense,
    on_eligible_level_for_sufficient_time,
    passed_drug_screen_check,
    special_conditions_are_current,
    supervision_level_raw_text_is_not_wrb,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_REQUIRED_CRITERIA = [
    on_eligible_level_for_sufficient_time,
    no_arrests_in_past_year,
    no_high_sanctions_in_past_year,
    fines_fees_eligible,
    passed_drug_screen_check,
    special_conditions_are_current,
    not_in_judicial_district_17_while_on_probation,
    not_permanently_rejected_from_compliant_reporting,
    no_recent_compliant_reporting_rejections,
    not_on_life_sentence_or_lifetime_supervision,
    supervision_level_is_not_furlough,
    supervision_level_is_not_internal_unknown,
    supervision_level_is_not_interstate_compact,
    supervision_level_is_not_residential_program,
    supervision_level_is_not_unassigned,
    supervision_level_raw_text_is_not_wrb,
    supervision_level_is_not_intake,
    no_murder_convictions,
    not_serving_ineligible_cr_offense,
    no_dui_offense_in_past_5_years,
]

_DISCRETION_CRITERIA = [
    no_zero_tolerance_codes_spans,
    has_active_sentence,
    supervision_not_past_full_term_completion_date_or_upcoming_90_days,
    not_serving_unknown_cr_offense,
    no_prior_record_with_ineligible_cr_offense,
    ineligible_offenses_expired,
]

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="TRANSFER_TO_COMPLIANT_REPORTING_NO_DISCRETION",
    description=__doc__,
    candidate_population_view_builder=probation_parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        criteria.VIEW_BUILDER for criteria in _REQUIRED_CRITERIA
    ]
    + [criteria.VIEW_BUILDER for criteria in _DISCRETION_CRITERIA],
    completion_event_builder=transfer_to_limited_supervision.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
