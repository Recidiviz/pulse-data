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
"""Shows the spans of time during which someone in TN may be eligible for compliant reporting.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events import transfer_to_limited_supervision
from recidiviz.task_eligibility.criteria.general import (
    has_active_sentence,
    supervision_level_is_not_internal_unknown,
    supervision_level_is_not_interstate_compact,
    supervision_level_is_not_unassigned,
    supervision_not_past_full_term_completion_date_or_upcoming_90_days,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    fines_fees_eligible,
    no_arrests_in_past_year,
    no_high_sanctions_in_past_year,
    no_murder_convictions,
    no_recent_compliant_reporting_rejections,
    no_zero_tolerance_codes_spans,
    not_in_judicial_district_17_while_on_probation,
    not_on_life_sentence_or_lifetime_supervision,
    not_permanently_rejected_from_compliant_reporting,
    offense_type_eligible,
    on_eligible_level_for_sufficient_time,
    passed_drug_screen_check,
    special_conditions_are_current,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which someone in TN may be eligible for compliant reporting.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="TRANSFER_TO_COMPLIANT_REPORTING_NO_DISCRETION",
    description=_DESCRIPTION,
    candidate_population_view_builder=probation_parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        on_eligible_level_for_sufficient_time.VIEW_BUILDER,
        no_arrests_in_past_year.VIEW_BUILDER,
        no_high_sanctions_in_past_year.VIEW_BUILDER,
        fines_fees_eligible.VIEW_BUILDER,
        passed_drug_screen_check.VIEW_BUILDER,
        special_conditions_are_current.VIEW_BUILDER,
        not_in_judicial_district_17_while_on_probation.VIEW_BUILDER,
        not_permanently_rejected_from_compliant_reporting.VIEW_BUILDER,
        no_recent_compliant_reporting_rejections.VIEW_BUILDER,
        not_on_life_sentence_or_lifetime_supervision.VIEW_BUILDER,
        no_murder_convictions.VIEW_BUILDER,
        no_zero_tolerance_codes_spans.VIEW_BUILDER,
        offense_type_eligible.VIEW_BUILDER,
        has_active_sentence.VIEW_BUILDER,
        supervision_not_past_full_term_completion_date_or_upcoming_90_days.VIEW_BUILDER,
        supervision_level_is_not_internal_unknown.VIEW_BUILDER,
        supervision_level_is_not_interstate_compact.VIEW_BUILDER,
        supervision_level_is_not_unassigned.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_limited_supervision.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
