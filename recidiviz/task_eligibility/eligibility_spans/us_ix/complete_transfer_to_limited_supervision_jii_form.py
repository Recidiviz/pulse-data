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
"""Builder for a task eligibility spans view that shows the spans of time during which
someone in ID is eligible or almost eligible to complete the form for transfer to limited unit supervision.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.criteria.general import (
    not_serving_a_life_sentence_on_supervision,
    not_serving_for_sexual_offense_on_supervision,
    supervision_level_is_not_diversion,
    supervision_level_is_not_limited,
    supervision_not_past_full_term_completion_date,
    under_supervision_custodial_authority_at_least_one_year,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    income_verified_within_3_months,
    lsir_level_low_for_90_days,
    no_active_nco,
    no_recent_marked_ineligible_unless_ffr,
    supervision_level_raw_text_is_not_so_or_soto,
)
from recidiviz.task_eligibility.criteria_condition import NotEligibleCriteriaCondition
from recidiviz.task_eligibility.eligibility_spans.us_ix import (
    complete_transfer_to_limited_supervision_form,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which someone in ID is eligible or almost eligible
to complete the form for transfer to limited unit supervision. These spans have a more flexible set of almost
eligible criteria conditions since it is JII-facing.
"""

COMPLETE_TRANSFER_TO_LSU_LINE_STAFF_VERSION = (
    complete_transfer_to_limited_supervision_form.VIEW_BUILDER
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    task_name="COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_JII_FORM",
    description=_DESCRIPTION,
    candidate_population_view_builder=COMPLETE_TRANSFER_TO_LSU_LINE_STAFF_VERSION.candidate_population_view_builder,
    criteria_spans_view_builders=[
        lsir_level_low_for_90_days.VIEW_BUILDER,
        supervision_not_past_full_term_completion_date.VIEW_BUILDER,
        income_verified_within_3_months.VIEW_BUILDER,
        under_supervision_custodial_authority_at_least_one_year.VIEW_BUILDER,
        no_active_nco.VIEW_BUILDER,
        supervision_level_is_not_limited.VIEW_BUILDER,
        supervision_level_raw_text_is_not_so_or_soto.VIEW_BUILDER,
        not_serving_for_sexual_offense_on_supervision.VIEW_BUILDER,
        not_serving_a_life_sentence_on_supervision.VIEW_BUILDER,
        no_recent_marked_ineligible_unless_ffr.VIEW_BUILDER,
        supervision_level_is_not_diversion.VIEW_BUILDER,
    ],
    completion_event_builder=COMPLETE_TRANSFER_TO_LSU_LINE_STAFF_VERSION.completion_event_builder,
    almost_eligible_condition=NotEligibleCriteriaCondition(
        criteria=income_verified_within_3_months.VIEW_BUILDER,
        description="Missing employment verification within 90 days",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
