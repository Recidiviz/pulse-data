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
"""Builder for a task eligibility spans view that shows the spans of time during which
someone in MI is eligible for minimum telephone reporting.
"""
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
    not_required_to_register_under_sora,
    not_serving_ineligible_offenses_for_telephone_reporting,
    supervision_and_assessment_level_eligible_for_telephone_reporting,
    supervision_level_is_not_modified,
    supervision_specialty_is_not_rposn,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which
someone in MI is eligible for minimum telephone reporting.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    task_name="COMPLETE_TRANSFER_TO_TELEPHONE_REPORTING_REQUEST",
    description=_DESCRIPTION,
    candidate_population_view_builder=probation_parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        not_serving_ineligible_offenses_for_telephone_reporting.VIEW_BUILDER,
        not_required_to_register_under_sora.VIEW_BUILDER,
        supervision_not_past_full_term_completion_date_or_upcoming_90_days.VIEW_BUILDER,
        on_minimum_supervision_at_least_six_months.VIEW_BUILDER,
        supervision_and_assessment_level_eligible_for_telephone_reporting.VIEW_BUILDER,
        supervision_specialty_is_not_rposn.VIEW_BUILDER,
        if_serving_an_ouil_or_owi_has_completed_12_months_on_supervision.VIEW_BUILDER,
        supervision_level_is_not_modified.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_limited_supervision.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
