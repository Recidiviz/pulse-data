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
"""Shows the spans of time during which someone in TX, who is on parole or dual parole and probation,
is eligible to be transferred to Annual Reporting Status (Limited Supervision). This status only
affects their parole status, not their probation status.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    transfer_to_limited_supervision,
)
from recidiviz.task_eligibility.criteria.general import (
    supervision_level_is_minimum_for_3_years,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    after_ars_ers_policy_effective_date,
    case_type_eligible_for_ars_ers,
    no_warrant_with_sustained_violation_within_2_years,
    not_supervision_within_6_months_of_release_date,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TX,
    task_name="TRANSFER_FROM_PAROLE_TO_ANNUAL_REPORT_STATUS",
    description=__doc__,
    candidate_population_view_builder=parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        supervision_level_is_minimum_for_3_years.VIEW_BUILDER,
        no_warrant_with_sustained_violation_within_2_years.VIEW_BUILDER,
        not_supervision_within_6_months_of_release_date.VIEW_BUILDER,
        case_type_eligible_for_ars_ers.VIEW_BUILDER,
        after_ars_ers_policy_effective_date.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_limited_supervision.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
