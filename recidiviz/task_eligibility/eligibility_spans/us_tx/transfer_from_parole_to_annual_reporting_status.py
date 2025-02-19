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
"""Builder for a task eligibility spans view that shows the spans of time during which
someone in TX, who is on parole or dual parole and probation, is eligible to be transferred
to Annual Reporting Status (Limited Supervision). This status only affects their parole status,
not their probation status.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    transfer_to_limited_supervision,
)
from recidiviz.task_eligibility.criteria.general import (
    on_minimum_supervision_at_least_1_year,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    not_convicted_of_ineligible_offense_for_ars,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = (
    "Shows the spans of time during which someone in TX, who is on parole or dual parole and probation, is eligible to "
    "be transferred to Annual Reporting Status (Limited Supervision). This status only affects their parole status, "
    "not their probation status."
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TX,
    task_name="TRANSFER_FROM_PAROLE_TO_ANNUAL_REPORTING_STATUS",
    description=_DESCRIPTION,
    candidate_population_view_builder=parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        on_minimum_supervision_at_least_1_year.VIEW_BUILDER,
        not_convicted_of_ineligible_offense_for_ars.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_limited_supervision.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
