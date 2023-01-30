# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Builder for a task eligiblity spans view that shows the spans of time during which
someone in ID is eligible for full term discharge from supervision.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    supervision_population_no_absconsion_bench_warrant,
)
from recidiviz.task_eligibility.completion_events import full_term_discharge
from recidiviz.task_eligibility.criteria.general import (
    supervision_past_full_term_completion_date,
)
from recidiviz.task_eligibility.criteria.state_specific.us_id import not_at_liberty
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which someone in ID is eligible
for full term discharge from supervision.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_ID,
    task_name="COMPLETE_FULL_TERM_DISCHARGE_FROM_SUPERVISION",
    description=_DESCRIPTION,
    candidate_population_view_builder=supervision_population_no_absconsion_bench_warrant.VIEW_BUILDER,
    criteria_spans_view_builders=[
        supervision_past_full_term_completion_date.VIEW_BUILDER,
        not_at_liberty.VIEW_BUILDER,
    ],
    completion_event_builder=full_term_discharge.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
