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
"""Shows the spans of time during which someone in NE is eligible
for an override to Conditional Low Risk supervision.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    parole_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_ne import (
    override_to_conditional_low_risk_supervision,
)
from recidiviz.task_eligibility.criteria.general import (
    no_top_three_severity_level_supervision_violation_within_6_months,
    not_supervision_within_1_month_of_projected_completion_date_min_external,
    on_parole_at_least_6_months,
    supervision_level_is_minimum,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ne import (
    compliant_with_special_conditions,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Shared list of criteria across NE supervision level overrides opportunities
US_NE_GENERAL_SUPERVISION_LEVEL_OVERRIDE_CRITERIA = [
    compliant_with_special_conditions,
    no_top_three_severity_level_supervision_violation_within_6_months,
    # More than 1 month until Earned Discharge Date
    not_supervision_within_1_month_of_projected_completion_date_min_external,
    on_parole_at_least_6_months,
]

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_NE,
    task_name="CONDITIONAL_LOW_RISK_SUPERVISION_OVERRIDE",
    description=__doc__,
    candidate_population_view_builder=parole_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        criterion.VIEW_BUILDER
        for criterion in US_NE_GENERAL_SUPERVISION_LEVEL_OVERRIDE_CRITERIA
    ]
    + [
        # In NE, this includes 'LOW', 'DV: LOW', and 'SO: LOW'
        # TODO(#40214): Clarify whether this should use raw ORAS score instead of
        # ingested supervision level.
        supervision_level_is_minimum.VIEW_BUILDER,
    ],
    completion_event_builder=override_to_conditional_low_risk_supervision.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
