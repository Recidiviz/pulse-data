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
"""Shows the spans of time during which someone in CA is eligible
for an attained employment kudos. This span only lasts for two months after the event 
happened and is only available to clients on PAROLE and not IN_CUSTODY.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    parole_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.general import kudos_sms_sent
from recidiviz.task_eligibility.criteria.state_specific.us_ca import (
    employment_for_0_to_2_months,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which someone in CA is eligible
for an attained employment kudos. This span only lasts for two months after the event 
happened and is only available to clients on PAROLE and not IN_CUSTODY.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_CA,
    task_name="kudos_employment_0_to_2_months",
    description=_DESCRIPTION,
    candidate_population_view_builder=parole_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        employment_for_0_to_2_months.VIEW_BUILDER,
    ],
    completion_event_builder=kudos_sms_sent.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
