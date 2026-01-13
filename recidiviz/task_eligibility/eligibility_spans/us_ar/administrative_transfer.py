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
"""Shows the spans of time during which someone in AR is eligible
for administrative transfer to a Community Corrections Center.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    transfer_to_supervision_run_facility,
)
from recidiviz.task_eligibility.criteria.general import (
    serving_incarceration_sentence_of_less_than_6_years,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ar import (
    in_county_jail_backup,
    not_on_90_day_revocation,
    sentence_statute_eligible_for_admin_transfer,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_AR,
    task_name="ADMINISTRATIVE_TRANSFER",
    description=__doc__,
    candidate_population_view_builder=incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        in_county_jail_backup.VIEW_BUILDER,
        not_on_90_day_revocation.VIEW_BUILDER,
        sentence_statute_eligible_for_admin_transfer.VIEW_BUILDER,
        serving_incarceration_sentence_of_less_than_6_years.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_supervision_run_facility.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
