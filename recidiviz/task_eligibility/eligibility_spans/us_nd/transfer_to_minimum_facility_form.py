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
"""
Shows the spans of time during which someone in ND is for a transfer into a 
minimum security facility.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_nd import (
    transfer_to_reentry_center,
)
from recidiviz.task_eligibility.criteria.general import custody_level_is_minimum
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.criteria.state_specific.us_nd import (
    not_in_minimum_security_facility,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which someone in ND is for a transfer into a
minimum security facility.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_ND,
    task_name="TRANSFER_TO_MINIMUM_FACILITY_FORM",
    description=_DESCRIPTION,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        custody_level_is_minimum.VIEW_BUILDER,
        not_in_minimum_security_facility.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_reentry_center.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
