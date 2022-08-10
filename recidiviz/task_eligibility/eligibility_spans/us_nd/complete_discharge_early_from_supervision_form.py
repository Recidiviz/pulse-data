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
someone in ND is eligible to have early termination paperwork completed for them in
preparation for an early discharge.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    active_supervision_population,
)
from recidiviz.task_eligibility.criteria.general import (
    supervision_early_discharge_date_within_30_days,
)
from recidiviz.task_eligibility.criteria.state_specific.us_nd import (
    implied_valid_early_termination_sentence_type,
    implied_valid_early_termination_supervision_level,
    not_in_active_revocation_status,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which someone in ND is eligible
to have early termination paperwork completed for them in preparation for an early
discharge.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_ND,
    task_name="COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_FORM",
    description=_DESCRIPTION,
    candidate_population_view_builder=active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        supervision_early_discharge_date_within_30_days.VIEW_BUILDER,
        implied_valid_early_termination_sentence_type.VIEW_BUILDER,
        implied_valid_early_termination_supervision_level.VIEW_BUILDER,
        not_in_active_revocation_status.VIEW_BUILDER,
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
