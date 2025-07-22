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
"""Task eligibility spans view that shows the spans of time when someone in MO is
eligible for outside clearance.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    granted_institutional_worker_status,
)
from recidiviz.task_eligibility.criteria.general import (
    incarceration_within_60_months_of_projected_full_term_completion_date_min,
    not_on_institutional_worker_status,
)

# from recidiviz.task_eligibility.criteria.state_specific.us_mo import (
#     not_eligible_or_almost_eligible_for_work_release,
# )
from recidiviz.task_eligibility.eligibility_spans.us_mo.work_release import (
    WORK_RELEASE_AND_OUTSIDE_CLEARANCE_SHARED_CRITERIA,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MO,
    task_name="OUTSIDE_CLEARANCE",
    description=__doc__,
    # TODO(#44398): Ensure that this is the correct candidate population.
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        *WORK_RELEASE_AND_OUTSIDE_CLEARANCE_SHARED_CRITERIA,
        incarceration_within_60_months_of_projected_full_term_completion_date_min.VIEW_BUILDER,
        # TODO(#45316): Un-comment line for
        # `US_MO_NOT_ELIGIBLE_OR_ALMOST_ELIGIBLE_FOR_WORK_RELEASE` once we're ready to
        # make the opportunities mutually exclusive.
        # not_eligible_or_almost_eligible_for_work_release.VIEW_BUILDER,
        # TODO(#44399): This criterion currently only captures people who have approved
        # outside-clearance requests, but since not every facility records
        # approvals/denials via the requests table, can we find another way to try to
        # identify residents already on OC and exclude them from eligibility here?
        not_on_institutional_worker_status.VIEW_BUILDER,
    ],
    # TODO(#44389): Make sure this completion event is pulling in the proper data from
    # upstream to capture outside-clearance events appropriately.
    completion_event_builder=granted_institutional_worker_status.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
