# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Task eligibility spans view that shows the spans of time when someone in TN is
eligible for Compliant Reporting under the policy implemented in 2025, for those who
being supervised as "misdemeanor probationers," who are eligible for Compliant Reporting
following intake.
"""

# TODO(#40868): Ideally, combine this logic into the eligibility spans for the pre-2025
# version of the CR policy, such that we have a single set of spans (if possible) for
# the old and new versions of the policy.

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_tn import (
    transfer_to_limited_supervision_2025_policy,
)
from recidiviz.task_eligibility.criteria.general import supervision_level_is_not_limited
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    no_ineligible_cr_offense_2025_policy,
    supervision_type_is_misdemeanor_probationer,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# The new Compliant Reporting policy has 3 "pathways" to get the opportunity:
#   - Intake pathway: for people who test Low-Compliant on the STRONG-R 2.0 during
#     intake and meet the set of intake-specific criteria.
#   - "Minimum" pathway: for people who have been on "Low" supervision (mapped to
#     'MINIMUM' internally) for 6+ months and meet the set of criteria for this pathway.
#   - "Misdemeanor Probationer" pathway: for people supervised as "Misdemeanor
#      Probationer" clients, who are eligible from intake and do not have a
#      disqualifying offense.
# There is some criteria set that is applied to both the intake and minimum groups. This
# TES file is for the misdemeanor probationer pathway,
# TRANSFER_INTAKE_GROUP_TO_COMPLIANT_REPORTING_2025_POLICY is for the intake pathway,
# and TRANSFER_MINIMUM_GROUP_TO_COMPLIANT_REPORTING_2025_POLICY is for the minimum
# pathway. All are combined in one opportunity record query.

# TODO(#72587): Do we want to use CR denial codes from contact note types to filter out
# clients who are ineligible per court requirements for supervision?
VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="TRANSFER_MISDEMEANOR_PROBATIONER_GROUP_TO_COMPLIANT_REPORTING_2025_POLICY",
    description=__doc__,
    candidate_population_view_builder=probation_parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        no_ineligible_cr_offense_2025_policy.VIEW_BUILDER,
        supervision_type_is_misdemeanor_probationer.VIEW_BUILDER,
        # filter out clients already on CR
        supervision_level_is_not_limited.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_limited_supervision_2025_policy.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
