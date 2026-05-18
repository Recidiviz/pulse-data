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
"""
Shows the spans of time during which someone in ND is eligible
for the Adult Training Program (ATP) under the post-2026-06-01 policy.

Starts at 2026-06-01 (inclusive); the pre-2026-06-01 policy is implemented in
transfer_to_atp_form.py.
"""
from datetime import date

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import granted_work_release
from recidiviz.task_eligibility.criteria.state_specific.us_nd import (
    atp_short_sentence_placement_requirements,
)
from recidiviz.task_eligibility.eligibility_spans.us_nd.transfer_to_atp_form import (
    ALMOST_ELIGIBLE_CONDITION,
    CRITERIA_SPANS_VIEW_BUILDERS,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# V2-only criteria added by the post-2026-06-01 DOCR policy update.
# TODO(#78553): Re-add US_ND_COMPLETED_CAREER_READINESS_COURSE here once the ND
# ingest fix in #78435 lands and state_program_assignment surfaces
# DISCHARGED_SUCCESSFUL for ND. Until then the criterion would emit zero spans.
V2_ADDITIONAL_CRITERIA_SPANS_VIEW_BUILDERS = [
    atp_short_sentence_placement_requirements.VIEW_BUILDER,
]

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_ND,
    task_name="TRANSFER_TO_ATP_FORM_V2",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        *CRITERIA_SPANS_VIEW_BUILDERS,
        *V2_ADDITIONAL_CRITERIA_SPANS_VIEW_BUILDERS,
    ],
    completion_event_builder=granted_work_release.VIEW_BUILDER,
    almost_eligible_condition=ALMOST_ELIGIBLE_CONDITION,
    policy_start_date=date(2026, 6, 1),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
