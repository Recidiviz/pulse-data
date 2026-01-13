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
"""Shows the eligibility spans for residents in AZ who are eligible for a 
supervision release directly into 35-day administrative supervision"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_az import (
    release_to_limited_supervision,
)
from recidiviz.task_eligibility.criteria.general import not_serving_for_sexual_offense
from recidiviz.task_eligibility.criteria.state_specific.us_az import (
    community_general_risk_level_4_or_below,
    community_violence_risk_level_6_or_below,
    mental_health_score_3_or_below,
    not_eligible_or_almost_eligible_for_overdue_for_acis_dtp,
    not_eligible_or_almost_eligible_for_overdue_for_acis_tpr,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_AZ,
    task_name="RELEASE_TO_35_ADMINISTRATIVE_SUPERVISION",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        # CSED (Community Supervision End Date) - ECRD <= 35 days
        # TODO(#38153): Add criteria
        # 22.19.1.1.1 NO BOEC (Board of Executive Clemency) granted releases
        # TODO(#38154): Add criteria
        # 22.19.1.1.2 NO Transition/Drug Transition legislative program legislative early releases
        not_eligible_or_almost_eligible_for_overdue_for_acis_dtp.VIEW_BUILDER,
        not_eligible_or_almost_eligible_for_overdue_for_acis_tpr.VIEW_BUILDER,
        # 22.19.1.1.3 NO Sex offenders (Codes F/N are acceptable)
        not_serving_for_sexual_offense.VIEW_BUILDER,
        # TODO(#38157): Make sure F/N codes are included in the criteria
        # 22.19.1.1.4 NO Maximum, Intensive Supervision Levels, Institution G/V scores of 5 or higher
        community_general_risk_level_4_or_below.VIEW_BUILDER,
        community_violence_risk_level_6_or_below.VIEW_BUILDER,
        # 22.19.1.1.5 NO SMI (Seriously Mentally Ill designation) or current high need General Mental Illness
        mental_health_score_3_or_below.VIEW_BUILDER,
        # TODO(#38159): Make sure we're including all of the relevant cases
    ],
    completion_event_builder=release_to_limited_supervision.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
