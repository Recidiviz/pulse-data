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
"""Shows the eligibility spans for residents in AZ who are overdue for a
Transition Program Release (TPR) release.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_az import (
    early_release_to_community_confinement_supervision_not_overdue,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az import (
    incarceration_past_acis_tpr_date,
    incarceration_within_6_months_of_acis_tpr_date,
    no_tpr_denial_or_release_in_current_incarceration,
    not_eligible_or_almost_eligible_for_overdue_for_acis_dtp,
)
from recidiviz.task_eligibility.eligibility_spans.us_az.overdue_for_acis_tpr_request import (
    NOT_INCARCERATION_WITHIN_6_MONTHS_OF_ACIS_DTP_DATE,
)
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateSpecificInvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_AZ,
    task_name="APPROACHING_ACIS_TPR_REQUEST",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        # Less than 6 months until the ACIS TPR date
        incarceration_within_6_months_of_acis_tpr_date.VIEW_BUILDER,
        # Not past the TPR date
        StateSpecificInvertedTaskCriteriaBigQueryViewBuilder(
            sub_criteria=incarceration_past_acis_tpr_date.VIEW_BUILDER,
            invert_meets_criteria_default=False,
        ),
        # Not within 6 months of an ACIS DTP date
        NOT_INCARCERATION_WITHIN_6_MONTHS_OF_ACIS_DTP_DATE,
        not_eligible_or_almost_eligible_for_overdue_for_acis_dtp.VIEW_BUILDER,
        # This is a catch for denials for TPR
        no_tpr_denial_or_release_in_current_incarceration.VIEW_BUILDER,
    ],
    completion_event_builder=early_release_to_community_confinement_supervision_not_overdue.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
