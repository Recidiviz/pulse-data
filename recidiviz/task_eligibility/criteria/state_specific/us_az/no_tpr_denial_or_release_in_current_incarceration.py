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
"""Shows the eligibility spans that describe when a resident has not been denied TPR nor been released
on TPR during their current period of incarceration.
"""
from recidiviz.task_eligibility.criteria.state_specific.us_az import (
    no_tpr_denial_in_current_incarceration,
    no_transition_release_in_current_sentence_span,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name="US_AZ_NO_TPR_DENIAL_OR_RELEASE_IN_CURRENT_INCARCERATION",
    sub_criteria_list=[
        no_tpr_denial_in_current_incarceration.VIEW_BUILDER,
        no_transition_release_in_current_sentence_span.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
