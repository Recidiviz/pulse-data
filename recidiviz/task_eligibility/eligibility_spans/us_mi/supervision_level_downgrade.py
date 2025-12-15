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
"""Shows the spans of time during which someone in MI may be eligible to have their supervision
level downgraded.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_mi import (
    supervision_level_downgrade_before_initial_classification_review_date,
)
from recidiviz.task_eligibility.criteria.general import (
    supervision_level_is_not_diversion,
    supervision_level_is_not_internal_unknown,
    supervision_level_is_not_interstate_compact,
    supervision_level_is_not_residential_program,
    supervision_level_is_not_unassigned,
    supervision_or_supervision_out_of_state_level_is_not_high,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mi import (
    not_on_electronic_monitoring,
    not_past_initial_classification_review_date,
    not_required_to_register_under_sora,
    not_serving_ineligible_offenses_for_downgrade_from_supervision_level,
    supervision_level_higher_than_assessment_level,
    supervision_level_is_not_modified,
    supervision_or_supervision_out_of_state_level_is_not_sai,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    task_name="SUPERVISION_LEVEL_DOWNGRADE",
    description=__doc__,
    candidate_population_view_builder=active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        supervision_level_higher_than_assessment_level.VIEW_BUILDER,
        not_past_initial_classification_review_date.VIEW_BUILDER,
        not_serving_ineligible_offenses_for_downgrade_from_supervision_level.VIEW_BUILDER,
        not_required_to_register_under_sora.VIEW_BUILDER,
        not_on_electronic_monitoring.VIEW_BUILDER,
        supervision_or_supervision_out_of_state_level_is_not_sai.VIEW_BUILDER,
        supervision_level_is_not_internal_unknown.VIEW_BUILDER,
        supervision_level_is_not_interstate_compact.VIEW_BUILDER,
        supervision_level_is_not_unassigned.VIEW_BUILDER,
        supervision_level_is_not_diversion.VIEW_BUILDER,
        supervision_or_supervision_out_of_state_level_is_not_high.VIEW_BUILDER,
        supervision_level_is_not_modified.VIEW_BUILDER,
        supervision_level_is_not_residential_program.VIEW_BUILDER,
    ],
    completion_event_builder=supervision_level_downgrade_before_initial_classification_review_date.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
