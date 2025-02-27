# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Builder for a task eligibility spans view that shows the spans of time during which
someone in MI is eligible for a written security committee classification review from the ADD
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    transfer_out_of_administrative_solitary_confinement,
)
from recidiviz.task_eligibility.criteria.general import (
    housing_unit_type_is_administrative_solitary_confinement,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mi import (
    not_past_add_in_person_review_for_scc_date,
    past_add_written_review_for_scc_date,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which someone in MI is eligible for a written
security classification committee review from the ADD"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    task_name="COMPLETE_ADD_WRITTEN_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM",
    description=_DESCRIPTION,
    candidate_population_view_builder=incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        past_add_written_review_for_scc_date.VIEW_BUILDER,
        not_past_add_in_person_review_for_scc_date.VIEW_BUILDER,
        housing_unit_type_is_administrative_solitary_confinement.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_out_of_administrative_solitary_confinement.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
