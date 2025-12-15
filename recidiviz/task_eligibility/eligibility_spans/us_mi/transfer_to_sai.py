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
"""Builder for a task eligibility spans view that shows the spans of time during which
someone in MI is eligible to transfer to the SAI (Special Alternative Incarceration) program.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    transfer_to_supervision_run_facility,
)
from recidiviz.task_eligibility.criteria.general import (
    no_violent_sexual_offenses,
    serving_first_prison_sentence,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mi import (
    minimum_sentence_length_eligible_for_sai,
    no_pending_felony_or_immigration_detainer,
    not_serving_ineligible_offenses_for_sai,
    screening_not_high_assault_risk,
    served_minimum_sentence_for_controlled_substances,
    served_minimum_sentence_for_firearm_felony,
    true_security_level_not_iv_or_v,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    task_name="TRANSFER_TO_SAI",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        no_violent_sexual_offenses.VIEW_BUILDER,
        serving_first_prison_sentence.VIEW_BUILDER,
        minimum_sentence_length_eligible_for_sai.VIEW_BUILDER,
        no_pending_felony_or_immigration_detainer.VIEW_BUILDER,
        not_serving_ineligible_offenses_for_sai.VIEW_BUILDER,
        screening_not_high_assault_risk.VIEW_BUILDER,
        served_minimum_sentence_for_controlled_substances.VIEW_BUILDER,
        served_minimum_sentence_for_firearm_felony.VIEW_BUILDER,
        true_security_level_not_iv_or_v.VIEW_BUILDER,
    ],
    # The TRANSFER_TO_TREATMENT_IN_PRISON enum was deprecated but
    # could be re-added in the future if this opportunity is revisited
    completion_event_builder=transfer_to_supervision_run_facility.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
