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
"""
Shows the spans of time during which someone in ID is eligible
for a transfer to a Community Reentry Center (CRC) for work-release.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_ix import (
    granted_work_release,
)
from recidiviz.task_eligibility.criteria.general import (
    custody_level_is_minimum,
    incarceration_not_within_6_months_of_full_term_completion_date,
    not_in_treatment_in_prison,
    not_serving_for_sexual_offense,
    not_serving_for_violent_offense,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    crc_work_release_time_based_criteria,
    no_absconsion_escape_and_eluding_police_offenses_within_10_years,
    no_sex_offender_alert,
    not_detainers_for_xcrc_and_crc,
    not_serving_a_rider_sentence,
    tentative_parole_date_not_within_6_months,
)
from recidiviz.task_eligibility.criteria_condition import NotEligibleCriteriaCondition
from recidiviz.task_eligibility.eligibility_spans.us_ix.transfer_to_crc_resident_worker_request import (
    US_IX_NOT_IN_CRC_FACILITY_VIEW_BUILDER,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """
Shows the spans of time during which someone in ID is eligible
for a transfer to a Community Reentry Center (CRC) for work-release.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    task_name="TRANSFER_TO_CRC_WORK_RELEASE_REQUEST",
    description=_DESCRIPTION,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        custody_level_is_minimum.VIEW_BUILDER,
        not_detainers_for_xcrc_and_crc.VIEW_BUILDER,
        not_serving_for_sexual_offense.VIEW_BUILDER,
        no_absconsion_escape_and_eluding_police_offenses_within_10_years.VIEW_BUILDER,
        # TODO(#24043) - remove this criteria and add one where we only filter out those already on wr
        US_IX_NOT_IN_CRC_FACILITY_VIEW_BUILDER,
        crc_work_release_time_based_criteria.VIEW_BUILDER,
        not_in_treatment_in_prison.VIEW_BUILDER,
        no_sex_offender_alert.VIEW_BUILDER,
        not_serving_for_violent_offense.VIEW_BUILDER,
        incarceration_not_within_6_months_of_full_term_completion_date.VIEW_BUILDER,
        tentative_parole_date_not_within_6_months.VIEW_BUILDER,
        not_serving_a_rider_sentence.VIEW_BUILDER,
    ],
    completion_event_builder=granted_work_release.VIEW_BUILDER,
    almost_eligible_condition=NotEligibleCriteriaCondition(
        criteria=not_serving_for_violent_offense.VIEW_BUILDER,
        description="Serving a sentence for a violent offense",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
