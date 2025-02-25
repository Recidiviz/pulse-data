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
"""Shows the spans of time during which someone in AR is eligible
for work release.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_ar import (
    granted_work_release,
)
from recidiviz.task_eligibility.criteria.general import (
    age_21_years_or_older,
    incarceration_within_42_months_of_parole_eligibility_date,
    no_felony_escapes,
    no_felony_fleeing_in_last_10_years,
    not_serving_a_life_sentence,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ar import (
    can_complete_one_semester_before_sentence_ends,
    eligible_for_minimum_custody_status,
    incarcerated_at_least_60_days_in_adc,
    medical_classification_aligns_with_work,
    no_filed_but_undisposed_detainers,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which someone in AR is eligible
for work release.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_AR,
    task_name="WORK_RELEASE",
    description=_DESCRIPTION,
    candidate_population_view_builder=incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        age_21_years_or_older.VIEW_BUILDER,
        incarcerated_at_least_60_days_in_adc.VIEW_BUILDER,
        incarceration_within_42_months_of_parole_eligibility_date.VIEW_BUILDER,
        not_serving_a_life_sentence.VIEW_BUILDER,
        no_felony_escapes.VIEW_BUILDER,
        no_felony_fleeing_in_last_10_years.VIEW_BUILDER,
        eligible_for_minimum_custody_status.VIEW_BUILDER,
        medical_classification_aligns_with_work.VIEW_BUILDER,
        can_complete_one_semester_before_sentence_ends.VIEW_BUILDER,
        no_filed_but_undisposed_detainers.VIEW_BUILDER,
    ],
    completion_event_builder=granted_work_release.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
