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
"""Builder for a task eligiblity spans view that shows the spans of time during which
someone in ID is eligible to complete the form for transfer to limited unit supervision.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    supervision_population_not_limited_or_unsupervised,
)
from recidiviz.task_eligibility.completion_events import transfer_to_limited_supervision
from recidiviz.task_eligibility.criteria.general import (
    negative_ua_within_90_days,
    no_felony_within_24_months,
    on_supervision_at_least_one_year,
    supervision_not_past_full_term_completion_date,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    income_verified_within_3_months,
    lsir_level_low_for_90_days,
    no_active_nco,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which someone in ID is eligible
to complete the form for transfer to limited unit supervision.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    task_name="COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM",
    description=_DESCRIPTION,
    candidate_population_view_builder=supervision_population_not_limited_or_unsupervised.VIEW_BUILDER,
    criteria_spans_view_builders=[
        negative_ua_within_90_days.VIEW_BUILDER,
        lsir_level_low_for_90_days.VIEW_BUILDER,
        no_felony_within_24_months.VIEW_BUILDER,
        supervision_not_past_full_term_completion_date.VIEW_BUILDER,
        income_verified_within_3_months.VIEW_BUILDER,
        on_supervision_at_least_one_year.VIEW_BUILDER,
        no_active_nco.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_limited_supervision.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
