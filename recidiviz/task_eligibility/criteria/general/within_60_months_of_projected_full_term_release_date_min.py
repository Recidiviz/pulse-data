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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
# ============================================================================
"""Spans of time when someone is within 60 months of their earliest projected full-term
release date.

NB: this criterion is built on the `is_past_completion_date_criteria_builder`, which
defaults to `compartment_level_1='SUPERVISION'` if a CL1 is not passed in. In this
case, all sentences are included for states not listed in
`STATES_WITH_NO_INCARCERATION_SENTENCES_ON_SUPERVISION` from
`recidiviz.calculator.query.state.views.sessions.state_sentence_configurations`. For
states in that list, which are states that use exclusively 'PROBATION' and 'PAROLE'
sentences for supervision, only 'PROBATION' and 'PAROLE' sentences are included in the
criterion. However, when this criterion is used for a state that has incarceration
sentences on supervision, it will include all sentences, regardless of the
`sentence_type`. In those states, the criterion does not necessarily reflect whether
someone is on supervision or incarcerated at any given point in time, even though the
helper function is defaulting to `compartment_level_1='SUPERVISION'`.
"""

from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    is_past_completion_date_criteria_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "WITHIN_60_MONTHS_OF_PROJECTED_FULL_TERM_RELEASE_DATE_MIN"

# TODO(#46222) and TODO(#46236): Revisit whether we should change the underlying
# criterion builder to allow all sentences, regardless of the `sentence_type`, to be
# included without having to pass in a `compartment_level_1` value (or set that value to
# `None`). Right now, the builder is technically using
# `compartment_level_1='SUPERVISION'`, although this ends up working for MO
# incarceration too since it includes all sentences, and we want to include all
# sentences when identifying release dates.
VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    is_past_completion_date_criteria_builder(
        meets_criteria_leading_window_time=60,
        date_part="MONTH",
        critical_date_name_in_reason="earliest_release_date",
        critical_date_column="sentence_projected_full_term_release_date_min",
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
