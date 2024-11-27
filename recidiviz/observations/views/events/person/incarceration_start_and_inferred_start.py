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
"""Event observation view documenting transitions to incarceration that were either
explicitly documented as entries into a facility or we can infer from other context.
"""
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.observations.views.events.person.incarceration_start import (
    VIEW_BUILDER as INCARCERATION_START_VIEW_BUILDER,
)
from recidiviz.observations.views.events.person.supervision_termination_with_incarceration_reason import (
    VIEW_BUILDER as SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = (
    "Transitions to incarceration that we either see explicitly documented "
    "as entries into a facility or we can infer from other context (e.g. supervision "
    "terminations with incarceration entry statuses)"
)

_SOURCE_DATA_QUERY_TEMPLATE = f"""
WITH combined_starts AS (
    SELECT
        state_code,
        person_id,
        event_date,
        is_discretionary,
        latest_active_supervision_type,
        most_severe_violation_type,
        prior_treatment_referrals_1y,
        violation_is_inferred,
    FROM `{{project_id}}.{INCARCERATION_START_VIEW_BUILDER.table_for_query.to_str()}`
    
    UNION ALL
    
    SELECT
        state_code,
        person_id,
        event_date,
        is_discretionary,
        latest_active_supervision_type,
        most_severe_violation_type,
        prior_treatment_referrals_1y,
        violation_is_inferred,
    FROM `{{project_id}}.{SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON_VIEW_BUILDER.table_for_query.to_str()}`
)
SELECT
    state_code,
    person_id,
    event_date,
    is_discretionary,
    latest_active_supervision_type,
    most_severe_violation_type,
    prior_treatment_referrals_1y,
    violation_is_inferred,
FROM combined_starts
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.INCARCERATION_START_AND_INFERRED_START,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "is_discretionary",
        "latest_active_supervision_type",
        "most_severe_violation_type",
        "prior_treatment_referrals_1y",
        "violation_is_inferred",
    ],
    event_date_col="event_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
