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
"""
Helper SQL queries for Arkansas
"""

from recidiviz.task_eligibility.utils.general_criteria_builders import (
    num_events_within_time_interval_spans,
)


def no_incarceration_sanctions_within_n_months(n: int) -> str:
    return f"""
    WITH incarceration_sanction_dates AS (
        SELECT
            person_id,
            state_code,
            date_effective AS event_date
        FROM
            `{{project_id}}.normalized_state.state_incarceration_incident_outcome`
        WHERE
            state_code = 'US_AR'
            AND outcome_type != 'DISMISSED'
    ),
    {num_events_within_time_interval_spans(
        events_cte="incarceration_sanction_dates",
        date_interval=n,
        date_part="MONTH"
    )}
    SELECT
        person_id,
        state_code,
        start_date,
        end_date,
        event_count = 0 as meets_criteria,
        TO_JSON(STRUCT(event_dates)) AS reason,
        event_dates,
    FROM event_count_spans
    """
