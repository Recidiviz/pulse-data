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
"""Helper functions for building BQ sessions views."""
# pylint: disable=line-too-long

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)


def create_sub_sessions_with_attributes(
    table_name: str, use_magic_date_end_dates: bool = False
) -> str:
    """Creates the `sub_sessions_with_attributes` CTE by:
    1) Creating non-overlapping sub-session time spans that cover the time period
     represented by the input sessions.
    2) For each sub-session, producing one row per input session that overlaps with this
     sub-session, including zero-day sessions, with all attribute values from that
     overlapping input session preserved.

    The |table_name| must have the following columns: state_code, person_id, start_date,
    and end_date.

    Sessions must be end-date exclusive such that the end date of one session is equal
    to the start date of the adjacent session.

    If |use_magic_date_end_dates| is True then open sub-sessions will have the
    MAGIC_END_DATE end_date, otherwise open sub-sessions will have a NULL end_date."""
    return f"""
/*
Create the periods CTE with non-null end dates for easier date logic
*/
periods_cte AS (
    SELECT
        input.* EXCEPT(end_date),
        {nonnull_end_date_clause('input.end_date')} AS end_date,
    FROM {table_name} input
),
/*
Start creating the new smaller sub-sessions with boundaries for every session date.
Generate the full list of the new sub-session start dates including all unique start
dates and any end dates that overlap another session.
*/
start_dates AS (
    SELECT DISTINCT
        state_code,
        person_id,
        start_date,
    FROM periods_cte
    UNION DISTINCT
    SELECT DISTINCT
        orig.state_code,
        orig.person_id,
        new_start_dates.end_date AS start_date,
    FROM periods_cte orig
    INNER JOIN periods_cte new_start_dates
        ON orig.state_code = new_start_dates.state_code
        AND orig.person_id = new_start_dates.person_id
        AND new_start_dates.end_date
            BETWEEN orig.start_date AND DATE_SUB(orig.end_date, INTERVAL 1 DAY)
),
/*
Generate the full list of the new sub-session end dates including all unique end dates
and any start dates that overlap another session.
*/
end_dates AS (
    SELECT DISTINCT
        state_code,
        person_id,
        end_date,
    FROM periods_cte
    UNION DISTINCT
    SELECT DISTINCT
        orig.state_code,
        orig.person_id,
        new_end_dates.start_date AS end_date,
    FROM periods_cte orig
    INNER JOIN periods_cte new_end_dates
        ON orig.state_code = new_end_dates.state_code
        AND orig.person_id = new_end_dates.person_id
        AND new_end_dates.start_date
            BETWEEN orig.start_date AND DATE_SUB(orig.end_date, INTERVAL 1 DAY)
),
/*
Join start and end dates together to create smaller sub-sessions. Each start date gets
matched to the closest following end date for each person. Note that this does not
associate zero-day (same day start and end sessions) as these sessions are added
separately in a subsequent CTE.
*/
sub_sessions AS (
    SELECT
        start_dates.state_code,
        start_dates.person_id,
        start_dates.start_date,
        MIN(end_dates.end_date) AS end_date,
    FROM start_dates
    INNER JOIN end_dates
        ON start_dates.state_code = end_dates.state_code
        AND start_dates.person_id = end_dates.person_id
        AND start_dates.start_date < end_dates.end_date
    GROUP BY state_code, person_id, start_date
),
/*
Add the attributes from the original periods to the overlapping sub-sessions and union
in zero-day sessions (same-day start and end) directly from the original periods CTE.
*/
sub_sessions_with_attributes AS (
    SELECT
        se.person_id,
        se.state_code,
        se.start_date,
        {'se.end_date' if use_magic_date_end_dates else revert_nonnull_end_date_clause('se.end_date')} AS end_date,
        c.* EXCEPT (person_id, state_code, start_date, end_date),
    FROM sub_sessions se
    INNER JOIN periods_cte c
        ON c.person_id = se.person_id
        AND c.state_code = se.state_code
        AND se.start_date BETWEEN c.start_date AND DATE_SUB(c.end_date, INTERVAL 1 DAY)
    UNION ALL

    /*
    Add the zero-day sessions, which cannot be divided further into sub-sessions, along
    with *all* the attributes from the overlapping sessions which includes the
    attributes of the zero-day session and the attributes of the session(s) that the
    zero-day period overlaps with.
    */
    SELECT
        single_day.person_id,
        single_day.state_code,
        single_day.start_date,
        single_day.end_date,
        all_periods.* EXCEPT (person_id, state_code, start_date, end_date),
    FROM periods_cte single_day
    INNER JOIN periods_cte all_periods
        ON single_day.person_id = all_periods.person_id
        AND single_day.state_code = all_periods.state_code
        -- Add the attributes of the zero-day period as well as any sessions that it
        -- falls within
        AND single_day.start_date BETWEEN all_periods.start_date AND all_periods.end_date
    WHERE single_day.start_date = single_day.end_date
)
"""
