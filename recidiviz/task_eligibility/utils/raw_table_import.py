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
"""Helper SQL fragments for ME
"""

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_start_date_clause,
    revert_nonnull_end_date_clause,
)


def cis_319_term_cte() -> str:
    """Helper method that returns a CTE with the CIS_319_TERM_latest table
    and the following:
        - Merged with recidiviz ids
        - Dates instead of datetimes
        - Drops zero-day sessions
        - Drops start dates that come after end_dates
        - Transform magic start and end dates to NULL
    """
    return f"""term_cte AS (
    -- Term data with intake date and expected release date
    SELECT 
        state_code, 
        person_id,
        {revert_nonnull_end_date_clause('start_date')} AS start_date,
        {revert_nonnull_end_date_clause('end_date')} AS end_date,
    FROM (
            SELECT
                *,
                {nonnull_start_date_clause('SAFE_CAST(LEFT(intake_date, 10) AS DATE)')} AS start_date,
                {nonnull_end_date_clause('SAFE_CAST(LEFT(Curr_Cust_Rel_Date, 10) AS DATE)')} AS end_date,  
                -- TODO(#16175) should ingest the expected release date sometime soon
            FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_319_TERM_latest`
            -- Using INNER JOIN here does drop a handful of people who for some reason don't have `person_id` values
            INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id`
                ON Cis_100_Client_Id = external_id
                AND id_type = 'US_ME_DOC'
         )
    WHERE start_date != end_date -- Drop if intake date is = Expected release date
        AND start_date < end_date -- Drop if end_datetime is before start_datetime
)"""
