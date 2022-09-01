#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Helper for populating missing export dates for Workflows export archive views."""


def populate_missing_export_dates() -> str:
    return """
        , start_dates_by_state AS (
            SELECT
                state_code,
                MIN(export_date) as records_start,
            FROM records_by_state_by_date
            GROUP BY 1
        )
        , all_dates_since_start AS (
            SELECT
                state_code,
                generated_export_date,
            FROM start_dates_by_state,
            UNNEST(
                GENERATE_DATE_ARRAY(
                    records_start,
                    CURRENT_DATE("US/Eastern")
                )
            ) generated_export_date
        )
        , date_to_archive_map AS (
            -- This CTE generates a mapping of every day from the start of records to the export dates in the archive
            -- record. If there is a date missing from the archive record, the export date will map to the next
            -- generated date. This is to fill in dates that are missing from the export with the previous day's data.
            SELECT
                all_dates_since_start.state_code,
                generated_export_date,
                ARRAY_AGG(export_date ORDER BY export_date DESC LIMIT 1)[OFFSET(0)] AS export_date,
            FROM all_dates_since_start
            LEFT JOIN records_by_state_by_date
                ON all_dates_since_start.state_code = records_by_state_by_date.state_code
                AND export_date <= generated_export_date
            GROUP BY 1, 2
        )
    """
