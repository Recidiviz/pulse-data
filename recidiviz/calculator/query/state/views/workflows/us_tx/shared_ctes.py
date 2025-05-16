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
"""CTE Logic that is shared across US_TX Workflows queries."""

from recidiviz.ingest.direct.regions.us_tx.ingest_views.us_tx_view_query_fragments import (
    PERIOD_EXCLUSIONS_FRAGMENT,
)

TX_MAGIC_END_DATE = "DATE(9999,9,9)"

US_TX_MAX_TERMINATION_DATES = f"""
    WITH 
    tx_most_recent_max_dates_by_supervision_period AS (
        SELECT *
        FROM (
            SELECT DISTINCT 
                SID_Number,
                Period_ID_Number,
                CAST(RMF_TIMESTAMP AS DATETIME) as update_datetime,
                COALESCE(DATE(Max_termination_Date), {TX_MAGIC_END_DATE}) AS Max_termination_Date
            FROM `{{project_id}}.{{us_tx_raw_data_up_to_date_dataset}}.SupervisionPeriod_latest`
            WHERE DATE(RMF_TIMESTAMP) IS NOT NULL -- only 172 cases where this is null and in those cases there's no start or end dates for the supervision periods so we'll exclude
                AND (
                    status IS NULL
                    OR status NOT IN {PERIOD_EXCLUSIONS_FRAGMENT}
    )
        )
        QUALIFY ROW_NUMBER() OVER(PARTITION BY SID_Number ORDER BY update_datetime DESC) = 1
    ),
    tx_max_date_by_person AS (
        SELECT 
            SID_Number, 
            IF(MAX(Max_termination_Date) = {TX_MAGIC_END_DATE}, NULL, MAX(Max_termination_Date)) AS tx_max_termination_Date
        FROM tx_most_recent_max_dates_by_supervision_period
        GROUP BY SID_Number
    )
    SELECT
        person_id,
        tx_max_termination_Date
    FROM tx_max_date_by_person
    JOIN `{{project_id}}.normalized_state.state_person_external_id`
    ON SID_Number = external_id
    WHERE id_type = "US_TX_SID"
"""
