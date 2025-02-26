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
"""Creates a view that calculates periods of time during which a person on Parole in
California had a consistent sustainable housing status."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    US_CA_RAW_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_NAME = (
    "us_ca_sustainable_housing_status_periods"
)

US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_DESCRIPTION = "Creates a view that calculates periods of time during which a person on Parole in California had a consistent sustainable housing status."

US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_QUERY_TEMPLATE = """
WITH formatted_cte AS (
    SELECT *
    FROM (
        SELECT DISTINCT
            OffenderId,
            CAST(ADDREFFECTIVEDATE AS DATETIME) AS start_date,
            CAST(IFNULL(ADDRENDDATE, '9999-12-31') AS DATETIME) AS end_date,
            AddressTypeDesc,
        FROM `{project_id}.{us_ca_raw_dataset}.ParoleHousing_latest`
    )
    -- Drop obviously incoherent periods before going any further
    WHERE start_date < end_date
        -- Drop rows about future address information and old/conversion data
        AND AddressTypeDesc NOT IN ('Planned (after release)', 'Conversion', 'Planned (after ISC Transfer)')
),
-- Following the approach of sessions, build a CTE with the smallest discrete time
-- periods based on all (potentially overlapping) periods in raw data
transitions_cte AS (
    SELECT DISTINCT
        OffenderId,
        start_date AS transition_date,
    FROM formatted_cte

    UNION DISTINCT

    SELECT DISTINCT
      OffenderId,
      end_date AS transition_date,
    FROM formatted_cte
),
-- build proto-periods
periods_cte AS (
    SELECT *
    FROM (
        SELECT
            OffenderId,
            transition_date AS start_date,
            LEAD(transition_date) OVER (PARTITION BY OffenderId ORDER BY transition_date) AS end_date,
        FROM transitions_cte
    )
    WHERE end_date IS NOT NULL
),
-- Join back to raw data to assign a housing type to each time period. In cases with overlapping
-- housing information, use the most recent housing type assigned to a person
periods_with_attributes AS (
    SELECT DISTINCT
        p.OffenderId,
        p.start_date,
        p.end_date,
        c.AddressTypeDesc,
        CASE
            WHEN c.AddressTypeDesc IN ("Community Program", "Physical (Home)") THEN 1
            ELSE 0
        END AS sustainable_housing,
    FROM periods_cte p
    LEFT JOIN formatted_cte c
        ON p.OffenderId = c.OffenderId
        AND p.start_date >= c.start_date
        AND p.end_date <= c.end_date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY p.OffenderId, p.start_date
        ORDER BY
            c.start_date DESC,
            -- Deterministically sort in cases of periods starting on the same day
            -- This somewhat arbitrary sort order will choose housing types that
            -- are disqualifying for SLD over more stable housing types
            CASE c.AddressTypeDesc
                WHEN 'Custody Program' THEN 0
                WHEN 'Local Jail' THEN 1
                WHEN 'Federal Custody' THEN 2
                WHEN 'Out-of-State Custody' THEN 3

                WHEN 'PAL Report Submitted' THEN 4

                WHEN 'Transient - Homeless' THEN 5
                WHEN 'Shelter Transient' THEN 6
                WHEN 'Temporary' THEN 7

                WHEN 'Mailing' THEN 8

                WHEN 'Community Program' THEN 9
                WHEN 'Physical (Home)' THEN 10

                ELSE 999
            END
    ) = 1
)

-- Collapse adjacent spans that are both temporally adjacent and have the same housing
-- status
SELECT DISTINCT
    OffenderId,
    MIN(start_date) OVER (w_spans) AS start_date,
    MAX(end_date) OVER (w_spans) AS end_date,
    ANY_VALUE(sustainable_housing) OVER (w_spans) AS sustainable_housing,
FROM (
    SELECT *,
        -- create temporary period IDs based on flags
        SUM(status_change) OVER (w_periods) AS status_period_id,
        SUM(temporal_gap) OVER (w_periods) AS temporal_period_id,
    FROM (
        -- Set flags for when a given period is different from the preceding period
        -- either in sustainable housing status or if there is a time gap. Either
        -- case would indicate that these periods should NOT be collapsed
        SELECT *,
            IF(sustainable_housing = LAG(sustainable_housing) OVER (w_flags), 0, 1) AS status_change,
            IF(start_date = LAG(end_date) OVER (w_flags), 0, 1) AS temporal_gap,
        FROM periods_with_attributes
        WINDOW w_flags AS (
            PARTITION BY OffenderId
            ORDER BY start_date, end_date
        )
    )
    WINDOW w_periods AS (
        PARTITION BY OffenderId
        ORDER BY start_date, end_date
    )
)
WINDOW w_spans AS (
    PARTITION BY OffenderId, status_period_id, temporal_period_id
)

ORDER BY OffenderId, start_date, end_date
"""

US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    us_ca_raw_dataset=US_CA_RAW_DATASET,
    view_id=US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_NAME,
    description=US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_DESCRIPTION,
    view_query_template=US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_BUILDER.build_and_print()
