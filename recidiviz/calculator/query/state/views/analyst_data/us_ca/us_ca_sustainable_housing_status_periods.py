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

# Declaring this locally since we probably don't want to make a habit of using non-latest
# views outside of ingest
US_CA_RAW_ALL_DATASET = "us_ca_raw_data"

US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_NAME = (
    "us_ca_sustainable_housing_status_periods"
)

US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_DESCRIPTION = "Creates a view that calculates periods of time during which a person on Parole in California had a consistent sustainable housing status."

US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_QUERY_TEMPLATE = """
WITH
/*
    Each row in this CTE represents one person's housing status for each day
    we received the raw data file PersonParole. The following AddressType values
    are classified as sustainable housing:
        - Community Program
        - Physical (Home)

    All other values of AddressType, including NULL, will return a 0 for the 
    sustainable_housing field.
*/
raw_address_data AS (
    SELECT
        OffenderId,
        update_datetime,
        CASE
            WHEN AddressType IN ("Community Program", "Physical (Home)") THEN 1
            ELSE 0
        END AS sustainable_housing,
    FROM `{project_id}.{raw_all_dataset}.PersonParole`
),
/*
    Handles some date parsing and replaces NULL end dates with '9999-01-31'
*/
-- TODO(#20888): Use StateSupervisionPeriod once it is ingested
IncarcerationParole_parsed AS (
    -- Use DISTINCT because it's possible to get some duplicates from this query
    -- which will cause problems later on with the LEAD/LAG functions
    SELECT DISTINCT
        OffenderId,
        DATE(out_date) AS out_date,
        IFNULL(DATE(ParoleEndDate), DATE('9999-01-31')) AS ParoleEndDate,
    FROM `{project_id}.{raw_dataset}.IncarcerationParole_latest`
),
/*
    The Delta file contains a record of when someone left the Parole population (for any number of reasons).
    For these purposes we are only concerned with the fact that someone left and WHEN they left. However,
    we exclude "Date Change" because these records refer to when someone is removed from the population of
    people who will become eligible for Parole within 90 days.

    This CTE will be used to close open periods from the IncarcerationParole table.
*/
delta_file AS (
    SELECT
        OffenderId,
        DATE(StatusDate) AS StatusDate,
        OffenderGroup,
    FROM `{project_id}.{raw_dataset}.Delta_latest`
    WHERE OffenderGroup != 'Date Change'
),
/*
    Uses the raw data table IncarcerationParole as the source of truth for when someone is actually on Parole.
    The most recent period of Parole (almost?) always has a NULL end_date (or 9999-01-31 and in one case 9999-01-30)
    and in a few cases (~670 / 50k people) there are multiple periods of parole with no valid end date. In these cases,
    this CTE will infer an end date of a all but the latest period of parole by setting the end date to one day before the
    beginning of the subsequent parole period. The latest (current) period of parole is not adjusted, keeping the NULL end date.

    After cleanup has been done on the IncarcerationParole records, we merge them with the Delta file to properly close any open
    periods that are missing a valid end date in the IncarcerationParole data. The join condition specifies that the end date
    specified by the Delta file must be after the IncarcerationParole start date and that the IncarcerationParole period end date is
    greater than '9999-01-01' (ie an open period).

    The result of this CTE is one row per person's period of parole, with start and end dates.
*/
valid_parole_periods AS (
    SELECT
        ip.OffenderId,
        ip.parole_start_date,
        -- Overwrite the end date from IncarcerationParole if there is a matching StatusDate in the Delta file
        IFNULL(
            df.StatusDate,
            ip.inferred_parole_end_date
        ) AS inferred_parole_end_date,
    FROM (
        SELECT
            OffenderId,
            out_date AS parole_start_date,
            -- TODO(#20888): Instead of closing previous open parole periods using the out_date
            -- of the next parole period, we should probably use the in_date of the next record in
            -- the IncarcerationParole raw table
            CASE
                -- Uses > '9999-01-01' to account for the occasional edge case of 9999-01-30 instead of
                -- the more common '9999-01-31'
                -- This condition will be true only for records where there is no valid end date AND
                -- a person has multiple periods of parole
                WHEN ParoleEndDate > '9999-01-01' AND COUNT(*) OVER (PARTITION BY OffenderId) > 1 THEN
                    -- If the LEAD window function returns NULL, it means we are at the latest (current) period
                    -- of parole so we should set the end date to '9999-01-31'. Otherwise, set the end date to one
                    -- day before the start of the subsequent period.
                    IFNULL(DATE_SUB(LEAD(out_date) OVER (
                        PARTITION BY OffenderId
                        ORDER BY out_date
                    ), INTERVAL 1 DAY), '9999-01-31')
                ELSE ParoleEndDate
            END AS inferred_parole_end_date,
        FROM IncarcerationParole_parsed
    ) ip
    LEFT JOIN delta_file df
        ON ip.OffenderId = df.OffenderId
        AND df.StatusDate BETWEEN ip.parole_start_date AND ip.inferred_parole_end_date
        AND ip.inferred_parole_end_date > '9999-01-01'
    WHERE TRUE
    -- There are a few cases where the Delta file lists multiple supervision exits during an IncarcerationParole
    -- parole period. This QUALIFY selects the first StatusDate within a given parole period as the true end of
    -- the parole period.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ip.OffenderId, ip.parole_start_date
        ORDER BY df.StatusDate
    ) = 1
),
/*
    This CTE returns a subset of the raw_address_data CTE, limited to only those records that were received
    during a period of time determined to be a valid parole period. It also caluculates whether or not a person's
    sustainable housing status changed from the last time we observed their status within a given period of parole.
*/
address_data_within_valid_parole_periods AS (
    SELECT
        rad.OffenderId,
        update_datetime,
        sustainable_housing,
        CASE
            WHEN LAG(sustainable_housing) OVER (person) = sustainable_housing THEN 0
            ELSE 1
        END AS changed_sustainable_housing,
        parole_start_date,
        inferred_parole_end_date,
    FROM raw_address_data rad
    INNER JOIN valid_parole_periods vpp
        ON rad.OffenderId = vpp.OffenderId
        AND rad.update_datetime BETWEEN vpp.parole_start_date AND vpp.inferred_parole_end_date
    WINDOW person AS (
        -- Having both OffenderId and parole_start_date here will ensure that we flag both when
        -- the sustainable housing status changes AND when a record is the first time we observe
        -- a person's status during a given period of parole, as both cases should start a new
        -- sustainable housing status period.
        PARTITION BY rad.OffenderId, vpp.parole_start_date
        ORDER BY update_datetime
    )
)

/*
    Finally, return a list of distinct time periods during which a person's sustainable
    housing status was constant (either sustainable or not).

    For cases when we only received one file within a given parole period OR when a person's
    status was different from both the preceeding observation AND the next observation, we create
    a single day period.
*/

SELECT
    OffenderId,
    update_datetime AS start_date,
    -- If the LEAD window function returns NULL here it means we either have a single-day period
    -- OR we are at the final observation of an ongoing period (in which case the end date should be NULL)
    IFNULL(
        LEAD(update_datetime) OVER (
            PARTITION BY OffenderId, parole_start_date
            ORDER BY update_datetime
        ),
        IF(
            inferred_parole_end_date > '9999-01-01',
            NULL,
            update_datetime
        )
    ) AS end_date,
    sustainable_housing,
FROM address_data_within_valid_parole_periods
WHERE changed_sustainable_housing = 1
ORDER BY OffenderId, start_date
"""

US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    raw_dataset=US_CA_RAW_DATASET,
    raw_all_dataset=US_CA_RAW_ALL_DATASET,
    view_id=US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_NAME,
    description=US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_DESCRIPTION,
    view_query_template=US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_BUILDER.build_and_print()
