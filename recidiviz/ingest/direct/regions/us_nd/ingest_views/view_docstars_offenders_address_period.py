# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Query containing offender information."""
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH
-- The result of this CTE is the same as the raw data table docstars_offenders, but with 
-- an additional field preprocessed to identify rows that are prison / jail addresses in ND. 
-- We do this to avoid mistaking those for personal addresses.
 annotated_rows AS (
    SELECT 
        SID,
        ITAGROOT_ID,
        ADDRESS_UPPERCASE,
        CITY_UPPERCASE,
        COUNTY_RESIDENCE,
        STATE_UPPERCASE,
        ZIP,
        update_datetime,
        (
            -- Known ND prison / jail addresses
            ADDRESS_UPPERCASE NOT LIKE '%3100 RAILROAD AVE%'
            AND ADDRESS_UPPERCASE NOT LIKE '%NDSP%'
            AND ADDRESS_UPPERCASE NOT LIKE '%PO BOX 5521%'
            AND ADDRESS_UPPERCASE NOT LIKE '%440 MCKENZIE ST%'
            AND ADDRESS_UPPERCASE NOT LIKE '%ABSCOND%'
            AND ADDRESS_UPPERCASE NOT LIKE '%250 N 31ST%'
            AND ADDRESS_UPPERCASE NOT LIKE '%461 34TH ST S%'
            AND ADDRESS_UPPERCASE NOT LIKE '%1600 2ND AVE SW%'
            AND ADDRESS_UPPERCASE NOT LIKE '%702 1ST AVE S%'
            AND ADDRESS_UPPERCASE NOT LIKE '%311 S 4TH ST STE 101%'
            AND ADDRESS_UPPERCASE NOT LIKE '%222 WALNUT ST W%'
            AND ADDRESS_UPPERCASE NOT LIKE '%709 DAKOTA AVE STE D%'
            AND ADDRESS_UPPERCASE NOT LIKE '%113 MAIN AVE E STE B%'
            AND ADDRESS_UPPERCASE NOT LIKE '%712 5TH AVE%'
            AND ADDRESS_UPPERCASE NOT LIKE '%705 EAST HIGHLAND DR. SUITE B%'
            AND ADDRESS_UPPERCASE NOT LIKE '%705 E HIGHLAND DR STE B%'
            AND ADDRESS_UPPERCASE NOT LIKE '%135 SIMS ST STE 205%'
            AND ADDRESS_UPPERCASE NOT LIKE '%638 COOPER AVE%'
            AND ADDRESS_UPPERCASE NOT LIKE '%206 MAIN ST W%'
            AND ADDRESS_UPPERCASE NOT LIKE '%519 MAIN ST STE 8%'
            AND ADDRESS_UPPERCASE NOT LIKE '%115 S 5TH ST STE A%'
            AND ADDRESS_UPPERCASE NOT LIKE '%117 HWY 49%'
            -- Garbage addresses
            AND ADDRESS_UPPERCASE NOT LIKE '%XXX%'
            AND ADDRESS_UPPERCASE NOT LIKE '%***%'
            AND CITY_UPPERCASE NOT IN ('ABSCONDER', 'ABSCONDED')
            -- General filter for addresses that seem to be correctional facilities of some sort
            AND ADDRESS_UPPERCASE NOT LIKE '%JAIL%'
            AND ADDRESS_UPPERCASE NOT LIKE '%PRISON%'
            AND ADDRESS_UPPERCASE NOT LIKE '%CCC%'
            AND ADDRESS_UPPERCASE NOT LIKE '%PENITENTIARY%'
        )  AS is_valid_address
    FROM (
        SELECT DISTINCT
            SID, ITAGROOT_ID,
            UPPER(ADDRESS) AS ADDRESS_UPPERCASE,
            UPPER(CITY) AS CITY_UPPERCASE,
            COUNTY_RESIDENCE,
            UPPER(STATE) AS STATE_UPPERCASE,
            ZIP,
            CAST(update_datetime AS DATETIME) AS update_datetime
        FROM {{docstars_offenders@ALL}}
    ) people
), 
-- Create proto-periods with each person's address during each span of time. 
preliminary_periods AS (
SELECT
    SID,
    ITAGROOT_ID,
    IF(is_valid_address, ADDRESS_UPPERCASE, NULL) AS ADDRESS,
    IF(is_valid_address, CITY_UPPERCASE, NULL) AS CITY,
    IF(is_valid_address, COUNTY_RESIDENCE, NULL) AS COUNTY_CODE,
    IF(is_valid_address, STATE_UPPERCASE, NULL) AS STATE,
    IF(is_valid_address, ZIP, NULL) AS ZIP,
    update_datetime AS start_date,
    LEAD(update_datetime) OVER (PARTITION BY SID ORDER BY update_datetime) AS end_date
FROM annotated_rows),

-- Aggregate adjacent periods in which a person's address did not change.
aggregated_periods AS (
    {aggregate_adjacent_spans(
        table_name="preliminary_periods",
        attribute=["ADDRESS", "CITY", "COUNTY_CODE", "STATE", "ZIP"],
        index_columns=["SID"])}
)

SELECT * FROM aggregated_periods
WHERE ADDRESS IS NOT NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="docstars_offenders_address_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
