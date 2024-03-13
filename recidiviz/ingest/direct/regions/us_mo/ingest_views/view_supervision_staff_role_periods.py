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
"""Query containing APFX supervision staff role period information."""

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH apfx90_periods AS (
    SELECT DISTINCT 
        BDGNO, 
        CLSTTL, 
        CASE WHEN LENGTH(STRDTE) = 5 THEN CONCAT('0', STRDTE) ELSE STRDTE END AS start_date,
        # TODO(#21919) - Two of the start dates seem to be missing a leading zero. We should confirm this, and then
        # probably remove this logic in favor of a raw data migration.
        ENDDTE AS end_date, 
        0 AS source_rank 
        # TODO(#21919) - Currently, when APFX90 and APFX91 have periods starting on the same date, the period from APFX91 is given preference. 
        # Verify that the sources should be ranked this way, and that APFX91 should have the higher rank.
    FROM {{LBCMDATA_APFX90}} 
    WHERE BDGNO IS NOT NULL
),
apfx91_periods AS (
    SELECT DISTINCT
        BDGNO,
        CLSTTL,
        CASE WHEN LENGTH(STRDTE) = 5 THEN CONCAT('0', STRDTE) ELSE STRDTE END AS start_date,
        '0' AS end_date,
        1 AS source_rank 
    FROM {{LBCMDATA_APFX91}}
    WHERE BDGNO IS NOT NULL
),
apfx_all AS (
    -- Unions the unique role periods from both APFX tables, filters out periods with missing roles or start dates,
    -- and addresses periods that are overlapping/inconsistent across the two APFX tables.
    SELECT 
        BDGNO,
        CLSTTL,
        start_date,
        NULLIF(end_date,'0') AS end_date 
    FROM (
        SELECT *, 
            MAX(source_rank) OVER(PARTITION BY BDGNO, CLSTTL, start_date) AS max_rank 
            -- In some rare cases, the two tables have role periods for the same individual with the same
            -- start date but with a non-null exit date in APFX90 (whereas all the periods in APFX91 are ongoing). 
            -- In these cases, we can assume that one of the two tables has incorrect period information; to avoid 
            -- replicating the error and to prevent role periods from overlapping, we only keep the periods from the 
            -- table treated assigned a higher source_rank (currently APFX91).
        FROM (
            SELECT * 
            FROM apfx90_periods 
            UNION DISTINCT (
                SELECT * FROM apfx91_periods
            )
        ) apfx_union
    ) ranked_union
    WHERE source_rank = max_rank AND
        CLSTTL IS NOT NULL AND 
        start_date IS NOT NULL
),
collapsed AS (
    -- Collapses bordering periods from the previous CTE into one period.
    {aggregate_adjacent_spans(
        table_name="apfx_all",
        attribute=["CLSTTL"],
        index_columns=["BDGNO"])}
)
SELECT 
    BDGNO,
    CLSTTL,
    start_date,
    end_date,
    ROW_NUMBER() OVER (PARTITION BY BDGNO ORDER BY start_date) AS SEQ
FROM collapsed
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="supervision_staff_role_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="BDGNO",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
