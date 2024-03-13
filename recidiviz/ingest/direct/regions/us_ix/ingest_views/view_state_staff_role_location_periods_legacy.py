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
"""Query that generates state staff role periods and location periods information."""
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

cleaned_rows_query = """
    SELECT
        empl_cd,
        empl_title,
        fac_cd,
        CASE WHEN emp.empl_stat = 'A' then '0' ELSE '1' END AS InActive,
        -- Since we got daily transfers for employee, we can use update_datetime as info_update_datetime
        update_datetime as info_update_date,
        update_datetime as file_update_datetime,
        MAX(update_datetime) OVER(PARTITION BY TRUE) as last_file_update_datetime
    FROM {employee@ALL} emp
"""

VIEW_QUERY_TEMPLATE = f"""
    WITH all_periods as (
        SELECT
            empl_cd,
            empl_title,
            fac_cd,
            CASE WHEN emp.empl_stat = 'A' then '0' ELSE '1' END AS InActive,
            update_datetime as start_date,
            LEAD(update_datetime) over(PARTITION BY empl_cd ORDER BY update_datetime) as end_date,
            MAX(update_datetime) OVER(PARTITION BY empl_cd) as last_appearance_date,
            MAX(update_datetime) OVER(PARTITION BY TRUE) as last_file_update_datetime
        FROM {{employee@ALL}} emp
    ),
    preliminary_periods as (
        SELECT
            empl_cd,
            empl_title,
            fac_cd,
            InActive,
            start_date,
            end_date
        FROM all_periods
        WHERE (start_date < last_appearance_date or start_date = last_file_update_datetime)
    ),
    final_periods as (
        {aggregate_adjacent_spans(
            table_name="preliminary_periods",
            attribute=["empl_title", "fac_cd", "InActive"],
            index_columns=["empl_cd"])}
    )
    SELECT
        empl_cd,
        UPPER(empl_title) as empl_title,
        UPPER(fac_cd) as fac_cd,
        start_date,
        -- For CIS data, let's end all staff role periods on the first Atlas data update date so that
        -- we don't end up with overlapping open periods when we see that staff memeber appear as active in Atlas
        COALESCE(end_date, CAST( DATE(2023,2,21) as DATETIME)) as end_date,
        row_number() OVER(partition by empl_cd order by start_date, end_date nulls last) as period_id
    FROM final_periods
    -- For now, narrow down to just supervision officers since that's our only current use case for StateStaff
    WHERE 
        InActive <> '1'
        AND
        (
            UPPER(empl_title) like '%P&P%' OR
            UPPER(empl_title) like '%PROBATION%' OR
            UPPER(empl_title) like '%PAROLE%' OR
            UPPER(empl_title) like '%SUPERVISION%'
        )
        AND 
        (   
            UPPER(empl_title) not like '%HEARING%' AND
            UPPER(empl_title) not like '%COMMISSION%'
        )
        -- exclude the leadership folks that are going to be ingested in a different view (state_staff_role_location_periods_leadership)
        -- (since this view is off of legacy data which is static, we know these are the only two leadership people in the view)
        AND empl_cd not in ('3538', '3178')
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="state_staff_role_location_periods_legacy",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="empl_cd, period_id",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
