# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Creates a view for collapsing raw ID employment data into contiguous periods of employment or unemployment overlapping with a supervision super session"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_EMPLOYMENT_STATUS_SESSIONS_VIEW_NAME = (
    "supervision_employment_status_sessions"
)

SUPERVISION_EMPLOYMENT_STATUS_SESSIONS_VIEW_DESCRIPTION = """View of continuous periods of unemployment or employment (including any alternate occupation status) overlapping with time on supervision"""

SUPERVISION_EMPLOYMENT_STATUS_SESSIONS_QUERY_TEMPLATE = """
    #TODO(#12307): Replace unnesting logic with efficient sessionization template.
    WITH date_array AS (
        SELECT *
        FROM 
            UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 20 YEAR), CURRENT_DATE("US/Eastern"))) date 
    )
    ,
    employment_daily_unnested AS (
    /* Unnests employment periods into daily employment status per person */
        SELECT DISTINCT
            s.person_id,
            s.state_code,
            date,
            
            # Client has an `is_employed` flag of TRUE if they have any recorded occupation that does not explicitly
            # indicate unemployment. This could include part-time employment, student status, conditions resulting in
            # inability to work, and other valid statuses (e.g., retired, volunteer, homemaker, etc.)
            LOGICAL_AND(COALESCE(e.employment_status != "UNEMPLOYED", TRUE)) AS is_employed,
            
            # If no employment periods are open on this day, we assume unemployment -- mark the status on these days as inferred.
            LOGICAL_AND(e.start_date IS NULL) AS is_inferred,
            
            # Use the original earliest date of employment, for cases where employment began before supervision start
            MIN(e.start_date) AS earliest_employment_period_start_date,
            
            # Use the most recent verification date associated with all active employment periods on a single day
            MAX(e.last_verified_date) AS last_verified_date,
        FROM 
            date_array d
        #TODO(#12724): Use a supervision sessions view that includes all supervision periods including those overlapping with incarceration
        INNER JOIN 
            `{project_id}.{sessions_dataset}.compartment_level_0_super_sessions_materialized` s
        ON 
            d.date BETWEEN s.start_date AND COALESCE(DATE_SUB(s.end_date, INTERVAL 1 DAY), CURRENT_DATE("US/Eastern"))
        LEFT JOIN 
            `{project_id}.{normalized_state_dataset}.state_employment_period` e
        ON 
            d.date BETWEEN e.start_date AND COALESCE(e.end_date, CURRENT_DATE("US/Eastern"))
            AND s.person_id = e.person_id
            AND s.state_code = e.state_code
        WHERE 
            s.state_code IN ("US_IX", "US_AR")
            AND s.compartment_level_0 = "SUPERVISION"
        GROUP BY 1, 2, 3
    )
    ,
    employment_end_reasons AS (
    /* Unions employment period end reasons with transitions to incarceration for the full set of deduped employment end reasons */
        SELECT *
        FROM (
            SELECT DISTINCT
                person_id,
                end_date AS employment_end_date_exclusive,
                end_reason AS employment_end_reason,
            FROM 
                `{project_id}.{normalized_state_dataset}.state_employment_period`
            UNION ALL
            SELECT
                person_id,
                DATE_SUB(start_date, INTERVAL 1 DAY) AS employment_end_date_exclusive,
                "INCARCERATED" AS employment_end_reason
            FROM
                `{project_id}.{sessions_dataset}.compartment_level_0_super_sessions_materialized`
            WHERE
                compartment_level_0 = 'INCARCERATION'
        )
        # Prioritize INCARCERATED end reason, and dedup to one employment end reason per person-date
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY person_id, employment_end_date_exclusive
            ORDER BY CASE WHEN employment_end_reason = "INCARCERATED" THEN 0 ELSE 1 END, employment_end_reason
        ) = 1
    )
    ,
    employment_sessionized AS (
        SELECT
            person_id,
            state_code,
            employment_session_id,
            MIN(date) AS employment_status_start_date,
            NULLIF(MAX(date), CURRENT_DATE("US/Eastern")) AS employment_status_end_date_exclusive,
            NULLIF(MAX(date), CURRENT_DATE("US/Eastern")) AS employment_status_end_date,
            MIN(earliest_employment_period_start_date) AS earliest_employment_period_start_date,
            MAX(last_verified_date) AS last_verified_date,
            ANY_VALUE(is_employed) AS is_employed,
            COUNTIF(is_inferred) AS unemployment_inferred_days,
        FROM (
            SELECT
                *,
                SUM(IF(new_session OR date_gap,1,0)) OVER (PARTITION BY person_id ORDER BY date) AS employment_session_id
            FROM (
                SELECT
                    *,
                    # Count as a new session if the employment status changed or if there was a gap in unnested employment status
                    COALESCE(LAG(is_employed) OVER (PARTITION BY person_id ORDER BY date) != is_employed, TRUE) AS new_session,
                    LAG(date) OVER (PARTITION BY person_id ORDER BY date) != DATE_SUB(date, INTERVAL 1 DAY) AS date_gap,
                FROM employment_daily_unnested
            )
        )
        GROUP BY 1,2,3
    )
    SELECT a.*, b.employment_end_reason AS employment_status_end_reason
    FROM 
        employment_sessionized a
    LEFT JOIN
        employment_end_reasons b
    ON 
        a.person_id = b.person_id
        AND a.employment_status_end_date = b.employment_end_date_exclusive    
"""

SUPERVISION_EMPLOYMENT_STATUS_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SUPERVISION_EMPLOYMENT_STATUS_SESSIONS_VIEW_NAME,
    description=SUPERVISION_EMPLOYMENT_STATUS_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=SUPERVISION_EMPLOYMENT_STATUS_SESSIONS_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_EMPLOYMENT_STATUS_SESSIONS_VIEW_BUILDER.build_and_print()
