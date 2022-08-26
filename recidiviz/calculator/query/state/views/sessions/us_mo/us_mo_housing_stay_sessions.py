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
"""Sessionization and deduplication of MO housing stays"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_HOUSING_STAY_SESSIONS_VIEW_NAME = "us_mo_housing_stay_sessions"

US_MO_HOUSING_STAY_SESSIONS_VIEW_DESCRIPTION = (
    """Sessionization and deduplication of MO housing stays"""
)

US_MO_HOUSING_STAY_SESSIONS_QUERY_TEMPLATE = f"""
    /* {{description}} */
    --TODO(#14709): Templatize sub-sessionization query
    WITH confinement_type_priority_cte AS
    /*
    CTE that is used to deduplicate in cases where a person is listed in more than one confinement type for a period
    of time.
    */
    (
    SELECT
      *
    FROM UNNEST(['SOLITARY_CONFINEMENT','GENERAL','INTERNAL_UNKNOWN','EXTERNAL_UNKNOWN']) AS confinement_type
    WITH offset AS confinement_type_priority
    )
    ,
    stay_type_priority_cte AS 
    /*
    Secondary deduplication CTE that is used to deduplicate in cases where a person is listed in more than one stay type 
    within the same confinement type for a period of time.
    */
    (  
    SELECT
      *
    FROM UNNEST(['PERMANENT','TEMPORARY','INTERNAL_UNKNOWN','EXTERNAL_UNKNOWN']) AS stay_type
    WITH offset AS stay_type_priority
    )
    ,
    periods_cte AS
    /*
    This CTE is the overlapping input periods, but with non-null end dates and with the deduplication priority columns
    added
    */
    (
    SELECT 
        c.* EXCEPT(end_date),
        {nonnull_end_date_clause('end_date')} AS end_date,
        COALESCE(confinement_type_priority,999) AS confinement_type_priority,
        COALESCE(stay_type_priority,999) AS stay_type_priority,
    FROM `{{project_id}}.{{sessions_dataset}}.us_mo_housing_stays_preprocessed` c
    LEFT JOIN confinement_type_priority_cte 
        USING(confinement_type)
    LEFT JOIN stay_type_priority_cte
        USING(stay_type)
    )
    ,
    start_dates AS
    /*
    Start creating the new smaller sub-sessions with boundaries for every attribute session date. Generate the full 
    list of the new sub-session start dates including all unique attribute start dates and any end dates that overlap
    another session.
    */
    (
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
    )
    ,
    end_dates AS 
    /*
    Generate the full list of the new sub-session end dates including all unique attribute end dates and any start 
    dates that overlap another session.
    */
    (
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
    sub_sessions AS
    /*
    Join start and end dates together to create smaller sub-sessions. Each start date gets matched to the closest
    following end date for each person. Note that this does not associate zero-day (same day start and end sessions).
    These sessions are added in separately in a subsequent CTE.
    */
    (
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
    )
    ,
    sub_sessions_with_attributes AS
    /*
    Takes the newly created sub-sessions and joins back to the original periods cte (based on date overlap) to get the 
    attributes that apply for that date-span. This CTE also unions in zero-day sessions (same-day start and end), which 
    get pulled in separately from the original periods CTE. 
    */
    (
    SELECT 
        se.person_id,
        se.state_code,
        se.start_date,
        se.end_date,
        c.facility_code,
        c.stay_type,
        c.confinement_type,
        c.confinement_type_priority,
        c.stay_type_priority,
    FROM sub_sessions se
    INNER JOIN periods_cte c
        ON c.person_id = se.person_id
        AND c.state_code = se.state_code
        AND se.start_date BETWEEN c.start_date AND DATE_SUB(c.end_date, INTERVAL 1 DAY)

    UNION ALL 
    
    /* 
    This is the separate logic for bringing in zero-day sessions. These zero-day sessions are never divided into new
    sub-sessions. However, in order to deduplicate zero-day sessions correctly, we do need to pull *all* the attributes
    from the periods CTE, which includes not only the attributes of the zero-day session, but also the attributes of 
    the session that the zero-day period overlaps with. This means that when a zero-day session falls within another 
    session, we create a duplicate for that day that contains both the attributes of the zero-day session itself as well
    as the overlapping session. The deduplication then happens in the following CTE.
    */
    SELECT
        single_day.person_id,
        single_day.state_code,
        single_day.start_date,
        single_day.end_date,
        all_periods.facility_code,
        all_periods.stay_type,
        all_periods.confinement_type,
        all_periods.confinement_type_priority,
        all_periods.stay_type_priority,
    FROM periods_cte single_day
    INNER JOIN periods_cte all_periods
        ON single_day.person_id = all_periods.person_id
        AND single_day.state_code = all_periods.state_code
        --This condition pulls in the attributes of the zero-day session itself as well as the session that it falls
        --within.
        AND single_day.start_date BETWEEN all_periods.start_date AND all_periods.end_date
    WHERE single_day.start_date = single_day.end_date
    )
    ,
    sessions_with_attributes_dedup AS
    /*
    Duplicate spans are created any time that there is more than one attribute value that applies for a given date 
    range. In this specific case, we know that can happen where someone is listed as GENERAL and SOLITARY_CONFINEMENT
    simultaneously. This CTE deduplicates so that all date-ranges are unique and prioritizes the session based on 
    the deduplication priority defined in the first two CTEs.
    */
    (
    SELECT 
      *
    FROM sub_sessions_with_attributes
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date 
        ORDER BY confinement_type_priority, stay_type_priority, facility_code) = 1
    )
    /*
    At this point we now have non-overlapping sessions and can just use our standard sessionization logic to aggregate
    together temporally adjacent sessions with identical attributes.
    */
    SELECT
        person_id,
        state_code,
        housing_stay_session_id,
        MIN(start_date) AS start_date,
        {revert_nonnull_end_date_clause('MAX(end_date)')} AS end_date,
        facility_code,
        stay_type,
        confinement_type,
    FROM
        (
        SELECT 
            *,
            SUM(IF(date_gap OR attribute_change,1,0)) OVER(PARTITION BY person_id ORDER BY start_date, end_date) AS housing_stay_session_id,
        FROM
        (
        SELECT
            *,
            COALESCE(CONCAT(facility_code, stay_type, confinement_type) 
              != LAG(CONCAT(facility_code, stay_type, confinement_type)) OVER w, TRUE) AS attribute_change,
            COALESCE(LAG(end_date) OVER w != start_date,TRUE) AS date_gap
        FROM sessions_with_attributes_dedup
        WINDOW w AS (PARTITION BY person_id ORDER BY start_date, end_date)
        )
      )
    GROUP BY 1,2,3,6,7,8
"""

US_MO_HOUSING_STAY_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_MO_HOUSING_STAY_SESSIONS_VIEW_NAME,
    sessions_dataset=SESSIONS_DATASET,
    description=US_MO_HOUSING_STAY_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=US_MO_HOUSING_STAY_SESSIONS_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_HOUSING_STAY_SESSIONS_VIEW_BUILDER.build_and_print()
