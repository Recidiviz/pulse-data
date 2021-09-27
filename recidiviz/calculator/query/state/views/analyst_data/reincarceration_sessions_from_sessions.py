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
"""A table of incarceration sessions that end in release with session identifiers for a subsequent reincarceration
sessions. Constructed directly from the sessions view."""
# pylint: disable=line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_NAME = (
    "reincarceration_sessions_from_sessions"
)

REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_DESCRIPTION = """

## Overview

The table `reincarceration_sessions_from_sessions_materialized` joins to `compartment_sessions` and identifies incarceration releases and subsequent reincarcerations when / if they occur. The view has a record for every person and every incarceration release -  if there is a subsequent reincarceration the `session_id` and `reincarceration_date` associated with the reincarceration session will be populated.

This table can be used to calculate reincarceration rates based on the time between release and reincarceration as well as cumulative reincarceration curves by time since release. Additionally, since the release session id and reincarceration session id are both included in this view, this table can be joined back to sessions to determine characteristics associated with the release cohort (age, gender, etc) as well as the reincarceration (crime type, inflow from, etc) by joining to sessions. 

The logic is slightly more complex than just identifying incarceration sessions that follow an incarceration release, due to the way that states can have temporary parole board holds. The guidance that we have received is that these temporary holds should not count either as reincarcerations or as releases (in cases where the parole board does not revoke the person). The reincarceration should count when / if a person is revoked to an incarceration session following a parole board hold.  As an example, let’s say the person had the following set of sessions:

1. Incarceration - General Term
2. Parole
3. Incarceration - Parole Board Hold
4. Parole
5. Incarceration - Parole Board Hold
6. Incarceration - General Term

In the above example, session 1 would be the incarceration release session and session 6 would be the subsequent reincarceration session.

## Field Definitions

|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	person_id	|	Unique person identifier	|
|	state_code	|	State	|
|	release_session_id	|	Session ID for the incarceration session from which the person was released from. This includes any incarceration session (except for temporary parole board holds) that transition to release or supervision compartments	|
|	release_date	|	Date that the person was released (one day after the release session end date)	|
|	days_since_release	|	Days between the person's release date and the last day of data	|
|	months_since_release	|	Calendar count of full months between the last day of data and the release date. This field is generally used to identify the cohort to be used for a particular calculation (for example 12 month recidivism requires a cohort that has been released for at least 12 months). We can select that cohort using this field - months_since_release >=12 because a value of 12 is people that have been released a full 12 months.	|
|	years_since_release	|	Calendar count of full years between the last day of data and the release date.	|
|	reincarceration	|	Binary indicator for whether the person has been reincarcerated	|
|	reincarceration_session_id	|	Session ID of the reincarceration session	|
|	reincarceration_date	|	Date of the reincarceration	|
|	release_to_reincarceration_days	|	Days between release and reincarceration	|
|	release_to_reincarceration_months	|	Calendar months between release date and reincarceration date. These fields are generally used to determine whether the event occurred within that time period. As such, the value gets rounded up - a person that revoked 11 months and 5 days after their supervision start will get a value of 12 because they should count towards a 12 month revocation rate but not an 11 month revocation rate	|
|	release_to_reincarceration_years	|	Years between the release and reincarceration. This value is rounded up as months are, as described above	|

## Methodology

This view is constructed directly from sessions. The query does a self join where reincarceration sessions are left joined to incarceration sessions that end in release. The alias "release_session" refers to the session from which a person is released, and the alias "reincarceration_session" refers to the incarceration sessions that follow this release.

The table is deduped so that each person's release session is associated with their first reincarceration, if there are more than one. For example, if a person was incarcerated three distinct times, the third reincarceration gets associated with only the second release, not the first. 

Releases are identified as those incarceration sessions that meet the following criteria:

1. Outflow to `SUPERVISION` or `RELEASE`
2. Are not parole board hold sessions

Reincarcerations are then joined to these releases based on:

1. Being an incarceration session that is not a parole board hold
2. Starting after the end date of the last supervision
3. Not inflowing from another incarceration term, unless that incarceration term is a parole board hold
"""

REINCARCERATION_SESSIONS_FROM_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH cte AS
    (
    SELECT 
        release_session.person_id,
        release_session.state_code,
        release_session.session_id AS release_session_id,
        --release_date is the day after the session end_date which represents the last full day in the compartment
        DATE_ADD(release_session.end_date, INTERVAL 1 DAY) AS release_date,
        DATE_DIFF(release_session.last_day_of_data, DATE_ADD(release_session.end_date, INTERVAL 1 DAY), DAY) AS days_since_release,
        
         /* 
        The two fields below calculate a calender count of months and years between the last day of data and the release
        date. The DATE_DIFF function returns the number of date part intervals between two dates, which is not 
        exactly what we want. If the last day of data is 2021-08-01 and a person is released on 2020-07-31, we would
        want that to count as 0 months, not 1. The months_since_release field below uses the DATE_DIFF function but then 
        subtracts 1 month in cases where the day of the month of the release date is greater than the day of 
        the month of the last day of data. This captures cases where the month number has changed, but a full month has 
        not gone by.
        
        Similar logic is used in the calendarized year count, except that rather than comparing the day of the month 
        across months, we compare the month and day of a year across years.
        */
            
        DATE_DIFF(release_session.last_day_of_data, DATE_ADD(release_session.end_date, INTERVAL 1 DAY), MONTH) 
            - IF(EXTRACT(DAY FROM DATE_ADD(release_session.end_date, INTERVAL 1 DAY))>EXTRACT(DAY FROM release_session.last_day_of_data), 1, 0) months_since_release,
        DATE_DIFF(release_session.last_day_of_data, DATE_ADD(release_session.end_date, INTERVAL 1 DAY), YEAR) 
            - IF(FORMAT_DATE("%m%d", DATE_ADD(release_session.end_date, INTERVAL 1 DAY))>FORMAT_DATE("%m%d", release_session.last_day_of_data), 1, 0) years_since_release,
        
        CASE WHEN reincarceration_session.start_date IS NOT NULL THEN 1 ELSE 0 END AS reincarceration,

        reincarceration_session.session_id AS reincarceration_session_id,
        reincarceration_session.start_date AS reincarceration_date,
        DATE_DIFF(reincarceration_session.start_date, release_session.end_date, DAY) - 1 AS release_to_reincarceration_days,
        
        /*
        The two fields below calculate the time between release to reincarceration date in calender months and
        calendar years. The logic is the same as that used above to calculate time since release with two related exceptions.
        Both of these are related to the fact that while 'time since release' fields are used to determine whether at least
        that amount of time has passed, the 'time to reincarceration' fields are used to determine whether a reincarceration 
        occurred within that time period. The first exception is that we add 1 to the calendar month or calendar year 
        calculations (as to indicate the month that the reincarceration should count towards), and the second is that the 
        day of month comparisons are now inclusive (so that a reincarceration that occurs exactly 1 month after release
        start counts towards the 1-month reincarceration rate).
        
        As an example, let's say a person was released on 2/15/21 and was reincarcerated on 3/16/21. This is 29 days
        and 1 calendar month between release and reincarceration. However, the reincarceration did not occur within the 
        person's first month released, so it should count towards a 2-month reincarceration rate, but not a 1-month 
        reincarceration rate. In this example, the value of release_to_reincarceration_months would be 2. If the reincarceration 
        had instead been on 3/15/21, the value would be 1 because the reincarceration did occur within 1 month.
        */
         
        DATE_DIFF(reincarceration_session.start_date, DATE_ADD(release_session.end_date, INTERVAL 1 DAY), MONTH) 
            - IF(EXTRACT(DAY FROM DATE_ADD(release_session.end_date, INTERVAL 1 DAY))>=EXTRACT(DAY FROM reincarceration_session.start_date), 1, 0) + 1 AS release_to_reincarceration_months,
        DATE_DIFF(reincarceration_session.start_date, DATE_ADD(release_session.end_date, INTERVAL 1 DAY), YEAR) 
            - IF(FORMAT_DATE("%m%d", DATE_ADD(release_session.end_date, INTERVAL 1 DAY))>=FORMAT_DATE("%m%d", reincarceration_session.start_date), 1, 0) + 1 AS release_to_reincarceration_years,
            
       
        ROW_NUMBER() OVER(PARTITION BY release_session.person_id, release_session.session_id ORDER BY reincarceration_session.session_id) AS rn
    FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized` release_session
    LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` reincarceration_session 
        ON reincarceration_session.person_id = release_session.person_id 
        AND reincarceration_session.compartment_level_1 IN ('INCARCERATION', 'INCARCERATION_OUT_OF_STATE')
        AND reincarceration_session.start_date > release_session.end_date
        AND reincarceration_session.compartment_level_2 NOT IN ('PAROLE_BOARD_HOLD', 'TEMPORARY_CUSTODY','SHOCK_INCARCERATION','COMMUNITY_PLACEMENT_PROGRAM')
        AND (reincarceration_session.inflow_from_level_1 NOT IN ('INCARCERATION', 'INCARCERATION_OUT_OF_STATE') 
            OR reincarceration_session.inflow_from_level_2 IN ('PAROLE_BOARD_HOLD', 'TEMPORARY_CUSTODY','SHOCK_INCARCERATION','COMMUNITY_PLACEMENT_PROGRAM'))
    WHERE release_session.compartment_level_1 IN ('INCARCERATION', 'INCARCERATION_OUT_OF_STATE')
        AND release_session.compartment_level_2 NOT IN ('PAROLE_BOARD_HOLD','TEMPORARY_CUSTODY','SHOCK_INCARCERATION','COMMUNITY_PLACEMENT_PROGRAM')
        AND (release_session.outflow_to_level_1 IN ('SUPERVISION','SUPERVISION_OUT_OF_STATE','RELEASE', 'PENDING_SUPERVISION') OR
            release_session.outflow_to_level_2 = 'COMMUNITY_PLACEMENT_PROGRAM')
    )
    SELECT * EXCEPT(rn)
    FROM cte
    WHERE rn = 1
    """

REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_NAME,
    view_query_template=REINCARCERATION_SESSIONS_FROM_SESSIONS_QUERY_TEMPLATE,
    description=REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_BUILDER.build_and_print()
