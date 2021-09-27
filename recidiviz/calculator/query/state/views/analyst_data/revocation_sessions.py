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
"""A table of supervision start sessions with revocation session identifiers in cases where the person was revoked from that supervision term"""
# pylint: disable=line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATION_SESSIONS_VIEW_NAME = "revocation_sessions"

REVOCATION_SESSIONS_VIEW_DESCRIPTION = """
## Overview

The view `revocation_sessions_materialized` has a record for each person and each supervision super session. It joins back to `compartment_sessions` and identifies supervision starts and subsequent revocations when / if they occur. The table has a record for every supervision start and if there is a subsequent revocation the session_id and revocation date associated with the revocation session will be populated. 

This table can be used to calculate revocation rates based on the time between a supervision start and revocation. Additionally, since the supervision session id and revocation session id are both included in this view, this table can be joined back to `compartment_sessions` to pull characteristics associated with the supervision cohort (age, gender, etc) as well as the revocation (violation type). 

The logic is slightly more complex than just looking at supervision sessions that transition to an incarceration session because of the fact that states can have temporary parole board holds which don’t necessarily lead to a revocation. 

As an example, let’s say a person had the following set of sessions:

1. Incarceration - General Term
2. Parole
3. Incarceration - Parole Board Hold
4. Parole
5. Incarceration - Parole Board Hold
6. Incarceration - General Term

In this example, the supervision session ID would be session 2 and the revocation session ID would be 6. This person spent time incarcerated while on two parole board holds, one of which led to a revocation. In this example we would compare the start date of session 6 to the start date of session 2 if looking to understand the timing of when the revocation occurred relative to their release from prison.

## Field Definitions

|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	person_id	|	Unique person identifier	|
|	state_code	|	State	|
|	supervision_super_session_id	|	An ID that groups together continuous stays on supervision, in cases where an individual may go on PAROLE_BOARD_HOLD and back to Parole several times before being revoked	|
|	supervision_session_id	|	Session ID associated with the supervision session that a person was either released or sentenced to	|
|	supervision_start_date	|	Start date of the supervision session. This is the date to which revocation dates are compared to in determining the amount of time spent on supervision prior to a revocation	|
|	days_since_start	|	Days between the person's supervision start date and the last day of data	|
|	months_since_start	|	Calendar count of full months between a person's supervision start date and the last day of data. This field is generally used to identify the cohort to be used for a particular calculation (for example 12 month revocation rate requires a cohort that has started supervision at least 12 months prior). We can select that cohort using this field - months_since_start >=12 because a value of 12 is people that started their supervision period more than 12 months ago	|
|	years_since_start	|	Calendar count of full years between the last day of data and the supervision start date.	|
|	revocation_date	|	If the supervision super session outflows to incarceration, this is the date that their incarceration session started	|
|	revocation	|	Binary revocation indicator to count revocations (1 if the supervision super session ended in a revocation, otherwise 0)	|
|	revocation_session_id	|	The session id associated with the incarceration session that starts because of a revocation	|
|	supervision_start_to_revocation_days	|	Days between the supervision session start and the start of incarceration session that starts because of a revocation	|
|	supervision_start_to_revocation_months	|	Months between the supervision session start and the start of the incarceration session that starts because of a revocation. These fields are generally used to determine whether the event occurred within that time period. As such, the value gets rounded up - a person that revoked 11 months and 5 days after their supervision start will get a value of 12 because they should count towards a 12 month revocation rate but not an 11 month revocation rate	|
|	supervision_start_to_revocation_years		Years between the supervision session start and the start of incarceration session that starts because of a revocation. This value is rounded up as months are, as described above	|
"""

REVOCATION_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        person_id,
        state_code,
        supervision_super_session_id,
        session_id_start AS supervision_session_id,
        start_date AS supervision_start_date,
        DATE_DIFF(last_day_of_data, start_date, DAY) AS days_since_start,
        
        /* 
        The two fields below calculate a calender count of months and years between the last day of data and the supervision
        start date. The DATE_DIFF function returns the number of date part intervals between two dates, which is not 
        exactly what we want. If the last day of data is 2021-08-01 and a person starts supervision on 2020-07-31, we would
        want that to count as 0 months, not 1. The months_since_start field below uses the DATE_DIFF function but then 
        subtracts 1 month in cases where the day of the month of the supervision start date is greater than the day of 
        the month of the last day of data. This captures cases where the month number has changed, but a full month has 
        not gone by.
        
        Similar logic is used in the calendarized year count, except that rather than comparing the day of the month 
        across months, we compare the month and day of a year across years.
        */
            
        DATE_DIFF(last_day_of_data, start_date, MONTH) 
            - IF(EXTRACT(DAY FROM start_date)>EXTRACT(DAY FROM last_day_of_data), 1, 0) months_since_start,
        DATE_DIFF(last_day_of_data, start_date, YEAR) 
            - IF(FORMAT_DATE("%m%d", start_date)>FORMAT_DATE("%m%d", last_day_of_data), 1, 0) years_since_start,
       
        revocation_date,
        CASE WHEN revocation_date IS NOT NULL THEN 1 ELSE 0 END AS revocation,
        CASE WHEN revocation_date IS NOT NULL THEN session_id_end + 1 END AS revocation_session_id,
        DATE_DIFF(revocation_date, start_date, DAY) AS supervision_start_to_revocation_days,
        
        /*
        The two fields below calculate the time between supervision start and revocation date in calender months and
        calendar years. The logic is the same as that used above to calculate time since start with two related exceptions.
        Both of these are related to the fact that while 'time since start' fields are used to determine whether at least
        that amount of time has passed, the 'time to revocation' fields are used to determine whether a revocation 
        occurred within that time period. The first exception is that we add 1 to the calendar month or calendar year 
        calculations (as to indicate the month that the revocation should count towards), and the second is that the 
        day of month comparisons are now inclusive (so that a revocation that occurs exactly 1 month after supervision
        start counts towards the 1-month revocation rate).
        
        As an example, let's say a person started supervision on 2/15/21 and had a revocation on 3/16/21. This is 29 days
        and 1 calendar month between supervision start and revocation. However, the revocation did not occur within the 
        person's first month on supervision, so it should count towards a 2-month revocation rate, but not a 1-month 
        revocation rate. In this example, the value of supervision_start_to_revocation_months would be 2. If the revocation 
        had instead been on 3/15/21, the value would be 1 because the revocation did occur within 1 month.
        */
         
        DATE_DIFF(revocation_date, start_date, MONTH) 
            - IF(EXTRACT(DAY FROM start_date)>=EXTRACT(DAY FROM revocation_date), 1, 0) + 1 AS supervision_start_to_revocation_months,
        DATE_DIFF(revocation_date, start_date, YEAR) 
            - IF(FORMAT_DATE("%m%d", start_date)>=FORMAT_DATE("%m%d", revocation_date), 1, 0) + 1 AS supervision_start_to_revocation_years,
            
        last_day_of_data,
    FROM
        (
        SELECT 
            *,
            CASE WHEN outflow_to_level_1 IN ('INCARCERATION', 'INCARCERATION_OUT_OF_STATE') THEN DATE_ADD(end_date, INTERVAL 1 DAY) END AS revocation_date
        FROM `{project_id}.{analyst_dataset}.supervision_super_sessions_materialized`
        )
    """

REVOCATION_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=REVOCATION_SESSIONS_VIEW_NAME,
    view_query_template=REVOCATION_SESSIONS_QUERY_TEMPLATE,
    description=REVOCATION_SESSIONS_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATION_SESSIONS_VIEW_BUILDER.build_and_print()
