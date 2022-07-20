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
"""View that generates individual records of release to analyse recidivism rates."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SESSION_COHORT_REINCARCERATION_VIEW_NAME = "session_cohort_reincarceration"

SESSION_COHORT_REINCARCERATION_VIEW_DESCRIPTION = (
    "View that generates individual records of release to analyse recidivism rates"
)

SESSION_COHORT_REINCARCERATION_QUERY_TEMPLATE = """
    /*{description}*/
    WITH cohort AS
    /*
    This cte defines the cohort and has a record for every person and every start event. In the case of 
    recidivism, the start event is the start of either a RELEASE or SUPERVISION session that inflowed
    from an INCARCERATION session (a person being released from prison). 
    */
    (
    SELECT
        person_id,
        state_code,
        session_id AS cohort_session_id,
        start_date as cohort_start_date,
        last_day_of_data
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
    WHERE compartment_level_1 IN ('RELEASE','SUPERVISION')
        AND inflow_from_level_1 = 'INCARCERATION' 
        AND inflow_from_level_2 IN ('GENERAL','TREATMENT_IN_PRISON','SHOCK_INCARCERATION')
    )
    ,
    event AS
    (
    /*
    This cte defines the event that we are interested in measuring the rate of. In the case of recidivism, it is 
    the start of an incarceration session.
    */
    SELECT
        person_id,
        state_code,
        session_id AS event_session_id,
        start_date AS event_date,
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
    WHERE compartment_level_1 = 'INCARCERATION'
        AND compartment_level_2 IN ('GENERAL','TREATMENT_IN_PRISON','SHOCK_INCARCERATION')
    )
    ,
    cohort_x_event AS
    /*
    This cte associates cohort starts (releases) and subsequent events (incarcerations). Incarcerations are limited in the 
    join to come after releases, and then deduplication is done to associate each incarceration with the first preceding release.
    */
    (
    SELECT
        cohort.person_id,
        cohort.state_code,
        cohort_session_id,
        cohort_start_date,
        event_session_id,
        event_date,
    FROM cohort
    JOIN event
        ON cohort.person_id = event.person_id
        AND cohort.state_code = event.state_code
        AND event_date>cohort_start_date
    /*
    These two qualify statements handle deduplication so that each reincarceration ties to the most recent release. This is
    relevant when people have more than one release and reincarceration. I believe that this deduplication is overkill
    for the specific example of recidivism because by definition you can't have more than one release from prison preceding 
    a single admission to prison. This is more relevant when measuring things like supervision start --> revocation where you could
    have two supervision starts preceding a single revocation (if the first one ended without revocation), and we would want to
    associate that revocation with the second (most recent) supervision start. I think it is still safer to leave both
    qualify statements in here so that the query does generalize to other cohort --> event rates as well.
    
    This first deduplication ranks events within cohort starts and chooses the first event. If a person has multiple events, 
    following a cohort start we are choosing the date of the first event.
    */
    QUALIFY ROW_NUMBER() OVER(PARTITION BY cohort.person_id, cohort_session_id ORDER BY event_session_id ASC) = 1
    /*
    This second deduplication ranks cohort starts within events. If a person has multiple cohort starts preceding an event, 
    we are choosing the date of the most recent cohort start. 
    */
        AND ROW_NUMBER() OVER(PARTITION BY cohort.person_id, event_session_id ORDER BY cohort_session_id DESC) = 1
    )
    /*
    Joins events back to the original releases cte so that we have a record for every release. 
    Also calculates the time between cohort start and event as well as days that the cohort
    has had to mature (last day of data minus cohort start date)
    */
    SELECT
        cohort.state_code,
        cohort.person_id,
        cohort.cohort_start_date,
        cohort.cohort_session_id,
        cohort_x_event.event_session_id,
        cohort_x_event.event_date,
        cohort.last_day_of_data,
        DATE_DIFF(cohort.last_day_of_data, cohort.cohort_start_date, DAY) AS cohort_days_to_mature,
        DATE_DIFF(cohort.last_day_of_data, cohort.cohort_start_date, MONTH)
          - IF(EXTRACT(DAY FROM cohort.cohort_start_date)>EXTRACT(DAY FROM cohort.last_day_of_data), 1, 0) cohort_months_to_mature,
        DATE_DIFF(cohort_x_event.event_date, cohort.cohort_start_date, DAY) AS cohort_start_to_event_days,
        DATE_DIFF(cohort_x_event.event_date, cohort.cohort_start_date, MONTH)
          - IF(EXTRACT(DAY FROM cohort.cohort_start_date)>=EXTRACT(DAY FROM cohort_x_event.event_date), 1, 0) + 1 AS cohort_start_to_event_months,
          sessions.gender,
          sessions.age_bucket_start,
          sessions.prioritized_race_or_ethnicity
    FROM cohort
    LEFT JOIN cohort_x_event
        ON cohort.person_id = cohort_x_event.person_id
        AND cohort.state_code = cohort_x_event.state_code
        AND cohort.cohort_session_id = cohort_x_event.cohort_session_id
    LEFT JOIN `sessions.compartment_sessions_materialized` sessions
        ON cohort.person_id = sessions.person_id
        AND cohort.cohort_session_id = sessions.session_id
    """

SESSION_COHORT_REINCARCERATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=SESSION_COHORT_REINCARCERATION_VIEW_NAME,
    view_query_template=SESSION_COHORT_REINCARCERATION_QUERY_TEMPLATE,
    description=SESSION_COHORT_REINCARCERATION_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SESSION_COHORT_REINCARCERATION_VIEW_BUILDER.build_and_print()
