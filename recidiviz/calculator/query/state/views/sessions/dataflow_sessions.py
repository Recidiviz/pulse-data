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
"""Sessionized view of each continuous period of dates with the same population attributes inherited from dataflow metrics"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

DATAFLOW_SESSIONS_VIEW_NAME = "dataflow_sessions"

DATAFLOW_SESSIONS_SUPPORTED_STATES = (
    "US_ND",
    "US_ID",
    "US_MO",
    "US_PA",
    "US_TN",
    "US_ME",
)

DATAFLOW_SESSIONS_VIEW_DESCRIPTION = """
## Overview

Dataflow sessions is the most finely grained sessionized view. This view is unique on `person_id` and `dataflow_session_id`. New sessions are defined by a gap in population data or a change in _any_ of the following fields:

1. `compartment_level_1`
2. `compartment_level_2`
3. `compartment_location`
4. `correctional_level`
5. `supervising_officer_external_id`
6. `case_type`

This table is the source of other sessions tables such as `compartment_sessions`, `location_sessions`, and `supervision_officer_sessions`. 

## Field Definitions

|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	person_id	|	Unique person identifier	|
|	dataflow_session_id	|	Ordered session number per person	|
|	state_code	|	State	|
|	start_date	|	Start day of session	|
|	end_date	|	Last full day of session	|
|	session_attributes	|	This is an array that stores values for metric_source, compartment_level_1, compartment_level_2, correctional_level, supervising_officer_external_id, and compartment_location in cases where there is more than of these values on a given day. This field allows us to unnest to create overlapping sessions and look at cases where a person has more than one attribute for a given time period	|
|	last_day_of_data	|	The last day for which we have data, specific to a state. The is is calculated as the state min of the max day of which we have population data across supervision and population metrics within a state. For example, if in ID the max incarceration population date value is 2021-09-01 and the max supervision population date value is 2021-09-02, we would say that the last day of data for ID is 2021-09-01.	|

## Methodology

1. Union together the three population metrics
    1. There are three dataflow population metrics - `INCARCERATION_POPULATION`, `SUPERVISION_POPULATION`, and `SUPERVISION_OUT_OF_STATE_POPULATION`. Each of these has a value for each person and day for which they are counted towards that population. 

2. Deduplicate 
    1. There are cases in each of these individual dataflow metrics where we have the same person on the same day with different values for supervision types or specialized purpose for incarceration. If someone is present in both `PROBATION` and `PAROLE` on a given day, they are recategorized to `DUAL`. The unioned population data is then deduplicated to be unique on person and day. We prioritize the metrics in the following order: (1) `INCARCERATION_POPULATION`, (2) `SUPERVISION_POPULATION`, (3) `SUPERVISION_OUT_OF_STATE_POPULATION`. This means that if a person shows up in both incarceration and supervision population on the same day, we list that person as only being incarcerated.

3. Aggregate into sessions 
    1. Continuous dates within `metric_source`, `compartment_level_1`, `compartment_level_2`, `location`, `correctional_level`, `supervising_officer_external_id`, `case_type`, and `person_id`
"""

DATAFLOW_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH population_cte AS
    /*
    Union together incarceration and supervision population metrics (both in state and out of state). There are cases in 
    each of these individual dataflow metrics where we have the same person on the same day with different values for 
    supervision types or specialized purpose for incarceration. This deduplication is handled further down in the query. 

    Create a field that identifies the compartment_level_1 (incarceration vs supervision) and compartment_level_2.

    The field "metric_source" is pulled from dataflow metric as to distinguish the population metric data sources. This 
    is done because SUPERVISION can come from either SUPERVISION_POPULATION and SUPERVISION_OUT_OF_STATE_POPULATION.

    Compartment location is defined as facility for incarceration and judicial district for supervision periods.
    */
    (
    SELECT 
        DISTINCT
        person_id,
        date_of_stay AS date,
        metric_type AS metric_source,
        created_on,
        state_code,
        'INCARCERATION' as compartment_level_1,
        /* TODO(#6126): Investigate ID missing reason for incarceration */
        CASE 
            WHEN state_code = 'US_ND' AND facility = 'CPP' 
                THEN 'COMMUNITY_CONFINEMENT'
            ELSE COALESCE(specialized_purpose_for_incarceration, 'GENERAL') END as compartment_level_2,
        COALESCE(facility,'EXTERNAL_UNKNOWN') AS compartment_location,
        COALESCE(facility,'EXTERNAL_UNKNOWN') AS facility,
        CAST(NULL AS STRING) AS supervision_office,
        CAST(NULL AS STRING) AS supervision_district,    
        CAST(NULL AS STRING) AS correctional_level,
        CAST(NULL AS STRING) AS supervising_officer_external_id,
        CAST(NULL AS STRING) AS case_type,
        judicial_district_code,
    FROM
        `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_included_in_state_population_materialized`
    WHERE state_code in ('{supported_states}')
        AND state_code NOT IN ('US_ID','US_TN')
    UNION ALL
    -- Use Idaho preprocessed dataset to deal with state-specific logic
    SELECT *
    FROM `{project_id}.{sessions_dataset}.us_id_incarceration_population_metrics_preprocessed_materialized`
    UNION ALL
    -- Use TN preprocessed dataset to deal with state-specific logic
    SELECT *
    FROM `{project_id}.{sessions_dataset}.us_tn_incarceration_population_metrics_preprocessed_materialized`
    UNION ALL
    SELECT
        DISTINCT
        person_id,
        date_of_stay AS date,
        metric_type AS metric_source,
        created_on,
        state_code,
        'INCARCERATION_NOT_INCLUDED_IN_STATE' as compartment_level_1,
        specialized_purpose_for_incarceration as compartment_level_2,
        COALESCE(facility,'EXTERNAL_UNKNOWN') AS compartment_location,
        COALESCE(facility,'EXTERNAL_UNKNOWN') AS facility,
        CAST(NULL AS STRING) AS supervision_office,
        CAST(NULL AS STRING) AS supervision_district,
        CAST(NULL AS STRING) AS correctional_level,
        CAST(NULL AS STRING) AS supervising_officer_external_id,
        CAST(NULL AS STRING) AS case_type,
        judicial_district_code,
    FROM
        `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_not_included_in_state_population_materialized`
    WHERE state_code in ('{supported_states}')
    UNION ALL
    SELECT 
        DISTINCT
        person_id,
        date_of_supervision AS date,
        metric_type AS metric_source,
        created_on,       
        state_code,
        'SUPERVISION' as compartment_level_1,
        supervision_type as compartment_level_2,
        CONCAT(COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN'),'|', COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN')) AS compartment_location,
        CAST(NULL AS STRING) AS facility,
        COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_office,
        COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_district,
        supervision_level AS correctional_level,
        supervising_officer_external_id,
        case_type,
        judicial_district_code,
    FROM
        `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_metrics_materialized`
    WHERE state_code in ('{supported_states}')
        AND state_code NOT IN ('US_ID', 'US_MO')
    UNION ALL
    -- Use Idaho preprocessed dataset to deal with state-specific logic
    SELECT 
        *
    FROM `{project_id}.{sessions_dataset}.us_id_supervision_population_metrics_preprocessed_materialized`
    UNION ALL
    -- Use MO preprocessed dataset to deal with state-specific logic
    SELECT 
        *
    FROM `{project_id}.{sessions_dataset}.us_mo_supervision_population_metrics_preprocessed_materialized`
    UNION ALL
    SELECT 
        DISTINCT
        person_id,
        date_of_supervision AS date,
        metric_type AS metric_source,
        created_on,       
        state_code,
        'SUPERVISION_OUT_OF_STATE' as compartment_level_1,
        supervision_type as compartment_level_2,
        CONCAT(COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN'),'|', COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN')) AS compartment_location,
        CAST(NULL AS STRING) AS facility,
        COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_office,
        COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_district,
        supervision_level AS correctional_level,
        supervising_officer_external_id,
        case_type,
        judicial_district_code,
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_out_of_state_population_metrics_materialized`
    WHERE state_code in ('{supported_states}')
      AND state_code != 'US_ID'
    UNION ALL
    -- Use Idaho preprocessed dataset to deal with state-specific logic
    SELECT 
        *
    FROM `{project_id}.{sessions_dataset}.us_id_supervision_out_of_state_population_metrics_preprocessed_materialized`
    )
    ,
    last_day_of_data_by_state_and_source AS
    /*
    Get the max date for each state and population source, and then the min of these dates for each state. This is to 
    be used as the 'current' date for which we assume anyone listed on this date is still in that compartment. 
    */
    (
    SELECT 
        state_code,
        metric_source,
        MAX(date) AS last_day_of_data
    FROM population_cte
    GROUP BY 1,2
    )
    ,
    last_day_of_data_by_state AS
    (
    SELECT 
        state_code,
        MIN(last_day_of_data) last_day_of_data
    FROM last_day_of_data_by_state_and_source
    GROUP BY 1
    ) 
    ,
    session_attributes_cte AS
    (
    SELECT 
        p.person_id,
        date,
        p.state_code,
        last_day_of_data,
        ARRAY_AGG(
            STRUCT(
                metric_source,
                compartment_level_1,
                compartment_level_2,
                compartment_location,
                facility,
                supervision_office,
                supervision_district,
                correctional_level,
                supervising_officer_external_id,
                case_type,
                COALESCE(j.judicial_district_code, p.judicial_district_code) AS judicial_district_code
                )
            ORDER BY 
                metric_source,
                compartment_level_1,
                compartment_level_2,
                compartment_location,
                facility,
                supervision_office,
                supervision_district,
                correctional_level,
                supervising_officer_external_id,
                case_type,
                COALESCE(j.judicial_district_code, p.judicial_district_code)
            ) AS session_attributes,
    FROM population_cte p
    JOIN last_day_of_data_by_state 
        USING(state_code)
    --TODO(#10747): Remove judicial district preprocessing once hydrated in population metrics
    LEFT JOIN `{project_id}.{sessions_dataset}.us_tn_judicial_district_sessions_materialized` j
        ON p.person_id = j.person_id
        AND p.date BETWEEN j.judicial_district_start_date AND COALESCE(j.judicial_district_end_date,'9999-01-01')
        AND p.state_code = 'US_TN'
    WHERE date<=last_day_of_data 
    GROUP BY 1,2,3,4
    )
    ,
    sessionized_cte AS 
    /*
    Aggregate across distinct sub-sessions (continuous dates within metric_source, compartment, location, and person_id)
    and get the range of dates that define the session
    */
    (
    SELECT
        person_id,
        dataflow_session_id,
        state_code,
        last_day_of_data,
        MIN(date) AS start_date,
        MAX(date) AS end_date,
        ANY_VALUE(session_attributes) session_attributes,
    FROM 
        (
        SELECT 
            *,
            SUM(IF(new_session OR date_gap,1,0)) OVER(PARTITION BY person_id ORDER BY date) AS dataflow_session_id
        FROM 
            (
            SELECT 
                *,
                COALESCE(LAG(TO_JSON_STRING(session_attributes)) OVER(PARTITION BY person_id, state_code ORDER BY date),'') != COALESCE(TO_JSON_STRING(session_attributes),'') AS new_session,
                LAG(date) OVER(PARTITION BY person_id ORDER BY date) != DATE_SUB(date, INTERVAL 1 DAY) AS date_gap
            FROM session_attributes_cte
            )
        )
    GROUP BY 1,2,3,4
    )
    /*
    Same as sessionized cte with null end dates for active sessions.
    */
    SELECT 
        person_id,
        dataflow_session_id,
        state_code,
        start_date,
        CASE WHEN end_date < last_day_of_data THEN end_date END AS end_date,
        session_attributes,
        last_day_of_data
    FROM sessionized_cte
"""

DATAFLOW_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=DATAFLOW_SESSIONS_VIEW_NAME,
    view_query_template=DATAFLOW_SESSIONS_QUERY_TEMPLATE,
    description=DATAFLOW_SESSIONS_VIEW_DESCRIPTION,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
    supported_states="', '".join(DATAFLOW_SESSIONS_SUPPORTED_STATES),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        DATAFLOW_SESSIONS_VIEW_BUILDER.build_and_print()
