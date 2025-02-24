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
"""Sessionized view of each continuous period of dates with the same population attributes inherited from dataflow metrics"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

DATAFLOW_SESSIONS_VIEW_NAME = "dataflow_sessions"

INCARCERATION_POPULATION_SPECIAL_STATES = ("US_CO", "US_IX")
SUPERVISION_POPULATION_SPECIAL_STATES = ("US_PA", "US_TN")

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
|	end_date_exclusive	|	Day that session ends	|
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

DATAFLOW_SESSIONS_QUERY_TEMPLATE = f"""
    WITH population_cte AS
    /*
    Union together incarceration and supervision population metrics (both in state and out of state). There are cases in
    each of these individual dataflow metrics where we have the same person on the same day with different values for
    supervision types or specialized purpose for incarceration. This deduplication is handled further down in the query.

    Create a field that identifies the compartment_level_1 (incarceration vs supervision) and compartment_level_2.

    The field "metric_source" is pulled from dataflow metric as to distinguish the population metric data sources. This
    is done because SUPERVISION can come from either SUPERVISION_POPULATION and SUPERVISION_OUT_OF_STATE_POPULATION.
    Compartment location is defined as facility for incarceration and supervision office and district for supervision
    periods.
    */
    (
    SELECT
        person_id,
        start_date_inclusive AS start_date,
        end_date_exclusive,
        metric_type AS metric_source,
        state_code,
        IF(included_in_state_population, 'INCARCERATION', 'INCARCERATION_NOT_INCLUDED_IN_STATE') AS compartment_level_1,
        /* TODO(#6126): Investigate ID missing reason for incarceration */
        CASE
            WHEN state_code = 'US_ND' AND facility = 'CPP'
                THEN 'COMMUNITY_CONFINEMENT'
            ELSE COALESCE(purpose_for_incarceration, 'GENERAL') END as compartment_level_2,
        COALESCE(facility,'EXTERNAL_UNKNOWN') AS compartment_location,
        COALESCE(facility,'EXTERNAL_UNKNOWN') AS facility,
        CAST(NULL AS STRING) AS supervision_office,
        CAST(NULL AS STRING) AS supervision_district,
        custody_level AS correctional_level,
        custody_level_raw_text AS correctional_level_raw_text,
        housing_unit,
        housing_unit_category,
        housing_unit_type,
        housing_unit_type_raw_text,
        CAST(NULL AS STRING) AS supervising_officer_external_id,
        CAST(NULL AS STRING) AS case_type,
        prioritized_race_or_ethnicity,
        gender,
        custodial_authority,
    FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_incarceration_population_span_metrics_materialized`
    WHERE state_code NOT IN ('{{incarceration_special_states}}')

    UNION ALL
    -- TODO(#15610): Remove ID preprocessing file when out of state facilities are flagged in sessions
    SELECT *
    FROM `{{project_id}}.{{sessions_dataset}}.us_ix_incarceration_population_metrics_preprocessed_materialized`

    UNION ALL
    -- TODO(#15610): Remove CO preprocessing file when out of state facilities are flagged in sessions
    SELECT *
    FROM `{{project_id}}.{{sessions_dataset}}.us_co_incarceration_population_metrics_preprocessed_materialized`

    UNION ALL
    SELECT
        person_id,
        start_date_inclusive AS start_date,
        end_date_exclusive,
        metric_type AS metric_source,
        metrics.state_code,
        IF(included_in_state_population, 'SUPERVISION', 'SUPERVISION_OUT_OF_STATE') AS compartment_level_1,
        supervision_type as compartment_level_2,
        CONCAT(COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN'),'|', COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN')) AS compartment_location,
        CAST(NULL AS STRING) AS facility,
        COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_office,
        COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_district,
        supervision_level AS correctional_level,
        supervision_level_raw_text AS correctional_level_raw_text,
        CAST(NULL AS STRING) AS housing_unit,
        CAST(NULL AS STRING) AS housing_unit_category,
        CAST(NULL AS STRING) AS housing_unit_type,
        CAST(NULL AS STRING) AS housing_unit_type_raw_text,
        staff.external_id AS supervising_officer_external_id,
        case_type,
        prioritized_race_or_ethnicity,
        gender,
        custodial_authority,
    FROM
        `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_supervision_population_span_metrics_materialized` metrics
    LEFT JOIN
        `{{project_id}}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` staff
    ON
        metrics.supervising_officer_staff_id = staff.staff_id
    WHERE metrics.state_code NOT IN ('{{supervision_special_states}}')

    UNION ALL
    -- TODO(#12046): [Pathways] Remove TN-specific raw supervision-level mappings
    SELECT
        *
    FROM `{{project_id}}.{{sessions_dataset}}.us_tn_supervision_population_metrics_preprocessed_materialized`

    UNION ALL
    -- TODO(#15613): Remove PA community confinement recategorization when hydrated in population metrics
    SELECT
        *
    FROM `{{project_id}}.{{sessions_dataset}}.us_pa_supervision_population_metrics_preprocessed_materialized`
    )
    ,
    population_with_location_names AS (
        SELECT
            population_cte.*,
            supervision_locations.supervision_office_name,
            supervision_locations.supervision_district_name,
            supervision_locations.supervision_region_name,
            incarceration_locations.facility_name,
        FROM population_cte
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.session_location_names_materialized` supervision_locations
            ON supervision_locations.state_code = population_cte.state_code
            -- Only join if district is non-null
            AND supervision_locations.supervision_district = population_cte.supervision_district
            AND IFNULL(supervision_locations.supervision_office, "UNKNOWN") = IFNULL(population_cte.supervision_office, "UNKNOWN")
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.session_location_names_materialized` incarceration_locations
            ON incarceration_locations.state_code = population_cte.state_code
            -- Only join if facility is non-null
            AND incarceration_locations.facility = population_cte.facility
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
        MAX(GREATEST(start_date,end_date_exclusive)) AS last_day_of_data
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
    {create_sub_sessions_with_attributes(
        table_name='population_with_location_names',
        use_magic_date_end_dates=True,
        end_date_field_name='end_date_exclusive'
    )}
    ,
    sub_sessions_with_attributes_dedup AS
    (
    SELECT
        person_id,
        start_date,
        end_date_exclusive,
        state_code,
        ARRAY_AGG(
            STRUCT(
                metric_source,
                IF(
                    compartment_level_2 = "INVESTIGATION",
                    "INVESTIGATION",
                    compartment_level_1
                ) AS compartment_level_1,
                compartment_level_2,
                compartment_location,
                facility,
                facility_name,
                supervision_office,
                supervision_office_name,
                supervision_district,
                supervision_district_name,
                supervision_region_name,
                correctional_level,
                correctional_level_raw_text,
                housing_unit,
                housing_unit_category,
                housing_unit_type,
                housing_unit_type_raw_text,
                supervising_officer_external_id,
                case_type,
                prioritized_race_or_ethnicity,
                gender,
                custodial_authority
                )
            ORDER BY
                metric_source,
                compartment_level_1,
                compartment_level_2,
                compartment_location,
                facility,
                facility_name,
                supervision_office,
                supervision_office_name,
                supervision_district,
                supervision_district_name,
                supervision_region_name,
                correctional_level,
                correctional_level_raw_text,
                housing_unit,
                housing_unit_category,
                housing_unit_type,
                housing_unit_type_raw_text,
                supervising_officer_external_id,
                case_type,
                prioritized_race_or_ethnicity,
                gender,
                custodial_authority
            ) AS session_attributes,
    FROM sub_sessions_with_attributes
    WHERE start_date != end_date_exclusive
    GROUP BY 1,2,3,4
    )
    ,
    sessionized_cte AS
    /*
    Final sessionization step that aggregates together any adjacent sessions that have identical attributes
    */
    (
    {aggregate_adjacent_spans(
                        table_name='sub_sessions_with_attributes_dedup',
                        attribute='session_attributes',
                        struct_attribute_subset='session_attributes',
                        session_id_output_name='dataflow_session_id',
                        end_date_field_name='end_date_exclusive'
                    )}
    )
    SELECT
        person_id,
        dataflow_session_id,
        state_code,
        start_date,
        DATE_SUB(end_date_exclusive, INTERVAL 1 DAY) AS end_date,
        end_date_exclusive,
        session_attributes,
        last_day_of_data,
    FROM sessionized_cte
    JOIN last_day_of_data_by_state
        USING(state_code)
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
    incarceration_special_states="', '".join(INCARCERATION_POPULATION_SPECIAL_STATES),
    supervision_special_states="', '".join(SUPERVISION_POPULATION_SPECIAL_STATES),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        DATAFLOW_SESSIONS_VIEW_BUILDER.build_and_print()
