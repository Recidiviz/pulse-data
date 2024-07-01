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
"""Sessionized view of each individual. Session defined as continuous stay within a compartment"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_SUB_SESSIONS_VIEW_NAME = "compartment_sub_sessions"

COMPARTMENT_SUB_SESSIONS_VIEW_DESCRIPTION = """This view creates our final output of sub-sessions. In takes the
pre-processed sub-sessions view and does the following:
1. Further sub-sessionize by age and assessment score
2. Create compartment session ids
"""

COMPARTMENT_SUB_SESSIONS_QUERY_TEMPLATE = f"""
    WITH session_union_with_other_atts AS
    /*
    Union together the preprocessed sub-sessions with person age sessions and assessment score sessions
    prior to further sub-sessionizing
    */
    (
    SELECT
        person_id,
        state_code,
        start_date,
        end_date_exclusive,
        dataflow_session_id,
        compartment_level_1,
        compartment_level_2,
        supervising_officer_external_id,
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
        case_type,
        open_supervision_type,
        gender,
        prioritized_race_or_ethnicity,
        start_reason,
        start_sub_reason,
        end_reason,
        CAST(NULL AS INT64) AS age,
        CAST(NULL AS INT64) AS assessment_score,
        metric_source,
        last_day_of_data,
        earliest_start_date,
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_preprocessed_materialized`
    UNION ALL
    SELECT
        person_id,
        state_code,
        start_date,
        --Null out the last end date in age sessions because we don't want a new sub-session to be created after a
        --person leaves the system
        IF(MAX(end_date_exclusive) OVER(PARTITION BY person_id) = end_date_exclusive, NULL, end_date_exclusive)
            AS end_date_exclusive,
        CAST(NULL AS INT64) AS dataflow_session_id,
        CAST(NULL AS STRING) AS compartment_level_1,
        CAST(NULL AS STRING) AS compartment_level_2,
        CAST(NULL AS STRING) AS supervising_officer_external_id,
        CAST(NULL AS STRING) AS compartment_location,
        CAST(NULL AS STRING) AS facility,
        CAST(NULL AS STRING) AS facility_name,
        CAST(NULL AS STRING) AS supervision_office,
        CAST(NULL AS STRING) AS supervision_office_name,
        CAST(NULL AS STRING) AS supervision_district,
        CAST(NULL AS STRING) AS supervision_district_name,
        CAST(NULL AS STRING) AS supervision_region_name,
        CAST(NULL AS STRING) AS correctional_level,
        CAST(NULL AS STRING) AS correctional_level_raw_text,
        CAST(NULL AS STRING) AS housing_unit,
        CAST(NULL AS STRING) AS housing_unit_category,
        CAST(NULL AS STRING) AS housing_unit_type,
        CAST(NULL AS STRING) AS housing_unit_type_raw_text,
        CAST(NULL AS STRING) AS case_type,
        CAST(NULL AS STRING) AS open_supervision_type,
        CAST(NULL AS STRING) AS gender,
        CAST(NULL AS STRING) AS prioritized_race_or_ethnicity,
        CAST(NULL AS STRING) AS start_reason,
        CAST(NULL AS STRING) AS start_sub_reason,
        CAST(NULL AS STRING) AS end_reason,
        age,
        CAST(NULL AS INT64) AS assessment_score,
        CAST(NULL AS STRING) AS metric_source,
        CAST(NULL AS DATE) AS last_day_of_data,
        CAST(NULL AS DATE) AS earliest_start_date,
    FROM `{{project_id}}.{{sessions_dataset}}.person_age_sessions`
    UNION ALL
    SELECT
        person_id,
        state_code,
        assessment_date AS start_date,
        score_end_date_exclusive AS end_date_exclusive,
        CAST(NULL AS INT64) AS dataflow_session_id,
        CAST(NULL AS STRING) AS compartment_level_1,
        CAST(NULL AS STRING) AS compartment_level_2,
        CAST(NULL AS STRING) AS supervising_officer_external_id,
        CAST(NULL AS STRING) AS compartment_location,
        CAST(NULL AS STRING) AS facility,
        CAST(NULL AS STRING) AS facility_name,
        CAST(NULL AS STRING) AS supervision_office,
        CAST(NULL AS STRING) AS supervision_office_name,
        CAST(NULL AS STRING) AS supervision_district,
        CAST(NULL AS STRING) AS supervision_district_name,
        CAST(NULL AS STRING) AS supervision_region_name,
        CAST(NULL AS STRING) AS correctional_level,
        CAST(NULL AS STRING) AS correctional_level_raw_text,
        CAST(NULL AS STRING) AS housing_unit,
        CAST(NULL AS STRING) AS housing_unit_category,
        CAST(NULL AS STRING) AS housing_unit_type,
        CAST(NULL AS STRING) AS housing_unit_type_raw_text,
        CAST(NULL AS STRING) AS case_type,
        CAST(NULL AS STRING) AS open_supervision_type,
        CAST(NULL AS STRING) AS gender,
        CAST(NULL AS STRING) AS prioritized_race_or_ethnicity,
        CAST(NULL AS STRING) AS start_reason,
        CAST(NULL AS STRING) AS start_sub_reason,
        CAST(NULL AS STRING) AS end_reason,
        CAST(NULL AS INT64) AS age,
        -- TODO(#17265): Consider removing this logic following investigation of unknown assessment scores
        COALESCE(assessment_score, -999) AS assessment_score,
        CAST(NULL AS STRING) AS metric_source,
        CAST(NULL AS DATE) AS last_day_of_data,
        CAST(NULL AS DATE) AS earliest_start_date,
    FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized`
    )
    ,
    {create_sub_sessions_with_attributes(table_name='session_union_with_other_atts', use_magic_date_end_dates=True, end_date_field_name='end_date_exclusive')}
    ,
    sub_sessions_with_attributes_dedup AS
    /*
    Sub-session deduplication is done such that we take the non-null value for the new attributes (assessment score and
    age) and choose the sub-session sourced from compartment_sub_sessions_preprocessed for all of the other attributes
    */
    (
    SELECT
        person_id,
        dataflow_session_id,
        state_code,
        metric_source,
        compartment_level_1,
        compartment_level_2,
        supervising_officer_external_id,
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
        case_type,
        open_supervision_type,
        prioritized_race_or_ethnicity,
        gender,
        age,
        assessment_score,
        start_date,
        CASE WHEN DATE_SUB(end_date_exclusive, INTERVAL 1 DAY) < last_day_of_data THEN end_date_exclusive END AS end_date_exclusive,
        last_day_of_data,
        earliest_start_date,
        start_reason,
        start_sub_reason,
        end_reason,
    FROM
        (
        SELECT
            * EXCEPT(age, assessment_score),
            -- This takes the non-null age and assessment score value for each sub-session
            FIRST_VALUE(age) OVER(PARTITION BY person_id, state_code, start_date, end_date_exclusive
                ORDER BY IF(age IS NULL,1,0)) AS age,
            FIRST_VALUE(assessment_score) OVER(PARTITION BY person_id, state_code, start_date, end_date_exclusive
                ORDER BY IF(assessment_score IS NULL,1,0)) AS assessment_score,
        FROM sub_sessions_with_attributes
        )
    -- This chooses the sub-session sourced from compartment_sub_sessions_preprocessed
    WHERE metric_source IS NOT NULL
        AND start_date<=last_day_of_data
    )
    /*
    Create sub-session and session ids
    */
    SELECT
        person_id,
        state_code,
        ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY start_date) AS sub_session_id,
        dataflow_session_id,
        SUM(CASE WHEN new_compartment_level_1 OR new_compartment_level_2 THEN 1 ELSE 0 END)
            OVER(PARTITION BY person_id ORDER BY start_date) AS session_id,
        start_date,
        end_date_exclusive,
        compartment_level_1,
        compartment_level_2,
        --This logic converts the end date to inclusive, coalesces with the last day of data (also inclusive),
        --takes the date difference, and then adds 1 day.
        DATE_DIFF(COALESCE(DATE_SUB(end_date_exclusive, INTERVAL 1 DAY), last_day_of_data), start_date, DAY)+1
            AS session_length_days,
        supervising_officer_external_id,
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
        case_type,
        open_supervision_type,
        prioritized_race_or_ethnicity,
        gender,
        age,
        assessment_score,
        start_reason,
        start_sub_reason,
        end_reason,
        metric_source,
        last_day_of_data,
        earliest_start_date,
        COALESCE(LAG(compartment_level_1) OVER w, 'LIBERTY') AS inflow_from_level_1,
        COALESCE(LAG(compartment_level_2) OVER w, 'LIBERTY_FIRST_TIME_IN_SYSTEM') AS inflow_from_level_2,
        LEAD(compartment_level_1) OVER w AS outflow_to_level_1,
        LEAD(compartment_level_2) OVER w AS outflow_to_level_2
    FROM
        (
        SELECT
            *,
            COALESCE(LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date),'') != COALESCE(compartment_level_1,'') AS new_compartment_level_1,
            COALESCE(LAG(compartment_level_2) OVER(PARTITION BY person_id ORDER BY start_date),'') != COALESCE(compartment_level_2,'') AS new_compartment_level_2
        FROM sub_sessions_with_attributes_dedup
        )
    WINDOW w AS (PARTITION BY person_id ORDER BY start_date ASC)
"""
COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=COMPARTMENT_SUB_SESSIONS_VIEW_NAME,
    view_query_template=COMPARTMENT_SUB_SESSIONS_QUERY_TEMPLATE,
    description=COMPARTMENT_SUB_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER.build_and_print()
