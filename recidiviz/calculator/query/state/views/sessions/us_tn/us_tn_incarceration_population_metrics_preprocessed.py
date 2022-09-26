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
"""TN State-specific preprocessing"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import revert_nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_NAME = (
    "us_tn_incarceration_population_metrics_preprocessed"
)

US_TN_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_DESCRIPTION = """TN State-specific preprocessing
- Sub-sessionizes with judicial district sessions to determine relevant judicial district
"""

US_TN_INCARCERATION_POPULATION_METRICS_PREPROCESSED_QUERY_TEMPLATE = f"""
    /*{{description}}*/   
    --TODO(#10747): Remove judicial district preprocessing once hydrated in population metrics
    WITH overlapping_periods_cte AS 
    (
    SELECT
        person_id,
        start_date_inclusive AS start_date,
        end_date_exclusive AS end_date,        
        metric_type AS metric_source,
        state_code,
        IF(included_in_state_population, 'INCARCERATION', 'INCARCERATION_NOT_INCLUDED_IN_STATE') AS compartment_level_1,
        COALESCE(purpose_for_incarceration, 'GENERAL') as compartment_level_2,
        COALESCE(facility,'EXTERNAL_UNKNOWN') AS compartment_location,
        COALESCE(facility,'EXTERNAL_UNKNOWN') AS facility,
        CAST(NULL AS STRING) AS supervision_office,
        CAST(NULL AS STRING) AS supervision_district,    
        CAST(NULL AS STRING) AS correctional_level,
        CAST(NULL AS STRING) AS correctional_level_raw_text,
        CAST(NULL AS STRING) AS supervising_officer_external_id,
        CAST(NULL AS STRING) AS case_type,
        judicial_district_code,
    FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_incarceration_population_span_metrics_materialized`
    WHERE state_code = 'US_TN'
    
    UNION ALL
    
    SELECT
        person_id,
        judicial_district_start_date AS start_date,
        DATE_ADD(judicial_district_end_date, INTERVAL 1 DAY) AS end_date,
        CAST(NULL AS STRING) AS metric_source,
        state_code,
        CAST(NULL AS STRING) AS compartment_level_1,
        CAST(NULL AS STRING) AS compartment_level_2, 
        CAST(NULL AS STRING) AS compartment_location,
        CAST(NULL AS STRING) AS facility,
        CAST(NULL AS STRING) AS supervision_office,
        CAST(NULL AS STRING) AS supervision_district,
        CAST(NULL AS STRING) AS correctional_level,
        CAST(NULL AS STRING) AS correctional_level_raw_text,
        CAST(NULL AS STRING) AS supervising_officer_external_id,
        CAST(NULL AS STRING) AS case_type,
        judicial_district_code,
    FROM `{{project_id}}.{{sessions_dataset}}.us_tn_judicial_district_sessions_materialized` s
    )
    ,
    {create_sub_sessions_with_attributes(table_name='overlapping_periods_cte', use_magic_date_end_dates=True)}
    ,
    sub_sessions_with_attributes_dedup AS
    (
    SELECT
        *
    FROM
        (
        SELECT 
            * EXCEPT(judicial_district_code),
            FIRST_VALUE(judicial_district_code) OVER(PARTITION BY person_id, state_code, start_date, end_date 
                ORDER BY IF(judicial_district_code IS NULL,1,0)) AS judicial_district_code,
        FROM sub_sessions_with_attributes
        )
    WHERE metric_source IS NOT NULL
    )
    SELECT 
        person_id,
        start_date,
        {revert_nonnull_end_date_clause('end_date')} AS end_date,
        metric_source,
        state_code,
        compartment_level_1,
        compartment_level_2,
        compartment_location,
        facility,
        supervision_office,
        supervision_district,
        correctional_level,
        correctional_level_raw_text,
        supervising_officer_external_id,
        case_type,
        judicial_district_code
    FROM sub_sessions_with_attributes_dedup   
"""

US_TN_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    view_id=US_TN_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_NAME,
    view_query_template=US_TN_INCARCERATION_POPULATION_METRICS_PREPROCESSED_QUERY_TEMPLATE,
    description=US_TN_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_DESCRIPTION,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER.build_and_print()
