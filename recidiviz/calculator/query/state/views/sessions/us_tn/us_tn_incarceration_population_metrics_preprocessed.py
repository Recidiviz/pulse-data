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
"""State-specific preprocessing for joining with dataflow sessions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_NAME = (
    "us_tn_incarceration_population_metrics_preprocessed"
)

US_TN_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_DESCRIPTION = (
    """State-specific preprocessing for joining with dataflow sessions"""
)

US_TN_INCARCERATION_POPULATION_METRICS_PREPROCESSED_QUERY_TEMPLATE = """
    /*{description}*/
    WITH temp_custody_cte AS
    (
    SELECT
        person_id,
        state_code,
        admission_date,
        release_date,
        'WEEKEND_CONFINEMENT' AS compartment_level_2,
    FROM `{project_id}.{state_base_dataset}.state_incarceration_period` 
    WHERE admission_reason_raw_text IN ('CCFA-WKEND','PRFA-WKEND')
        AND state_code = 'US_TN'
    )
    SELECT DISTINCT
        population.person_id,
        population.date_of_stay AS date,
        population.metric_type AS metric_source,
        population.created_on,       
        population.state_code,
        'INCARCERATION' AS compartment_level_1,
        COALESCE(tc.compartment_level_2, population.specialized_purpose_for_incarceration) AS compartment_level_2,
        COALESCE(population.facility,'EXTERNAL_UNKNOWN') AS compartment_location,
        COALESCE(population.facility,'EXTERNAL_UNKNOWN') AS facility,
        CAST(NULL AS STRING) AS supervision_office,
        CAST(NULL AS STRING) AS supervision_district,
        CAST(NULL AS STRING) AS correctional_level,
        CAST(NULL AS STRING) AS correctional_level_raw_text,
        CAST(NULL AS STRING) AS supervising_officer_external_id,
        CAST(NULL AS STRING) AS case_type,
        population.judicial_district_code,
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_included_in_state_population_materialized` population
    LEFT JOIN temp_custody_cte tc
        ON tc.person_id = population.person_id
        AND population.date_of_stay BETWEEN tc.admission_date AND COALESCE(tc.release_date, '9999-01-01')
        AND population.specialized_purpose_for_incarceration = 'GENERAL'
    WHERE population.state_code = 'US_TN'
"""

US_TN_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_TN_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_NAME,
    view_query_template=US_TN_INCARCERATION_POPULATION_METRICS_PREPROCESSED_QUERY_TEMPLATE,
    description=US_TN_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_DESCRIPTION,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    state_base_dataset=STATE_BASE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER.build_and_print()
