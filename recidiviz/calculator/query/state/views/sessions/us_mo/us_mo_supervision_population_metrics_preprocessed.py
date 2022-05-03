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
"""MO State-specific preprocessing for joining with dataflow sessions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_NAME = (
    "us_mo_supervision_population_metrics_preprocessed"
)

US_MO_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_DESCRIPTION = """MO State-specific preprocessing for joining with dataflow sessions:
- Recategorize supervision type/compartment level 2 as 'ABSCONSION' if date of supervision falls within a supervision period where start reason = 'ABSCONSION'
"""

US_MO_SUPERVISION_POPULATION_METRICS_PREPROCESSED_QUERY_TEMPLATE = """
    /*{description}*/   
    WITH absconsion_cte AS
    (
    SELECT
        person_id,
        state_code,
        start_date,
        termination_date,
        'ABSCONSION' AS supervision_type
    FROM `{project_id}.{state_base_dataset}.state_supervision_period` 
    WHERE admission_reason ='ABSCONSION'
        AND state_code = 'US_MO'
    )
    SELECT DISTINCT
        population.person_id,
        population.date_of_supervision AS date,
        population.metric_type AS metric_source,
        population.created_on,       
        population.state_code,
        'SUPERVISION' AS compartment_level_1,
        COALESCE(abs.supervision_type, population.supervision_type) AS compartment_level_2,
        CONCAT(COALESCE(population.level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN'),'|', COALESCE(population.level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN')) AS compartment_location,
        CAST(NULL AS STRING) AS facility,
        COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_office,
        COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_district,
        population.supervision_level AS correctional_level,
        population.supervision_level_raw_text AS correctional_level_raw_text,
        population.supervising_officer_external_id,
        population.case_type,
        judicial_district_code,
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_metrics_materialized` population
    LEFT JOIN absconsion_cte abs
        ON abs.person_id = population.person_id
        AND population.date_of_supervision BETWEEN abs.start_date AND COALESCE(DATE_SUB(abs.termination_date, INTERVAL 1 DAY), '9999-01-01')
    WHERE population.state_code = 'US_MO'
"""

US_MO_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_MO_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_NAME,
    view_query_template=US_MO_SUPERVISION_POPULATION_METRICS_PREPROCESSED_QUERY_TEMPLATE,
    description=US_MO_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_DESCRIPTION,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    state_base_dataset=STATE_BASE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER.build_and_print()
